import os
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist
from torch.amp import autocast, GradScaler
import time


class CustomDataset(Dataset):
    def __init__(self, n, m):
        # Pin to shared memory for faster DataLoader access
        self.X = torch.randn(n, m).share_memory_()
        self.y = torch.randn(n).share_memory_()

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


class Trainer:
    def __init__(
        self,
        model      : nn.Module,
        train_data : DataLoader,
        device     : int,
        use_amp    : bool = True,
        compile    : bool = True,
    ) -> None:
        self.local_rank  = device
        self.global_rank = dist.get_rank()
        self.train_data  = train_data
        self.use_amp     = use_amp
        self.scaler      = GradScaler('cuda') if use_amp else None
        self.loss_fn     = nn.MSELoss()

        # Move model and wrap with DDP
        self.model = model.to(device)
        self.model = DDP(
            self.model,
            device_ids=[self.local_rank],
            # Overlap gradient communication with backward pass
            gradient_as_bucket_view=True
        )

        # torch.compile fuses ops into single CUDA kernels (PyTorch 2.0+)
        if compile and hasattr(torch, "compile"):
            self.model = torch.compile(self.model)

        # SGD with momentum — faster convergence than plain SGD
        self.optimizer = torch.optim.SGD(
            self.model.parameters(),
            lr=1e-3,
            momentum=0.9,
            nesterov=True,
        )

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad(set_to_none=True)   # faster than zero_grad()

        # Mixed precision: fp16 forward + backward, fp32 optimizer step
        with autocast('cuda', enabled=self.use_amp):
            output = self.model(source).squeeze(-1)
            loss   = self.loss_fn(output, targets)

        if self.use_amp:
            self.scaler.scale(loss).backward()
            self.scaler.step(self.optimizer)
            self.scaler.update()
        else:
            loss.backward()
            self.optimizer.step()

        return loss.item()

    def _run_epoch(self, epoch):
        self.model.train()
        self.train_data.sampler.set_epoch(epoch)
        total_loss = 0.0

        for source, targets in self.train_data:
            # Non-blocking transfer overlaps CPU→GPU copy with compute
            source  = source.to(self.local_rank, non_blocking=True)
            targets = targets.to(self.local_rank, non_blocking=True)
            total_loss += self._run_batch(source, targets)

        if self.global_rank == 0:
            print(f"[Epoch {epoch}] loss: {total_loss/len(self.train_data):.4f}")

    def train(self, max_epochs: int):
        for epoch in range(max_epochs):
            self._run_epoch(epoch)


def load_train_objs(n, m):
    train_set = CustomDataset(n, m)
    model     = nn.Linear(m, 1)
    return train_set, model


def prepare_dataloader(dataset: Dataset, batch_size: int):
    return DataLoader(
        dataset,
        batch_size  = batch_size,
        pin_memory  = True,
        shuffle     = False,
        # More workers = more CPU prefetch parallelism
        num_workers = min(8, os.cpu_count() // dist.get_world_size()),
        sampler     = DistributedSampler(dataset),
        # Keeps workers alive between epochs — no respawn overhead
        persistent_workers = True,
        # Prefetch next batch while GPU is busy
        prefetch_factor    = 2,
    )


def setup_ddp():
    dist.init_process_group(backend="nccl")
    torch.cuda.set_device(int(os.environ["LOCAL_RANK"]))
    # Allow TF32 for faster matmuls on Ampere+ GPUs
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32        = True
    # cuDNN auto-tuner finds fastest conv algorithm for fixed input sizes
    torch.backends.cudnn.benchmark         = True


def cleanup_ddp():
    dist.destroy_process_group()


def main(total_epochs, n, m, batch_size, use_amp=True, compile=True):
    setup_ddp()
    local_rank = int(os.environ["LOCAL_RANK"])

    dataset, model = load_train_objs(n, m)
    train_data     = prepare_dataloader(dataset, batch_size)

    # Warmup CUDA before timing
    torch.cuda.synchronize()
    start = time.time()

    trainer = Trainer(
        model, train_data, local_rank,
        use_amp=use_amp,
        compile=compile,
    )
    trainer.train(total_epochs)

    torch.cuda.synchronize()
    elapsed = time.time() - start

    if local_rank == 0:
        print(f"Done — n:{n} m:{m} epochs:{total_epochs} "
              f"time:{elapsed:.2f}s "
              f"throughput:{n*total_epochs/elapsed:.0f} samples/s")

    cleanup_ddp()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("n",            type=int)
    parser.add_argument("m",            type=int)
    parser.add_argument("total_epochs", type=int)
    parser.add_argument("--batch_size", default=32,   type=int)
    parser.add_argument("--no-amp",     action="store_true")
    parser.add_argument("--no-compile", action="store_true")
    args = parser.parse_args()
    main(
        args.total_epochs, args.n, args.m, args.batch_size,
        use_amp  = not args.no_amp,
        compile  = not args.no_compile,
    )
