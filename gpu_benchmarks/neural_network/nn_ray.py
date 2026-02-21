import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import time

# Ray Train imports
import ray
from ray import train
from ray.train import ScalingConfig, Checkpoint
from ray.train.torch import TorchTrainer, prepare_model, prepare_data_loader


# ── Dataset ──────────────────────────────────────────────────────────────────

class CustomDataset(Dataset):
    def __init__(self, n, m, nb_classes):
        self.X = torch.randn(n, m)
        self.y = torch.randint(0, nb_classes, (n,))

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

class NeuralNetwork(nn.Module):
    def __init__(self,layer_size,nb_classes):
        super(NeuralNetwork, self).__init__()
        hidden_size = 4096
        self.layer1 = nn.Linear(layer_size,hidden_size)
        self.batch_norm = nn.BatchNorm1d(hidden_size)
        self.relu = nn.ReLU()
        self.layer2 = nn.Linear(hidden_size, nb_classes)

    def forward(self, x):
        x = torch.flatten(x, 1)
        x = self.layer1(x)
        x = self.batch_norm(x)
        x = self.relu(x)
        x = self.layer2(x)
        output = F.log_softmax(x, dim=1)
        return output

# ── Training function (runs on every worker) ─────────────────────────────────

def train_func(config: dict):
    # Unpack config
    n           = config["n"]
    m           = config["m"]
    nb_classes  = config["nb_classes"]
    batch_size  = config["batch_size"]
    lr          = config["lr"]
    max_epochs  = config["max_epochs"]

    # Model — prepare_model moves it to the right device and wraps with DDP
    model = NeuralNetwork(m, nb_classes)
    model = prepare_model(model)

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    # DataLoader — prepare_data_loader adds DistributedSampler automatically
    dataset    = CustomDataset(n, m, nb_classes)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    dataloader = prepare_data_loader(dataloader)

    # Resume from checkpoint if one exists
    start_epoch = 0
    checkpoint  = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            state = torch.load(os.path.join(ckpt_dir, "checkpoint.pt"))
            model.module.load_state_dict(state["model_state"])
            optimizer.load_state_dict(state["optimizer_state"])
            start_epoch = state["epoch"] + 1
            print(f"Resumed from epoch {start_epoch}")

    # Training loop
    for epoch in range(start_epoch, max_epochs):
        model.train()
        total_loss = 0.0

        for X, y in dataloader:
            optimizer.zero_grad()
            pred = model(X).squeeze(-1)
            loss = F.nll_loss(pred, y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        avg_loss = total_loss / len(dataloader)

        # Save checkpoint and report metrics to Ray Train
        # train.report is how Ray collects metrics from all workers
        with tempfile.TemporaryDirectory() as ckpt_dir:
            # Only rank 0 needs to save the actual weights
            if train.get_context().get_world_rank() == 0:
                raw_model = model.module if hasattr(model, "module") else model
                torch.save(
                    {
                        "epoch": epoch,
                        "model_state": raw_model.state_dict(),
                        "optimizer_state": optimizer.state_dict(),
                    },
                    os.path.join(ckpt_dir, "checkpoint.pt"),
                )
            checkpoint = Checkpoint.from_directory(ckpt_dir)
            train.report(
                {"epoch": epoch, "train_loss": avg_loss},
                checkpoint=checkpoint,
            )

# ── Evaluation (runs on the driver, after training) ──────────────────────────

def evaluate(model, m, nb_classes, batch_size):
    device   = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model    = model.to(device)
    model.eval()

    test_dataset = CustomDataset(100, m, nb_classes)
    test_loader  = DataLoader(test_dataset, batch_size=batch_size)
    total_loss   = 0.0

    with torch.no_grad():
        for X, y in test_loader:
            X, y = X.to(device), y.to(device)
            pred = model(X).squeeze(-1)
            total_loss += F.nll_loss(pred, y, reduction='sum').item()

    print(f"Test Loss: {total_loss / len(test_loader):.4f}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(n, m, nb_classes, total_epochs, batch_size, num_workers, use_gpu):
    import tempfile  # needed inside train_func too

    ray.init()  # connects to existing cluster or starts a local one

    config = {
        "n":           n,
        "m":           m,
        "nb_classes":  nb_classes,
        "batch_size":  batch_size,
        "lr":          1e-3,
        "max_epochs":  total_epochs,
    }

    # ScalingConfig defines how many workers and whether they use GPUs
    scaling_config = ScalingConfig(
        num_workers=num_workers,  # number of distributed training workers
        use_gpu=use_gpu,          # give each worker a GPU
        resources_per_worker={
            "CPU": 4,             # CPU cores per worker for data loading
            "GPU": 1,             # 1 GPU per worker
        },
    )

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )

    start  = time.time()
    result = trainer.fit()
    print(f"Training time: {time.time() - start:.2f}s")
    print(f"Best checkpoint: {result.best_checkpoints}")

    # Load best checkpoint and evaluate
    best_checkpoint = result.checkpoint
    with best_checkpoint.as_directory() as ckpt_dir:
        state = torch.load(os.path.join(ckpt_dir, "checkpoint.pt"))
        model = NeuralNetwork(m, nb_classes)
        model.load_state_dict(state["model_state"])

    evaluate(model, m, nb_classes, batch_size)
    ray.shutdown()


if __name__ == "__main__":
    import argparse
    import tempfile

    parser = argparse.ArgumentParser()
    parser.add_argument("n",            type=int,            help="number of samples")
    parser.add_argument("m",            type=int,            help="number of features")
    parser.add_argument('nb_classes', type=int, help='number of classes')
    parser.add_argument("total_epochs", type=int,            help="number of epochs")
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--num_workers",type=int, default=1, help="number of GPU workers")
    parser.add_argument("--use_gpu",    action="store_true", default=True)
    args = parser.parse_args()

    main(args.n, args.m, args.nb_classes, args.total_epochs, args.batch_size, args.num_workers, args.use_gpu)