import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist
import time

class CustomDataset(Dataset):
    def __init__(self, n, m, nb_classes):
        self.X = torch.randn(n, m)
        self.y = torch.randint(0, nb_classes, (n,))

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

class Trainer:
    def __init__(
        self,
        model: nn.Module,
        train_data: DataLoader,
        device: int,  # local rank
    ) -> None:
        self.local_rank = device
        self.global_rank = dist.get_rank()
        self.model = model.to(device)
        self.train_data = train_data
        self.optimizer = torch.optim.SGD(model.parameters(), lr=1e-1)
        self.epochs_run = 0

        # Wrap model with DDP
        self.model = DDP(self.model, device_ids=[self.local_rank])

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source).squeeze(-1)
        loss = F.nll_loss(output, targets)
        loss.backward()
        self.optimizer.step()
        return loss.item()

    def _run_epoch(self, epoch):
        self.model.train()
        # Tell sampler which epoch it is for proper shuffling
        self.train_data.sampler.set_epoch(epoch)
        total_loss = 0
        for source, targets in self.train_data:
            source = source.to(self.local_rank)
            targets = targets.to(self.local_rank)
            total_loss += self._run_batch(source, targets)
        if self.global_rank == 0:
            avg_loss = total_loss / len(self.train_data)
            print(f"[Epoch {epoch}] Train loss: {avg_loss:.4f}")

    def train(self, max_epochs: int):
        for epoch in range(self.epochs_run, max_epochs):
            self._run_epoch(epoch)

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

def load_train_objs(n, m, nb_classes):
    train_set = CustomDataset(n, m, nb_classes)
    model = NeuralNetwork(m, nb_classes)  # don't move to device yet; Trainer handles it
    return train_set, model


def prepare_dataloader(dataset: Dataset, batch_size: int):
    return DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=True,
        shuffle=False,           # must be False when using DistributedSampler
        num_workers=4,
        sampler=DistributedSampler(dataset),  # splits data across GPUs
    )


def test(dataloader, model, device):
    # Only evaluate on rank 0
    if dist.get_rank() != 0:
        return
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            pred = model(X).squeeze(-1)
            test_loss += F.nll_loss(pred, y, reduction='sum').item()
            pred_labels = pred.argmax(dim=1, keepdim=True)
            correct += pred_labels.eq(y.view_as(pred_labels)).sum().item()
    test_loss /= len(dataloader.dataset)
    print(f"Test Loss: {test_loss:.4f}, Test Accuracy: {100. * correct / len(dataloader.dataset):.4f}")


def setup_ddp():
    dist.init_process_group(backend="nccl")  # nccl is optimal for GPU clusters
    torch.cuda.set_device(int(os.environ["LOCAL_RANK"]))

def cleanup_ddp():
    dist.destroy_process_group()


def main(total_epochs, n, m, nb_classes, batch_size):
    setup_ddp()
    local_rank = int(os.environ["LOCAL_RANK"])

    dataset, model = load_train_objs(n, m, nb_classes)
    train_data = prepare_dataloader(dataset, batch_size)

    start = time.time()
    trainer = Trainer(model, train_data, local_rank)
    trainer.train(total_epochs)

    if local_rank == 0:
        print(f"Training done — n: {n}, m: {m}, nb_classes: {nb_classes}, time: {time.time() - start:.2f}s")
        test_dataset = CustomDataset(100, m, nb_classes)
        test_data = DataLoader(test_dataset, batch_size=batch_size)
        test(test_data, trainer.model.module, local_rank)

    cleanup_ddp()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('n', type=int, help='number of samples')
    parser.add_argument('m', type=int, help='number of features')
    parser.add_argument('nb_classes', type=int, help='number of classes')
    parser.add_argument('total_epochs', type=int)
    parser.add_argument('--batch_size', default=32, type=int)
    args = parser.parse_args()
    main(args.total_epochs, args.n, args.m, args.nb_classes, args.batch_size)