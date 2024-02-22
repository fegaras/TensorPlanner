import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import pandas as pd

class CustomDataset(Dataset):
    def __init__(self, n, m):
        self.n = n
        self.m = m
        X = torch.randn(n,m)
        y = torch.randn(n)
        self.data = [(torch.tensor(X.iloc[idx]),torch.tensor(y.iloc[idx])) for idx in range(n)]

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.data[idx]

class Trainer:
    def __init__(
        self,
        model: torch.nn.Module,
        train_data: DataLoader,
        optimizer: torch.optim.Optimizer,
        save_every: int, 
    ) -> None:
        self.model = model
        self.train_data = train_data
        self.optimizer = optimizer
        self.save_every = save_every

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source)
        loss = F.cross_entropy(output, targets)
        loss.backward()
        self.optimizer.step()

    def _run_epoch(self, epoch):
        b_sz = len(next(iter(self.train_data))[0])
        print(f"[Epoch {epoch} | Batchsize: {b_sz} | Steps: {len(self.train_data)}")
        for source, targets in self.train_data:
            self._run_batch(source, targets)

    def _save_checkpoint(self, epoch):
        ckp = self.model.state_dict()
        PATH = "checkpoint.pt"
        torch.save(ckp, PATH)
        print(f"Epoch {epoch} | Training checkpoint saved at {PATH}")

    def train(self, max_epochs: int):
        for epoch in range(max_epochs):
            self._run_epoch(epoch)
            if epoch % self.save_every == 0:
                self._save_checkpoint(epoch)


def load_train_objs(input_file, output_file, n, m):
    train_set = CustomDataset(n,m)  # load your dataset
    model = torch.nn.Linear(m, 1)  # load your model
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)
    return train_set, model, optimizer


def prepare_dataloader(dataset: Dataset, batch_size: int):
    return DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=True,
        shuffle=True
    )

def test(dataloader, model):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.eval()
    test_loss = 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            loss = torch.mean((pred - y) ** 2)
            print(loss)
            test_loss += loss.item()
    test_loss /= num_batches
    print(f"Test Error: {test_loss}\n")

def main(total_epochs, save_every, input_file, output_file, n, m, batch_size):
    dataset, model, optimizer = load_train_objs(input_file, output_file, n, m)
    train_data = prepare_dataloader(dataset, batch_size)
    trainer = Trainer(model, train_data, optimizer, save_every)
    trainer.train(total_epochs)
    test_dataset = CustomDataset(100,m)
    test_data = prepare_dataloader(test_dataset, batch_size)
    test(test_data,trainer.model)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='simple distributed training job')
    parser.add_argument('total_epochs', type=int, help='Total epochs to train the model')
    parser.add_argument('save_every', type=int, help='How often to save a snapshot')
    parser.add_argument('n', type=int, help='input size')
    parser.add_argument('m', type=int, help='feature size')
    parser.add_argument('--batch_size', default=32, type=int, help='Input batch size on each device (default: 32)')
    args = parser.parse_args()

    main(args.total_epochs, args.save_every, args.input_file, args.output_file, args.n, args.m, args.batch_size)