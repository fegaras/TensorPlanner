import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import time
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP

class CustomDataset(Dataset):
    def __init__(self, n, m, nb_classes):
        self.n = n
        self.m = m
        X = torch.randn(n,m)
        y = torch.randn(n, nb_classes).softmax(dim=1)
        self.data = [(torch.index_select(X,0,torch.tensor([idx])),torch.index_select(y,0,torch.tensor([idx]))) for idx in range(n)]

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.data[idx]

class NeuralNetwork(nn.Module):
    def __init__(self,layer_size,nb_classes):
        super(NeuralNetwork, self).__init__()
        self.layer1 = nn.Linear(layer_size,1000)
        self.relu = nn.ReLU()
        self.layer2 = nn.Linear(1000, nb_classes)

    def forward(self, input):
        return self.layer2(self.relu(self.layer1(input)))

class Trainer:
    def __init__(
        self,
        model: torch.nn.Module,
        train_data: DataLoader,
        optimizer: torch.optim.Optimizer
    ) -> None:
        self.model = model
        self.train_data = train_data
        self.optimizer = optimizer

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source)
        criterion = nn.CrossEntropyLoss()
        loss = criterion(output, targets)
        loss.backward()
        self.optimizer.step()

    def _run_epoch(self, epoch):
        b_sz = len(next(iter(self.train_data))[0])
        print(f"[Epoch {epoch} | Batchsize: {b_sz} | Steps: {len(self.train_data)}")
        for source, targets in self.train_data:
            self._run_batch(source, targets)

    def train(self, max_epochs: int):
        local_rank = int(os.environ["LOCAL_RANK"])
        self.model = DDP(self.model)
        
        for epoch in range(max_epochs):
            self._run_epoch(epoch)

def load_train_objs(n, m, nb_classes):
    train_set = CustomDataset(n,m, nb_classes=nb_classes)  # load your dataset
    model = NeuralNetwork(m, nb_classes)  # load your model
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

def main(total_epochs, n, m, nb_classes, batch_size):
    dataset, model, optimizer = load_train_objs(n, m, nb_classes)
    train_data = prepare_dataloader(dataset, batch_size)
    start = time.time()
    trainer = Trainer(model, train_data, optimizer)
    trainer.train(total_epochs)
    print(f"Neural Network, n: {n}, m: {m}, time: {time.time()-start}")
    test_dataset = CustomDataset(100,m,nb_classes)
    test_data = prepare_dataloader(test_dataset, batch_size)
    test(test_data,trainer.model)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='simple distributed training job')
    parser.add_argument('n', type=int, help='input size')
    parser.add_argument('m', type=int, help='feature size')
    parser.add_argument('total_epochs', type=int, help='Total epochs to train the model')
    parser.add_argument('nb_classes', type=int, help='number of classes')
    parser.add_argument('--batch_size', default=32, type=int, help='Input batch size on each device (default: 32)')
    args = parser.parse_args()

    dist.init_process_group(backend="gloo")
    main(args.total_epochs, args.n, args.m, args.nb_classes, args.batch_size)
    dist.destroy_process_group()
