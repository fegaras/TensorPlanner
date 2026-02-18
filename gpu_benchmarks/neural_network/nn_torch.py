import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import argparse
import torch.optim as optim
from torch.optim.lr_scheduler import StepLR
from torchvision import datasets, transforms
import time
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP

class CustomDataset(Dataset):
    def __init__(self, n, m, nb_classes):
        self.n = n
        self.m = m
        dtype = torch.float
        # device = torch.device("cuda:0")
        X = torch.randn(n, m, dtype=dtype)
        # y = torch.randn(n, nb_classes, dtype=dtype).softmax(dim=1)
        y = torch.randint(0, nb_classes, (n,))
        self.data = [(torch.index_select(X,0,torch.tensor([idx])),torch.index_select(y,0,torch.tensor([idx]))) for idx in range(n)]

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.data[idx]

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

def train(args, model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        target = torch.flatten(target)
        loss = F.nll_loss(output, target)
        # criterion = nn.CrossEntropyLoss()
        # loss = criterion(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), loss.item()))


def test(model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            target = torch.flatten(target)
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            # loss = torch.mean((output - target) ** 2)
            # test_loss += loss.item()
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset),
        100. * correct / len(test_loader.dataset)))


def main():
    parser = argparse.ArgumentParser(description='simple distributed training job')
    parser.add_argument('n', type=int, help='input size')
    parser.add_argument('m', type=int, help='feature size')
    parser.add_argument('nb_classes', type=int, help='number of classes')
    parser.add_argument('--batch_size', default=32, type=int, help='Input batch size on each device (default: 32)')
    parser.add_argument('--lr', type=float, default=1.0, metavar='LR', help='learning rate (default: 1.0)')
    parser.add_argument('--epochs', type=int, default=10, metavar='N',
                        help='number of epochs to train (default: 14)')
    parser.add_argument('--log-interval', type=int, default=500, metavar='N',
                        help='how many batches to wait before logging training status')
    args = parser.parse_args()
    use_cuda = torch.cuda.is_available()

    if use_cuda:
        print("Using GPU")
        device = torch.device("cuda")
        dist.init_process_group(backend="nccl")
    else:
        device = torch.device("cpu")
        dist.init_process_group(backend="gloo")

    train_kwargs = {'batch_size': args.batch_size}
    test_kwargs = {'batch_size': args.batch_size}
    if use_cuda:
        cuda_kwargs = {'num_workers': 1,
                       'pin_memory': True,
                       'shuffle': True}
        train_kwargs.update(cuda_kwargs)
        test_kwargs.update(cuda_kwargs)

    dataset1 = CustomDataset(args.n, args.m, nb_classes=args.nb_classes)
    dataset2 = CustomDataset(100, args.m, nb_classes=args.nb_classes)
    train_loader = DataLoader(dataset1,**train_kwargs)
    test_loader = DataLoader(dataset2, **test_kwargs)

    model = NeuralNetwork(args.m, args.nb_classes).to(device)
    optimizer = optim.SGD(model.parameters(), lr=args.lr)

    scheduler = StepLR(optimizer, step_size=1)
    start = time.time()
    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch)
        test(model, device, test_loader)
        scheduler.step()
    print(f"Neural Network, n: {args.n}, m: {args.m}, time: {time.time()-start}")

if __name__ == '__main__':
    main()
    dist.destroy_process_group()
