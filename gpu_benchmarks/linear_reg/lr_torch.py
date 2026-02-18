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

LOCAL_RANK = int(os.environ['LOCAL_RANK'])
WORLD_SIZE = int(os.environ['WORLD_SIZE'])
WORLD_RANK = int(os.environ['RANK'])

class CustomDataset(Dataset):
    def __init__(self, n, m):
        self.n = n
        self.m = m
        dtype = torch.float
        X = torch.randn(n, m, dtype=dtype)
        y = torch.randn(n, dtype=dtype)
        self.data = [(torch.index_select(X,0,torch.tensor([idx])),torch.index_select(y,0,torch.tensor([idx]))) for idx in range(n)]

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.data[idx]

def train(args, model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        output = output.squeeze(-1)
        loss = torch.nn.MSELoss()
        err = loss(output,targets)
        err.backward()
        # target = torch.flatten(target)
        # loss = F.nll_loss(output, target)
        # criterion = nn.CrossEntropyLoss()
        # loss = criterion(output, target)
        # loss.backward()
        optimizer.step()
        if(batch_idx % args.log_interval == 0 and WORLD_RANK == 0):
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), err.item()))


def test(model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            # target = torch.flatten(target)
            # test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            loss = torch.mean((output - target) ** 2)
            test_loss += loss.item()
            
            # pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            # correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)

    if(WORLD_RANK == 0):
        print('\nTest set: Average loss: {:.4f}\n'.format(test_loss))


def main():
    parser = argparse.ArgumentParser(description=' distributed LR job')
    parser.add_argument('n', type=int, help='input size')
    parser.add_argument('m', type=int, help='feature size')
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

    dataset1 = CustomDataset(args.n, args.m)
    dataset2 = CustomDataset(100, args.m)
    train_loader = DataLoader(dataset1,**train_kwargs)
    test_loader = DataLoader(dataset2, **test_kwargs)

    model = torch.nn.Linear(args.m, 1).to(device)
    optimizer = optim.SGD(model.parameters(), lr=args.lr)

    scheduler = StepLR(optimizer, step_size=1)
    if(dist.is_initialized()):
        print(f"Number of nodes: {dist.get_world_size()}")
    start = time.time()
    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch)
        test(model, device, test_loader)
        scheduler.step()
    print(f"Neural Network, n: {args.n}, m: {args.m}, time: {time.time()-start}")

if __name__ == '__main__':
    main()
    dist.destroy_process_group()
