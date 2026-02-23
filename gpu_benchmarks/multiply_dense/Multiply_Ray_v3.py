import sys
import os
import time
import numpy as np
import torch
import ray
from typing import Dict, Tuple, List
import subprocess
import threading

@ray.remote
def generate_block(i: int, j: int, seed: int, block_size: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((block_size, block_size)).astype(np.float32)

@ray.remote(num_gpus=1)
def upload_blocks_to_gpu(
    block_refs : List[ray.ObjectRef],
    keys       : List[Tuple[int,int]],
) -> Dict[str, torch.Tensor]:
    device = torch.device("cuda")
    blocks = ray.get(block_refs)
    return {
        f"({i},{j})": torch.tensor(arr, device=device, dtype=torch.float32)
        for (i, j), arr in zip(keys, blocks)
    }

@ray.remote(num_gpus=1)
def compute_C_batch(
    A_gpu      : Dict[str, torch.Tensor],
    B_gpu      : Dict[str, torch.Tensor],
    tasks      : List[Tuple[int,int]],
    num_blocks : int,
    block_size : int,
) -> Dict[str, np.ndarray]:
    device  = torch.device("cuda")
    BS = block_size
    K = num_blocks
    results = {}

    for (i, j) in tasks:
        A_stack = torch.stack(
            [A_gpu[f"({i},{k})"] for k in range(K)])
        B_stack = torch.stack(
            [B_gpu[f"({k},{j})"] for k in range(K)])
        C_block = torch.bmm(
            A_stack.float(),
            B_stack.float(),
        ).sum(dim=0)

        results[f"({i},{j})"] = C_block.cpu().numpy()

        del A_stack, B_stack, C_block

    torch.cuda.synchronize()
    return results

class TaskBlockMatMul:

    def __init__(
        self,
        matrix_size : int,
        block_size  : int,
        num_gpus    : int,
    ):
        assert matrix_size % block_size == 0, \
            f"matrix_size ({matrix_size}) must be divisible " \
            f"by block_size ({block_size})"

        self.matrix_size = matrix_size
        self.block_size  = block_size
        self.num_blocks  = matrix_size // block_size
        self.num_gpus    = num_gpus

        self.A_plasma   : Dict[Tuple, ray.ObjectRef] = {}
        self.B_plasma   : Dict[Tuple, ray.ObjectRef] = {}
        self.A_gpu_refs : List[ray.ObjectRef] = []
        self.B_gpu_refs : List[ray.ObjectRef] = []
        self.assignments: List[List[Tuple]]   = []

        self._print_config()

    def _print_config(self):
        nb  = self.num_blocks
        bs  = self.block_size
        ms  = self.matrix_size
        ng  = self.num_gpus
        bpg = bs * bs * 4 / 1e9

        tasks_per_gpu    = (nb * nb + ng - 1) // ng
        A_blocks_per_gpu = tasks_per_gpu   # rough estimate
        B_blocks_per_gpu = tasks_per_gpu

        print(f"\n{'='*60}")
        print(f"  matrix_size  : {ms:>10,}  ({ms:,} x {ms:,})")
        print(f"  block_size   : {bs:>10,}  ({bs:,} x {bs:,})")
        print(f"  num_blocks   : {nb:>10,}  ({nb} x {nb} = {nb*nb:,} blocks)")
        print(f"  num_gpus     : {ng:>10}")
        print(f"  tasks/gpu    : {tasks_per_gpu:>10,}")
        print(f"  block mem    : {bpg:>9.3f} GB (fp32)")
        print(f"{'='*60}\n")

    def _assign_tasks(self) -> List[List[Tuple]]:
        all_tasks = [
            (i, j)
            for i in range(self.num_blocks)
            for j in range(self.num_blocks)
        ]
        assignments = [[] for _ in range(self.num_gpus)]
        for idx, task in enumerate(all_tasks):
            assignments[idx % self.num_gpus].append(task)
        return assignments

    def setup(self, seed: int = 42):
        nb = self.num_blocks
        bs = self.block_size

        # Phase 1: generate blocks into plasma
        print("Phase 1: Generating blocks into plasma store...")
        t0 = time.time()

        for i in range(nb):
            for k in range(nb):
                self.A_plasma[(i,k)] = generate_block.remote(
                    i, k, seed + i * nb + k, bs)

        for k in range(nb):
            for j in range(nb):
                self.B_plasma[(k,j)] = generate_block.remote(
                    k, j, seed + 10_000 + k * nb + j, bs)

        all_plasma = list(self.A_plasma.values()) + \
                     list(self.B_plasma.values())
        ray.wait(all_plasma, num_returns=len(all_plasma))
        print(f"  Done in {time.time()-t0:.2f}s  "
              f"({len(all_plasma):,} blocks, "
              f"{len(all_plasma) * bs * bs * 2 / 1e9:.1f} GB total)")

        # Phase 2: upload blocks to GPUs
        print("Phase 2: Uploading blocks to GPU memory...")
        t1 = time.time()

        self.assignments = self._assign_tasks()
        self.A_gpu_refs  = []
        self.B_gpu_refs  = []

        for gpu_idx, tasks in enumerate(self.assignments):
            needed_rows = {i for (i, _) in tasks}
            needed_cols = {j for (_, j) in tasks}

            A_keys = [(i, k) for i in needed_rows for k in range(nb)]
            B_keys = [(k, j) for k in range(nb) for j in needed_cols]

            self.A_gpu_refs.append(
                upload_blocks_to_gpu.options(num_gpus=1).remote(
                    [self.A_plasma[(i,k)] for (i,k) in A_keys], A_keys))
            self.B_gpu_refs.append(
                upload_blocks_to_gpu.options(num_gpus=1).remote(
                    [self.B_plasma[(k,j)] for (k,j) in B_keys], B_keys))

        ray.wait(
            self.A_gpu_refs + self.B_gpu_refs,
            num_returns=len(self.A_gpu_refs) + len(self.B_gpu_refs))
        print(f"  Done in {time.time()-t1:.2f}s\n")

    def compute(self) -> Tuple[Dict, float]:
        nb = self.num_blocks
        bs = self.block_size
        print(f"Computing {nb*nb:,} blocks across {self.num_gpus} GPUs...")
        t0 = time.time()

        futures = [
            compute_C_batch.options(num_gpus=1).remote(
                self.A_gpu_refs[gpu_idx],
                self.B_gpu_refs[gpu_idx],
                tasks,
                nb,
                bs,
            )
            for gpu_idx, tasks in enumerate(self.assignments)
        ]

        results_list = ray.get(futures)
        elapsed      = time.time() - t0

        C = {}
        for results in results_list:
            for key_str, arr in results.items():
                a, b = key_str.strip("()").split(",")
                C[(int(a.strip()), int(b.strip()))] = arr

        return C, elapsed


    def verify_spot_check(self, C: Dict, n: int = 4) -> bool:
        import random
        random.seed(0)
        nb     = self.num_blocks
        bs     = self.block_size
        keys   = random.sample(list(C.keys()), min(n, len(C)))
        all_ok = True

        print(f"\nSpot-checking {len(keys)} C blocks...")
        for (i, j) in keys:
            A_row = [ray.get(self.A_plasma[(i,k)]) for k in range(nb)]
            B_col = [ray.get(self.B_plasma[(k,j)]) for k in range(nb)]
            C_ref = sum(
                A_row[k].astype(np.float32) @ B_col[k].astype(np.float32)
                for k in range(nb)
            )
            err    = np.max(np.abs(C[(i,j)].astype(np.float32) - C_ref))
            ok     = err < 2.0
            all_ok = all_ok and ok
            print(f"  C[{i:3d},{j:3d}]: max_err={err:.4f} "
                  f"{'✓' if ok else '✗'}")

        print(f"Spot check: {'PASSED ✓' if all_ok else 'FAILED ✗'}")
        return all_ok


    def report_performance(self, elapsed: float):
        N      = self.matrix_size
        nb     = self.num_blocks
        bs     = self.block_size
        ng     = self.num_gpus
        flops  = 2 * N * N * N
        tflops = flops / elapsed / 1e12
        mem_gb = 3 * N * N * 4 / 1e9

        print(f"\n{'='*50}")
        print(f"  matrix_size : {N:,} x {N:,}")
        print(f"  block_size  : {bs:,} x {bs:,}")
        print(f"  num_blocks  : {nb} x {nb} = {nb*nb:,}")
        print(f"  num_gpus    : {ng}")
        print(f"  time        : {elapsed:.2f}s")
        print(f"  TFLOPS      : {tflops:.3f}")
        print(f"  A+B+C mem   : {mem_gb:.1f} GB (fp32)")
        print(f"{'='*50}\n")

def verify_small(num_gpus: int):
    bs          = 1024
    nb          = max(4, num_gpus)
    matrix_size = bs * nb

    print(f"\n--- Correctness check "
          f"({matrix_size}x{matrix_size}, "
          f"block={bs}, nb={nb}, gpus={num_gpus}) ---")

    np.random.seed(0)
    A_np  = np.random.randn(matrix_size, matrix_size).astype(np.float32)
    B_np  = np.random.randn(matrix_size, matrix_size).astype(np.float32)
    C_ref = A_np @ B_np

    dm = TaskBlockMatMul(
        matrix_size=matrix_size,
        block_size=bs,
        num_gpus=num_gpus,
    )
    dm.setup(seed=0)

    # Override plasma with known data
    for i in range(nb):
        for k in range(nb):
            dm.A_plasma[(i,k)] = ray.put(
                A_np[i*bs:(i+1)*bs, k*bs:(k+1)*bs].astype(np.float32))
    for k in range(nb):
        for j in range(nb):
            dm.B_plasma[(k,j)] = ray.put(
                B_np[k*bs:(k+1)*bs, j*bs:(j+1)*bs].astype(np.float32))

    # Re-upload with known data
    dm.A_gpu_refs = []
    dm.B_gpu_refs = []
    for gpu_idx, tasks in enumerate(dm.assignments):
        needed_rows = {i for (i,_) in tasks}
        needed_cols = {j for (_,j) in tasks}
        A_keys = [(i,k) for i in needed_rows for k in range(nb)]
        B_keys = [(k,j) for k in range(nb) for j in needed_cols]
        dm.A_gpu_refs.append(
            upload_blocks_to_gpu.options(num_gpus=1).remote(
                [dm.A_plasma[(i,k)] for (i,k) in A_keys], A_keys))
        dm.B_gpu_refs.append(
            upload_blocks_to_gpu.options(num_gpus=1).remote(
                [dm.B_plasma[(k,j)] for (k,j) in B_keys], B_keys))
    ray.wait(dm.A_gpu_refs + dm.B_gpu_refs,
             num_returns=len(dm.A_gpu_refs) + len(dm.B_gpu_refs))

    C, _ = dm.compute()

    max_err = max(
        np.max(np.abs(
            C[(i,j)].astype(np.float32) -
            C_ref[i*bs:(i+1)*bs, j*bs:(j+1)*bs]))
        for i in range(nb) for j in range(nb)
    )
    ok = max_err < 2.0
    print(f"Max error: {max_err:.4f} → {'PASSED ✓' if ok else 'FAILED ✗'}\n")
    assert ok, f"Correctness check failed: max_err={max_err}"

def monitor_gpus(interval: float = 2.0, stop_event=None):
    """
    Background thread that prints GPU stats every `interval` seconds.
    Run alongside your compute to see live utilization.
    """
    while not (stop_event and stop_event.is_set()):
        result = subprocess.run(
            ["nvidia-smi",
             "--query-gpu=index,utilization.gpu,memory.used,memory.free",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True
        )
        ts = time.strftime("%H:%M:%S")
        print(f"\n[{ts}] GPU utilization:")
        for line in result.stdout.strip().split("\n"):
            idx, gpu_util, mem_used, mem_free = line.split(", ")
            bar = "█" * (int(gpu_util) // 5)
            print(f"  GPU {idx}: [{bar:<20}] {gpu_util:>3}% | "
                  f"mem {mem_used:>6}/{int(mem_used)+int(mem_free)} MB")
        time.sleep(interval)

def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <matrix_size>")
        sys.exit(1)

    matrix_size = int(sys.argv[1])
    block_size  = 8192

    if matrix_size % block_size != 0:
        print(f"Error: matrix_size ({matrix_size}) must be divisible "
              f"by block_size ({block_size})")
        sys.exit(1)

    num_blocks = matrix_size // block_size

    ray.init(address=os.environ.get("ip_head", None))
    num_gpus = int(ray.available_resources().get("GPU", 0))

    if num_gpus == 0:
        print("Error: no GPUs available in Ray cluster.")
        ray.shutdown()
        sys.exit(1)

    print(f"Ray resources : {ray.available_resources()}")
    print(f"matrix_size   : {matrix_size:,}")
    print(f"block_size    : {block_size:,}")
    print(f"num_blocks    : {num_blocks} x {num_blocks} = {num_blocks**2:,}")
    print(f"num_gpus      : {num_gpus}\n")
    stop_event  = threading.Event()
    monitor     = threading.Thread(
        target=monitor_gpus,
        args=(2.0, stop_event),
        daemon=True,
    )
    monitor.start()

    verify_small(num_gpus=num_gpus)

    dm = TaskBlockMatMul(
        matrix_size=matrix_size,
        block_size=block_size,
        num_gpus=num_gpus,
    )
    dm.setup(seed=42)

    C, elapsed = dm.compute()
    dm.report_performance(elapsed)
    dm.verify_spot_check(C, n=4)
    stop_event.set()
    monitor.join()
    ray.shutdown()

if __name__ == "__main__":
    main()
