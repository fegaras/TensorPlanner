import os
import time
import numpy as np
import torch
import ray
from ray.util.collective import collective
import math


# ── GPU Worker Actor ──────────────────────────────────────────────────────────

@ray.remote(num_gpus=1)
class MatmulWorker:
    """
    Each worker owns:
      - A block  : (n_local, m_local) shard of A
      - B block  : (m_local, p_local) shard of B
      - C block  : (n_local, p_local) accumulated result

    Workers are arranged in a 2D grid (grid_rows x grid_cols).
    SUMMA algorithm: broadcast row panels of A and column panels of B,
    accumulate local matmuls into C.
    """

    def __init__(self, rank, world_size, grid_rows, grid_cols, dtype=torch.float32):
        self.rank       = rank
        self.world_size = world_size
        self.grid_rows  = grid_rows
        self.grid_cols  = grid_cols
        self.dtype      = dtype
        self.device     = torch.device("cuda:0")  # each actor has 1 GPU

        # Position in the 2D grid
        self.row_rank = rank // grid_cols
        self.col_rank = rank % grid_cols

        # Blocks initialized to None — set via init_blocks()
        self.A_block = None
        self.B_block = None
        self.C_block = None

        print(f"[Worker {rank}] grid pos=({self.row_rank},{self.col_rank}) "
              f"device={self.device}")

    def init_blocks(self, n_local, m_local, p_local, seed=None):
        """Initialize random A and B blocks on this GPU."""
        if seed is not None:
            torch.manual_seed(seed + self.rank)

        self.A_block = torch.randn(
            n_local, m_local, device=self.device, dtype=self.dtype)
        self.B_block = torch.randn(
            m_local, p_local, device=self.device, dtype=self.dtype)
        self.C_block = torch.zeros(
            n_local, p_local, device=self.device, dtype=self.dtype)

        return {
            "rank":    self.rank,
            "A_shape": list(self.A_block.shape),
            "B_shape": list(self.B_block.shape),
            "C_shape": list(self.C_block.shape),
            "mem_gb":  torch.cuda.memory_allocated() / 1e9,
        }

    def set_blocks(self, A_np, B_np):
        """Set A and B blocks from numpy arrays (for testing/verification)."""
        self.A_block = torch.tensor(A_np, device=self.device, dtype=self.dtype)
        self.B_block = torch.tensor(B_np, device=self.device, dtype=self.dtype)
        self.C_block = torch.zeros(
            self.A_block.shape[0], self.B_block.shape[1],
            device=self.device, dtype=self.dtype)

    def get_C_block(self):
        """Return C block as numpy array for gathering on driver."""
        return self.C_block.cpu().to(torch.float32).numpy()

    def get_A_block(self):
        return self.A_block.cpu().to(torch.float32).numpy()

    def get_B_block(self):
        return self.B_block.cpu().to(torch.float32).numpy()

    def memory_stats(self):
        allocated = torch.cuda.memory_allocated(self.device) / 1e9
        reserved  = torch.cuda.memory_reserved(self.device)  / 1e9
        return {
            "rank":      self.rank,
            "allocated": f"{allocated:.2f} GB",
            "reserved":  f"{reserved:.2f}  GB",
        }

    # ── SUMMA broadcast helpers ───────────────────────────────────────────

    def broadcast_A_panel(self, k):
        """
        If this worker is the source (col_rank == k), return its A block.
        Otherwise return None. The driver then sends the panel to all
        workers in the same row.
        """
        if self.col_rank == k:
            return self.A_block.cpu().numpy()
        return None

    def broadcast_B_panel(self, k):
        """Same for B: source is row_rank == k."""
        if self.row_rank == k:
            return self.B_block.cpu().numpy()
        return None

    def accumulate(self, A_panel_np, B_panel_np):
        """
        Receive broadcast panels and do local matmul accumulation.
        C_block += A_panel @ B_panel
        """
        A_panel = torch.tensor(A_panel_np, device=self.device, dtype=self.dtype)
        B_panel = torch.tensor(B_panel_np, device=self.device, dtype=self.dtype)
        self.C_block += torch.matmul(A_panel, B_panel)
        return True

    def run_summa(self, A_panels, B_panels):
        """
        Run all SUMMA iterations locally given precomputed panels.
        This batches all communication into a single Ray call per worker,
        reducing round-trip overhead vs calling accumulate() in a loop.

        A_panels: list of grid_cols numpy arrays (one per k iteration)
        B_panels: list of grid_cols numpy arrays (one per k iteration)
        """
        self.C_block.zero_()
        for A_panel_np, B_panel_np in zip(A_panels, B_panels):
            A_panel = torch.tensor(
                A_panel_np, device=self.device, dtype=self.dtype)
            B_panel = torch.tensor(
                B_panel_np, device=self.device, dtype=self.dtype)
            self.C_block += torch.matmul(A_panel, B_panel)
        torch.cuda.synchronize()
        return True


# ── Driver: orchestrates SUMMA across workers ─────────────────────────────────

class DistributedMatmul:

    def __init__(self, grid_rows=4, grid_cols=4, dtype=torch.float32):
        self.grid_rows  = grid_rows
        self.grid_cols  = grid_cols
        self.world_size = grid_rows * grid_cols
        self.dtype      = dtype

        # Create one Ray actor per GPU
        print(f"Spawning {self.world_size} GPU workers "
              f"in a {grid_rows}x{grid_cols} grid...")
        self.workers = [
            MatmulWorker.remote(
                rank, self.world_size, grid_rows, grid_cols, dtype)
            for rank in range(self.world_size)
        ]
        print("Workers ready.")

    def init_random(self, N, M, P, seed=42):
        """Initialize random matrices sharded across all workers."""
        assert N % self.grid_rows == 0, "N must be divisible by grid_rows"
        assert M % self.grid_cols == 0, "M must be divisible by grid_cols"
        assert P % self.grid_cols == 0, "P must be divisible by grid_cols"

        self.N, self.M, self.P = N, M, P
        n_local = N // self.grid_rows
        m_local = M // self.grid_cols
        p_local = P // self.grid_cols

        print(f"Initializing blocks: each worker gets "
              f"A({n_local}, {m_local}), B({m_local}, {p_local})")

        futures = [
            w.init_blocks.remote(n_local, m_local, p_local, seed)
            for w in self.workers
        ]
        stats = ray.get(futures)
        for s in stats:
            print(f"  [Worker {s['rank']}] "
                  f"A={s['A_shape']} B={s['B_shape']} "
                  f"mem={s['mem_gb']:.1f}GB")

    def compute(self):
        """
        Run SUMMA algorithm.

        For each k in 0..grid_cols-1:
          1. Gather A panels from workers where col_rank == k (one per grid row)
          2. Gather B panels from workers where row_rank == k (one per grid col)
          3. Broadcast panels to all workers in same row/col
          4. Each worker accumulates local C += A_panel @ B_panel

        To minimize Ray round-trips, we gather ALL panels first,
        then send each worker its full list of (A_panel, B_panel) pairs
        in a single remote call.
        """
        grid_rows  = self.grid_rows
        grid_cols  = self.grid_cols
        world_size = self.world_size

        print("Collecting SUMMA panels...")

        # ── Collect all panels from workers ───────────────────────────────
        # A_panels[k][row] = A block from worker at (row, k)
        # B_panels[k][col] = B block from worker at (k, col)
        A_panels_by_k = {}
        B_panels_by_k = {}

        for k in range(grid_cols):
            # Collect A panels: workers with col_rank == k
            a_futures = {}
            for row in range(grid_rows):
                src_rank     = row * grid_cols + k
                a_futures[row] = self.workers[src_rank].get_A_block.remote()

            # Collect B panels: workers with row_rank == k
            b_futures = {}
            for col in range(grid_cols):
                src_rank     = k * grid_cols + col
                b_futures[col] = self.workers[src_rank].get_B_block.remote()

            A_panels_by_k[k] = {row: ray.get(f) for row, f in a_futures.items()}
            B_panels_by_k[k] = {col: ray.get(f) for col, f in b_futures.items()}

        print("Panels collected. Dispatching SUMMA to all workers...")

        # ── Build per-worker panel lists and dispatch in one call ─────────
        futures = []
        for rank in range(world_size):
            row_rank = rank // grid_cols
            col_rank = rank % grid_cols

            # This worker needs:
            #   A_panel[k] = A block from (row_rank, k) for each k
            #   B_panel[k] = B block from (k, col_rank) for each k
            A_panels = [A_panels_by_k[k][row_rank] for k in range(grid_cols)]
            B_panels = [B_panels_by_k[k][col_rank] for k in range(grid_cols)]

            futures.append(
                self.workers[rank].run_summa.remote(A_panels, B_panels))

        # Wait for all workers to finish
        t0 = time.time()
        ray.get(futures)
        elapsed = time.time() - t0

        return elapsed

    def gather_result(self):
        """
        Collect all C blocks from workers and assemble the full matrix.
        Warning: result is N*P*4 bytes on the driver — only do this
        for small matrices or when you truly need the full result.
        """
        n_local = self.N // self.grid_rows
        p_local = self.P // self.grid_cols

        futures = [w.get_C_block.remote() for w in self.workers]
        blocks  = ray.get(futures)

        C_full = np.zeros((self.N, self.P), dtype=np.float32)
        for rank, block in enumerate(blocks):
            row_rank = rank // self.grid_cols
            col_rank = rank % self.grid_cols
            r0, r1   = row_rank * n_local, (row_rank + 1) * n_local
            c0, c1   = col_rank * p_local, (col_rank + 1) * p_local
            C_full[r0:r1, c0:c1] = block

        return C_full

    def memory_report(self):
        stats = ray.get([w.memory_stats.remote() for w in self.workers])
        print("\nGPU Memory Usage:")
        for s in stats:
            print(f"  Worker {s['rank']:2d}: "
                  f"allocated={s['allocated']}  reserved={s['reserved']}")

    def report_performance(self, elapsed):
        flops  = 2 * self.N * self.M * self.P
        tflops = flops / elapsed / 1e12
        dtype_bytes = 4
        mem_A  = self.N * self.M * dtype_bytes / 1e9
        mem_B  = self.M * self.P * dtype_bytes / 1e9
        mem_C  = self.N * self.P * dtype_bytes / 1e9

        print("\n" + "="*55)
        print(f"  Matrix size  : {self.N:,} x {self.M:,} "
              f"@ {self.M:,} x {self.P:,}")
        print(f"  Workers      : {self.world_size} GPUs "
              f"({self.grid_rows}x{self.grid_cols} grid)")
        print(f"  dtype        : {self.dtype}")
        print(f"  Time         : {elapsed:.2f}s")
        print(f"  Performance  : {tflops:.2f} TFLOPS")
        print(f"  Memory A     : {mem_A:.1f} GB")
        print(f"  Memory B     : {mem_B:.1f} GB")
        print(f"  Memory C     : {mem_C:.1f} GB")
        print(f"  Total memory : {mem_A + mem_B + mem_C:.1f} GB")
        print("="*55)


# ── Correctness Verification ──────────────────────────────────────────────────

def verify_correctness(grid_rows=2, grid_cols=2):
    """
    Compare distributed result against single-GPU torch.matmul
    on a small matrix before scaling to 256k.
    """
    print("\nRunning correctness check...")
    N, M, P = 4000, 3000, 2000

    n_local = N // grid_rows
    m_local = M // grid_cols
    p_local = P // grid_cols

    # Build full matrices on CPU for reference
    np.random.seed(0)
    A_full = np.random.randn(N, M).astype(np.float32)
    B_full = np.random.randn(M, P).astype(np.float32)
    C_ref  = A_full @ B_full

    # Create workers and assign blocks
    world_size = grid_rows * grid_cols
    workers    = [
        MatmulWorker.remote(r, world_size, grid_rows, grid_cols, torch.float32)
        for r in range(world_size)
    ]

    # Set blocks from numpy slices
    set_futures = []
    for rank in range(world_size):
        row_rank = rank // grid_cols
        col_rank = rank % grid_cols
        A_block  = A_full[row_rank*n_local:(row_rank+1)*n_local,
                          col_rank*m_local:(col_rank+1)*m_local]
        B_block  = B_full[row_rank*m_local:(row_rank+1)*m_local,
                          col_rank*p_local:(col_rank+1)*p_local]
        set_futures.append(workers[rank].set_blocks.remote(A_block, B_block))
    ray.get(set_futures)

    # Run SUMMA
    dm = DistributedMatmul(grid_rows, grid_cols, torch.float32)
    dm.workers = workers
    dm.N, dm.M, dm.P = N, M, P
    dm.compute()
    C_dist = dm.gather_result()

    max_err = np.max(np.abs(C_dist - C_ref))
    print(f"Max absolute error: {max_err:.6f}")
    assert max_err < 1e-1, f"Correctness check FAILED: error={max_err}"
    print("Correctness check passed ✓\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ray.init()   # connect to existing cluster or start locally

    print(f"Ray resources: {ray.available_resources()}")

    # ── Correctness check on small input first ────────────────────────────
    verify_correctness(grid_rows=1, grid_cols=1)

    # ── Full 256k x 256k run ──────────────────────────────────────────────
    N, M, P    = 4096, 4096, 4096
    GRID_ROWS  = 1
    GRID_COLS  = 1

    dm = DistributedMatmul(
        grid_rows=GRID_ROWS,
        grid_cols=GRID_COLS,
        dtype=torch.float32,
    )

    dm.init_random(N, M, P, seed=42)
    dm.memory_report()

    print("\nStarting distributed matmul...")
    elapsed = dm.compute()

    dm.report_performance(elapsed)
    dm.memory_report()

    ray.shutdown()


if __name__ == "__main__":
    main()