import os
import time
import torch
import torch.distributed as dist


# ── Process grid helpers ──────────────────────────────────────────────────────

def setup_process_grid(grid_rows, grid_cols):
    """
    Create row and column communication groups for the 2D process grid.
    Returns (row_group, col_group, row_rank, col_rank)
    """
    rank = dist.get_rank()

    row_rank = rank // grid_cols   # which row in the grid
    col_rank = rank % grid_cols    # which col in the grid

    # All ranks in the same grid row communicate together
    row_groups = []
    for r in range(grid_rows):
        members = [r * grid_cols + c for c in range(grid_cols)]
        group   = dist.new_group(ranks=members)
        if row_rank == r:
            my_row_group = group
        row_groups.append(group)

    # All ranks in the same grid column communicate together
    col_groups = []
    for c in range(grid_cols):
        members = [r * grid_cols + c for r in range(grid_rows)]
        group   = dist.new_group(ranks=members)
        if col_rank == c:
            my_col_group = group
        col_groups.append(group)

    return my_row_group, my_col_group, row_rank, col_rank


# ── SUMMA algorithm ───────────────────────────────────────────────────────────

def summa_matmul(A_block, B_block, row_group, col_group, row_rank, col_rank, grid_cols):
    """
    SUMMA: Scalable Universal Matrix Multiply Algorithm.

    A(n, m) @ B(m, p) = C(n, p)

    Each GPU holds:
      A_block: (n/grid_rows, m/grid_cols)
      B_block: (m/grid_rows, p/grid_cols)

    Steps per iteration k (0..grid_cols-1):
      1. Root of row broadcast: rank with col_rank == k broadcasts its A block
         to all ranks in the same row group
      2. Root of col broadcast: rank with row_rank == k broadcasts its B block
         to all ranks in the same col group
      3. All ranks do local matmul and accumulate into C_block
    """
    device   = A_block.device
    n_local  = A_block.shape[0]
    p_local  = B_block.shape[1]
    m_local  = A_block.shape[1]   # == B_block.shape[0]

    C_block = torch.zeros(n_local, p_local, device=device, dtype=A_block.dtype)

    for k in range(grid_cols):
        # ── Broadcast a panel of A along this rank's row ──────────────────
        if col_rank == k:
            A_panel = A_block.contiguous()
        else:
            A_panel = torch.zeros(n_local, m_local, device=device, dtype=A_block.dtype)

        # Broadcast from the rank in this row whose col_rank == k
        row_src = dist.get_global_rank(row_group, k)
        dist.broadcast(A_panel, src=row_src, group=row_group)

        # ── Broadcast a panel of B along this rank's column ───────────────
        if row_rank == k:
            B_panel = B_block.contiguous()
        else:
            B_panel = torch.zeros(m_local, p_local, device=device, dtype=B_block.dtype)

        col_src = dist.get_global_rank(col_group, k)
        dist.broadcast(B_panel, src=col_src, group=col_group)

        # ── Local matrix multiply and accumulate ──────────────────────────
        C_block += torch.matmul(A_panel, B_panel)

    return C_block


# ── Verification: compare against single-GPU result on small input ────────────

def verify_result(C_block, A_block, B_block, row_rank, col_rank,
                  grid_rows, grid_cols, n, m, p):
    rank       = dist.get_rank()
    world_size = dist.get_world_size()
    device     = C_block.device

    # Gather all blocks on rank 0 for verification
    C_flat = C_block.contiguous().view(-1)
    all_C  = [torch.zeros_like(C_flat) for _ in range(world_size)]
    dist.all_gather(all_C, C_flat)

    if rank == 0:
        n_local = n // grid_rows
        p_local = p // grid_cols
        # Reconstruct C from blocks
        C_full = torch.zeros(n, p, device=device)
        for r in range(grid_rows):
            for c in range(grid_cols):
                idx   = r * grid_cols + c
                block = all_C[idx].view(n_local, p_local)
                C_full[r*n_local:(r+1)*n_local, c*p_local:(c+1)*p_local] = block

        # Gather A and B blocks similarly
        A_flat = A_block.contiguous().view(-1)
        B_flat = B_block.contiguous().view(-1)
        all_A  = [torch.zeros_like(A_flat) for _ in range(world_size)]
        all_B  = [torch.zeros_like(B_flat) for _ in range(world_size)]
        dist.all_gather(all_A, A_flat)
        dist.all_gather(all_B, B_flat)

        m_local = m // grid_cols
        A_full  = torch.zeros(n, m, device=device)
        B_full  = torch.zeros(m, p, device=device)
        for r in range(grid_rows):
            for c in range(grid_cols):
                idx = r * grid_cols + c
                A_full[r*n_local:(r+1)*n_local, c*m_local:(c+1)*m_local] = \
                    all_A[idx].view(n_local, m_local)
                B_full[r*m_local:(r+1)*m_local, c*p_local:(c+1)*p_local] = \
                    all_B[idx].view(m_local, p_local)

        C_ref = torch.matmul(A_full, B_full)
        err   = torch.max(torch.abs(C_full - C_ref)).item()
        print(f"Max absolute error vs reference: {err:.6f}")
        return err < 1e-2
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dist.init_process_group(backend="nccl")

    rank       = dist.get_rank()
    world_size = dist.get_world_size()
    device     = torch.device(f"cuda:{int(os.environ['LOCAL_RANK'])}")
    torch.cuda.set_device(device)

    # 2D process grid: 4x4 for 16 GPUs
    # grid_rows = 4
    # grid_cols = 4
    grid_rows = 1
    grid_cols = 1
    assert grid_rows * grid_cols == world_size, \
        f"Grid {grid_rows}x{grid_cols} doesn't match world_size {world_size}"

    # ── Matrix dimensions ─────────────────────────────────────────────────
    N = 4096
    M = 4096
    P = 4096

    assert N % grid_rows == 0 and M % grid_cols == 0 and P % grid_cols == 0, \
        "Matrix dimensions must be divisible by grid size"

    n_local = N // grid_rows
    m_local = M // grid_cols
    p_local = P // grid_cols

    row_rank = rank // grid_cols
    col_rank = rank % grid_cols

    if rank == 0:
        mem_per_block_gb = (n_local * m_local * 4) / 1e9   # float32
        print(f"Grid: {grid_rows}x{grid_cols}")
        print(f"Block size per GPU: ({n_local}, {m_local})")
        print(f"Memory per block (fp32): {mem_per_block_gb:.1f} GB")

    # ── Initialize blocks ──────────────────
    # fp32: each block ≈ 64k*64k*4 bytes = 16GB, well within 80GB A100
    A_block = torch.randn(n_local, m_local, device=device, dtype=torch.float32)
    B_block = torch.randn(m_local, p_local, device=device, dtype=torch.float32)

    # ── Setup communication groups ────────────────────────────────────────
    row_group, col_group, row_rank, col_rank = setup_process_grid(grid_rows, grid_cols)
    
    C_block = summa_matmul(A_block, B_block, row_group, col_group,
                           row_rank, col_rank, grid_cols)
    verify_result(C_block, A_block, B_block, row_rank, col_rank,
                  grid_rows, grid_cols, N, M, P)

    dist.barrier()

    # ── Warmup pass ───────────────────────────────────────────────────────
    if rank == 0:
        print("Warming up...")
    _ = summa_matmul(A_block, B_block, row_group, col_group,
                     row_rank, col_rank, grid_cols)
    torch.cuda.synchronize()
    dist.barrier()

    # ── Timed run ─────────────────────────────────────────────────────────
    start_event = torch.cuda.Event(enable_timing=True)
    end_event   = torch.cuda.Event(enable_timing=True)

    dist.barrier()
    start_event.record()

    C_block = summa_matmul(A_block, B_block, row_group, col_group,
                           row_rank, col_rank, grid_cols)

    end_event.record()
    torch.cuda.synchronize()
    elapsed_ms = start_event.elapsed_time(end_event)

    # ── Report performance ────────────────────────────────────────────────
    if rank == 0:
        elapsed_s = elapsed_ms / 1000
        # FLOPs for matmul: 2 * N * M * P
        flops     = 2 * N * M * P
        tflops    = flops / elapsed_s / 1e12
        print(f"\nResults:")
        print(f"  Matrix size : {N}x{M} @ {M}x{P}")
        print(f"  Time        : {elapsed_s:.2f}s")
        print(f"  Performance : {tflops:.1f} TFLOPS")
        print(f"  C_block shape on rank 0: {C_block.shape}")

    dist.destroy_process_group()


if __name__ == "__main__":
    main()