import cupy as cp
import cupyx.scipy.sparse as cpsp
import dask.array as da
import dask
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster
import sys
import os
import time

def make_sparse_block(block_size, density, seed):
    """Generate sparse CSR block directly on GPU — no dense intermediate."""
    nrows = ncols = block_size
    nnz   = max(1, int(nrows * ncols * density))
    rng   = cp.random.RandomState(seed)
    rows  = rng.randint(0, nrows, size=nnz).astype(cp.int32)
    cols  = rng.randint(0, ncols, size=nnz).astype(cp.int32)
    data  = rng.normal(1.0, 1.0, size=nnz).astype(cp.float32)
    return cpsp.csr_matrix(
        (data, (rows, cols)),
        shape=(nrows, ncols),
        dtype=cp.float32,
    )

def make_dense_block(block_size, seed):
    """Generate a dense block directly on GPU."""
    rng = cp.random.RandomState(seed)
    return rng.normal(1.0, 1.0, size=(block_size, block_size)).astype(cp.float32)

def sparse_dense_block_dot(A_sparse, B_dense):
    """
    Multiply one sparse CSR block by one dense block.
    A_sparse : (block_size x block_size) CSR
    B_dense  : (block_size x block_size) dense fp32
    Returns  : (block_size x block_size) dense fp32

    CuPy uses cuSPARSE SpMM kernel — much faster than SpGEMM
    and no large intermediate buffer allocation.
    """
    result = A_sparse.dot(B_dense)   # SpMM: sparse @ dense → dense
    cp.cuda.Stream.null.synchronize()
    return result.astype(cp.float32)

def compute_output_block(
    A_blocks : list,   # list of K sparse CSR blocks: A[i, 0..K-1]
    B_blocks : list,   # list of K dense blocks:      B[0..K-1, j]
) -> cp.ndarray:
    """
    C[i,j] = sum_k A[i,k] @ B[k,j]

    Receives concrete block lists — no Dask arrays inside delayed.
    Uses SpMM (sparse @ dense) — no large intermediate buffers.
    Accumulates one k step at a time to keep memory constant.
    """
    acc = None
    for A_blk, B_blk in zip(A_blocks, B_blocks):
        # SpMM: sparse CSR @ dense → dense
        contrib = A_blk.dot(B_blk).astype(cp.float32)
        if acc is None:
            acc = contrib
        else:
            acc += contrib
        del contrib

    cp.get_default_memory_pool().free_all_blocks()
    return acc

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <N> <M>")
        sys.exit(1)

    N          = int(sys.argv[1])
    M          = int(sys.argv[2])
    block_size = 4096
    density    = 0.01
    n_blocks = N // block_size
    m_blocks = M // block_size

    # cluster = LocalCUDACluster()
    client = Client(os.environ["ip_head"])
    print(f"Dashboard  : {client.dashboard_link}")
    print(f"Workers    : {len(client.scheduler_info()['workers'])}")
    print(f"N={N} M={M}  block_size={block_size}  "
          f"n_blocks={n_blocks} m_blocks={m_blocks}  density={density}\n")

    print("Generating sparse matrix xs...")
    t0 = time.time()

    xs_futures = [
        [client.submit(
            make_sparse_block,
            block_size,
            density,
            seed = i * n_blocks + k,
            pure = False,
        ) for k in range(n_blocks)]
        for i in range(n_blocks)
    ]

    all_sparse = [f for row in xs_futures for f in row]
    wait(all_sparse)
    print(f"  xs ready: {n_blocks}x{n_blocks} sparse CSR blocks "
          f"in {time.time()-t0:.2f}s")

    print("Generating dense matrix ys...")
    t1 = time.time()

    ys_futures = [
        [client.submit(
            make_dense_block,
            block_size,
            seed = 10_000 + k * m_blocks + j,
            pure = False,
        ) for j in range(m_blocks)]
        for k in range(n_blocks)
    ]

    all_dense = [f for row in ys_futures for f in row]
    wait(all_dense)
    print(f"  ys ready: {n_blocks}x{m_blocks} dense blocks "
          f"in {time.time()-t1:.2f}s\n")

    output_futures = [
        [client.submit(
            compute_output_block,
            [xs_futures[i][k] for k in range(n_blocks)],  # A[i,*] futures
            [ys_futures[k][j] for k in range(n_blocks)],  # B[*,j] futures
            pure=False,
        ) for j in range(m_blocks)]
        for i in range(n_blocks)
    ]

    start      = time.time()
    all_output = [f for row in output_futures for f in row]
    wait(all_output)
    elapsed    = time.time() - start
    print(f"\nTime     : {elapsed:.2f}s")

    results = client.gather(all_output)
    grid    = [
        results[i * m_blocks:(i+1) * m_blocks]
        for i in range(n_blocks)
    ]
    elapsed    = time.time() - start
    print(f"\nTime to gather : {elapsed:.2f}s")
    print(f"C[0,0] top-left 4x4:\n{cp.asnumpy(grid[0][0][:4, :4])}")

    nnz    = int(N * N * density)
    flops  = 2 * nnz * N
    gflops = flops / elapsed / 1e9
    print(f"\nN        : {N:,}")
    print(f"density  : {density}")
    print(f"nnz      : {nnz:,}")
    print(f"GFLOPS   : {gflops:.2f}")

    client.close()
    # cluster.close()
