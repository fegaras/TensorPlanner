import cupy as cp
import cupyx.scipy.sparse as cpsp
import dask
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster
import sys
import os
import time

def make_sparse_adjacency_block(nrows, ncols, density, seed):
    """
    Generate one block of the adjacency matrix Gz directly as CSR.
    Values are 1.0 where random() < sparsity — matches the original:
      Gz = tensor*(N)(N)[ ((i,j),1.0) | i,j, random() < sparsity ]
    """
    nnz  = max(1, int(nrows * ncols * density))
    rng  = cp.random.RandomState(seed)
    rows = rng.randint(0, nrows, size=nnz).astype(cp.int32)
    cols = rng.randint(0, ncols, size=nnz).astype(cp.int32)
    data = cp.ones(nnz, dtype=cp.float32)
    return cpsp.csr_matrix(
        (data, (rows, cols)),
        shape=(nrows, ncols),
        dtype=cp.float32,
    )


def compute_out_degree_block(G_block):
    """
    Compute out-degree for each row in this block.
    Cz = tensor*(N) [ (i, +/v) | ((i,j),v) <- Gz, group by i ]
    Returns 1D array of row sums.
    """
    return cp.asarray(G_block.sum(axis=1)).ravel()


def normalize_block(G_block, C_local, row_offset):
    """
    Normalize each row of G by its out-degree to get transition matrix E.
    Ez = tensor*(N)(N) [ ((i,j), 1.0/c) | ((i,j),v) <- Gz, c = Cz[i] ]

    Rows with zero out-degree get uniform probability (teleportation only).
    C_local covers rows [row_offset .. row_offset+nrows).
    """
    nrows = G_block.shape[0]

    # Safe inverse: 1/c where c > 0, else 0
    inv_c = cp.where(C_local > 0,
                     cp.float32(1.0) / C_local,
                     cp.float32(0.0))

    # Scale each row of the sparse matrix by inv_c
    # Equivalent to diag(inv_c) @ G_block
    D = cpsp.diags(inv_c, format="csr", dtype=cp.float32)
    return D.dot(G_block)


def spmv_block(E_block, P_chunk):
    """
    Sparse matrix-vector product for one row block of E.t @ P.

    In PageRank: Ez.t @ (b * Pz)
    Ez.t has shape (N, N), so Ez.t[col_start:col_end, :] @ P
    = Ez[:, col_start:col_end].T @ P

    E_block  : (block_rows x N) CSR — one row stripe of E
    P_chunk  : (block_rows,) dense — the matching chunk of P
    Returns  : (N,) partial contribution to the full output
    """
    # E_block.T @ P_chunk: shape (N,) contribution
    return E_block.T.dot(P_chunk)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <N> <iterations>")
        sys.exit(1)

    N          = int(sys.argv[1])
    iterations = int(sys.argv[2])
    b          = cp.float32(0.85)
    sparsity   = 0.01
    block_size = min(4096, N)
    num_blocks = N // block_size

    if N % block_size != 0:
        # Adjust block_size to divide N evenly
        for bs in range(4096, 0, -1):
            if N % bs == 0:
                block_size = bs
                num_blocks = N // block_size
                break

    # cluster = LocalCUDACluster()
    client = Client(os.environ["ip_head"])
    n_workers = len(client.scheduler_info()["workers"])
    print(f"Dashboard  : {client.dashboard_link}")
    print(f"Workers    : {n_workers}")
    print(f"N={N}  iterations={iterations}  "
          f"block_size={block_size}  "
          f"num_blocks={num_blocks}  "
          f"sparsity={sparsity}\n")

    # Gz[i,j] = 1.0 if random() < sparsity
    # Stored as num_blocks row stripes, each (block_size x N)
    print("Building Gz (sparse adjacency)...")
    t0 = time.time()

    # Each row stripe i is assembled from num_blocks column blocks
    def make_row_stripe(i, block_size, N, num_blocks, density):
        """Build full row stripe i of Gz by hstacking column blocks."""
        col_blocks = []
        for k in range(num_blocks):
            seed = i * num_blocks + k
            blk  = make_sparse_adjacency_block(
                block_size, block_size, density, seed)
            col_blocks.append(blk)
        return cpsp.hstack(col_blocks, format="csr")

    Gz_futures = [
        client.submit(
            make_row_stripe,
            i, block_size, N, num_blocks, sparsity,
            pure=False,
        )
        for i in range(num_blocks)
    ]
    wait(Gz_futures)
    print(f"  Gz ready in {time.time()-t0:.2f}s  "
          f"({num_blocks} row stripes of shape "
          f"({block_size} x {N}))")

    # Cz[i] = sum_j Gz[i,j]   (row sums)
    print("Computing Cz (out-degrees)...")

    def row_sums(G_stripe):
        return cp.asarray(G_stripe.sum(axis=1)).ravel()

    Cz_futures = [
        client.submit(row_sums, Gz_futures[i], pure=False)
        for i in range(num_blocks)
    ]
    wait(Cz_futures)

    # Ez[i,j] = Gz[i,j] / Cz[i]   (each row sums to 1 for non-dangling)
    print("Computing Ez (row-normalized transition matrix)...")

    def normalize_stripe(G_stripe, C_chunk):
        inv_c = cp.where(C_chunk > 0,
                         cp.float32(1.0) / C_chunk,
                         cp.float32(0.0))
        D = cpsp.diags(inv_c, format="csr", dtype=cp.float32)
        return D.dot(G_stripe)

    Ez_futures = [
        client.submit(
            normalize_stripe,
            Gz_futures[i],
            Cz_futures[i],
            pure=False,
        )
        for i in range(num_blocks)
    ]
    wait(Ez_futures)
    print(f"  Ez ready in {time.time()-t0:.2f}s\n")

    # Pz[i] = 1/N  for all i
    # Stored as num_blocks chunks of shape (block_size,)
    Pz_futures = [
        client.submit(
            lambda bs, n: cp.full(bs, cp.float32(1.0 / n), dtype=cp.float32),
            block_size, N,
            pure=False,
        )
        for _ in range(num_blocks)
    ]
    wait(Pz_futures)
    print(f"Pz initialized: {num_blocks} chunks of {block_size} elements\n")

    # Pz = Ez.t @ (b * Pz) + (1-b)/N
    #
    # Ez.t @ v means: for each column j of Ez, dot with v
    # Since Ez is stored as row stripes Ez[i] of shape (block_size x N):
    #   Ez.t @ v = sum_i  Ez[i].T @ v[i*bs:(i+1)*bs]
    # Each Ez[i].T has shape (N x block_size), v chunk has shape (block_size,)
    # Result: sum of (N,) vectors = (N,) output

    def partial_transpose_spmv(E_stripe, P_chunk):
        """
        Contribution of stripe i to Ez.t @ P:
          E_stripe.T @ P_chunk  →  shape (N,)
        """
        return E_stripe.T.dot(P_chunk)

    def sum_contributions(partials):
        """Sum list of (N,) partial SpMV results."""
        result = partials[0].copy()
        for p in partials[1:]:
            result += p
        return result

    def apply_damping(v, b, N):
        """
        Final PageRank update:
          Pz = b * Ez.t @ Pz + (1-b)/N
        """
        return b * v + cp.float32((1.0 - float(b)) / N)

    def split_into_chunks(v, num_blocks, block_size):
        """Split a full (N,) vector into num_blocks chunks."""
        return [v[i*block_size:(i+1)*block_size].copy()
                for i in range(num_blocks)]

    print(f"Running {iterations} PageRank iterations...")
    t_iter = time.time()

    for it in range(iterations):
        t_it = time.time()

        # Each stripe i contributes Ez[i].T @ Pz[i] → shape (N,)
        partial_futures = [
            client.submit(
                partial_transpose_spmv,
                Ez_futures[i],
                Pz_futures[i],
                pure=False,
            )
            for i in range(num_blocks)
        ]
        wait(partial_futures)

        # Sum all partial contributions → full (N,) vector
        partials = client.gather(partial_futures)

        # Summation and damping on worker 0
        workers  = list(client.scheduler_info()["workers"].keys())
        Pz_new_f = client.submit(
            sum_contributions,
            partials,
            pure=False,
        )
        wait([Pz_new_f])

        # Apply damping: b * (Ez.t @ Pz) + (1-b)/N
        Pz_new_f = client.submit(
            apply_damping,
            Pz_new_f,
            float(b),
            N,
            pure=False,
        )
        wait([Pz_new_f])

        # Split back into chunks for next iteration
        Pz_full    = client.gather(Pz_new_f)
        Pz_futures = [
            client.submit(
                lambda v, i, bs=block_size: v[i*bs:(i+1)*bs].copy(),
                Pz_full, i,
                pure=False,
            )
            for i in range(num_blocks)
        ]
        wait(Pz_futures)

        if it % max(1, iterations // 10) == 0:
            print(f"  iter {it+1:4d}/{iterations}  "
                  f"({time.time()-t_it:.3f}s/iter)")

    total_iter = time.time() - t_iter
    print(f"\nAll iterations done in {total_iter:.2f}s  "
          f"({total_iter/iterations:.3f}s/iter)\n")

    Pz_chunks = client.gather(Pz_futures)
    Pz_full   = cp.concatenate(Pz_chunks)

    print(f"Pz sum (should be ~1.0) : {float(Pz_full.sum()):.6f}")
    print(f"Pz max                  : {float(Pz_full.max()):.6f}")
    print(f"Pz min                  : {float(Pz_full.min()):.6f}")
    print(f"Top-5 node ranks:")
    top5 = cp.argsort(Pz_full)[-5:][::-1]
    for rank, node in enumerate(top5.tolist()):
        print(f"  rank {rank+1}: node {node:6d}  score={float(Pz_full[node]):.6f}")

    client.close()
    # cluster.close()
