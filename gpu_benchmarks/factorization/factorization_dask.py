import cupy as cp
import cupyx.scipy.sparse as cpsp
import dask
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster
import sys
import os
import time

def make_sparse_R_block(nrows, ncols, density, seed):
    nnz  = max(1, int(nrows * ncols * density))
    rng  = cp.random.RandomState(seed)
    rows = rng.randint(0, nrows, size=nnz).astype(cp.int32)
    cols = rng.randint(0, ncols, size=nnz).astype(cp.int32)
    data = rng.uniform(0.0, 1.0, size=nnz).astype(cp.float32)
    return cpsp.csr_matrix(
        (data, (rows, cols)),
        shape=(nrows, ncols),
        dtype=cp.float32,
    )

def make_P_block(block_rows, D, seed):
    rng = cp.random.RandomState(seed)
    return rng.uniform(0.0, 1.0, size=(block_rows, D)).astype(cp.float32)


def make_Q_block(D, block_cols, N, seed):
    rng = cp.random.RandomState(seed)
    return (rng.uniform(0.0, 1.0, size=(D, block_cols)) / N).astype(cp.float32)

def hstack_dense(blocks):
    """Hstack a list of dense CuPy blocks → (D x M). Runs on worker."""
    return cp.hstack(blocks)


def vstack_dense(blocks):
    """Vstack a list of dense CuPy blocks → (N x D). Runs on worker."""
    return cp.vstack(blocks)


def extract_col_stripe(R_stripes, j, block_cols):
    """
    Extract column stripe j from list of row-stripe CSR blocks.
    All data stays on workers — no driver involvement.
    """
    col_start = j * block_cols
    col_end   = col_start + block_cols
    parts     = [stripe[:, col_start:col_end] for stripe in R_stripes]
    return cpsp.vstack(parts, format="csr")

def update_P_block(R_stripe, P_block, Q_full, alpha, beta):
    """
    P[i] = P[i]*(1 - alpha*beta) + 2*alpha * (R[i] - P[i]@Q) @ Q.T

    R_stripe : (block_rows x M) sparse CSR  — on worker
    P_block  : (block_rows x D) dense       — on worker
    Q_full   : (D x M)          dense       — assembled on worker, not driver
    """
    E1 = P_block @ Q_full                          # (block_rows x M) dense
    E2 = R_stripe - cpsp.csr_matrix(E1)            # sparse residual
    P1 = cp.float32(2.0 * alpha) * E2.dot(Q_full.T)  # SpMM → (block_rows x D)
    return P_block * cp.float32(1.0 - alpha * beta) + P1

def update_Q_block(R_col_stripe, P_full, Q_block, alpha, beta):
    """
    Q[j] = Q[j]*(1 - alpha*beta) + 2*alpha * (R[:,j] - P@Q[j]).T @ P

    R_col_stripe : (N x block_cols) sparse CSR — on worker
    P_full       : (N x D)          dense      — assembled on worker
    Q_block      : (D x block_cols) dense      — on worker
    """
    E1 = P_full @ Q_block                              # (N x block_cols) dense
    E2 = R_col_stripe - cpsp.csr_matrix(E1)            # sparse residual
    Q1 = cp.float32(2.0 * alpha) * E2.T.dot(P_full)   # (block_cols x D)
    Q1 = Q1.T                                          # (D x block_cols)
    return Q_block * cp.float32(1.0 - alpha * beta) + Q1


def compute_loss(R_stripe, P_block, Q_full, beta):
    """Reconstruction loss for convergence monitoring — runs on worker."""
    E1   = P_block @ Q_full
    E2   = R_stripe - cpsp.csr_matrix(E1)
    loss = float(E2.power(2).sum())
    loss += beta * (float(cp.sum(P_block ** 2)) +
                    float(cp.sum(Q_full  ** 2)))
    return loss

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: python {sys.argv[0]} <N> <M> <D> <iterations>")
        sys.exit(1)

    N          = int(sys.argv[1])
    M          = int(sys.argv[2])
    D          = int(sys.argv[3])
    iterations = int(sys.argv[4])
    sparsity   = 0.01
    alpha      = 0.002
    beta       = 0.02

    block_rows = min(4096, N)
    block_cols = min(4096, M)
    while N % block_rows != 0: block_rows -= 1
    while M % block_cols != 0: block_cols -= 1
    num_row_blocks = N // block_rows
    num_col_blocks = M // block_cols

    # cluster = LocalCUDACluster()
    client    = Client(os.environ.get("ip_head", None))
    n_workers = len(client.scheduler_info()["workers"])
    print(f"Dashboard      : {client.dashboard_link}")
    print(f"Workers        : {n_workers}")
    print(f"N={N} M={M} D={D} iterations={iterations}")
    print(f"block_rows={block_rows} num_row_blocks={num_row_blocks}")
    print(f"block_cols={block_cols} num_col_blocks={num_col_blocks}\n")

    # Initialize R, P, Q as futures on workers

    print("Initializing R, P, Q on workers...")
    t0 = time.time()

    def make_R_stripe(i, block_rows, M, num_col_blocks, block_cols, density):
        col_blocks = [
            make_sparse_R_block(block_rows, block_cols, density,
                                seed=i * num_col_blocks + k)
            for k in range(num_col_blocks)
        ]
        return cpsp.hstack(col_blocks, format="csr")

    R_futures = [
        client.submit(make_R_stripe, i, block_rows, M,
                      num_col_blocks, block_cols, sparsity, pure=False)
        for i in range(num_row_blocks)
    ]
    P_futures = [
        client.submit(make_P_block, block_rows, D,
                      seed=10_000 + i, pure=False)
        for i in range(num_row_blocks)
    ]
    Q_futures = [
        client.submit(make_Q_block, D, block_cols, N,
                      seed=20_000 + j, pure=False)
        for j in range(num_col_blocks)
    ]

    wait(R_futures + P_futures + Q_futures)
    print(f"  Done in {time.time()-t0:.2f}s\n")

    # Main loop — all assembly and updates stay on workers

    print(f"Running {iterations} iterations...")
    t_total = time.time()

    for it in range(iterations):
        t_it = time.time()

        # Assemble Q_full on a worker
        # hstack_dense receives Q block futures — Dask ships them
        # worker-to-worker, never through the driver
        Q_full_future = client.submit(
            hstack_dense,
            Q_futures,       # list of futures → Dask resolves on worker
            pure=False,
        )

        # Update all P blocks in parallel
        # Each P[i] depends on Q_full_future — Dask reuses the same
        # future reference without re-sending data
        P_futures = [
            client.submit(
                update_P_block,
                R_futures[i],
                P_futures[i],
                Q_full_future,   # future ref — not the actual array
                alpha, beta,
                pure=False,
            )
            for i in range(num_row_blocks)
        ]
        wait(P_futures)

        # Assemble P_full on a worker
        P_full_future = client.submit(
            vstack_dense,
            P_futures,       # list of futures resolved on worker
            pure=False,
        )

        # Extract R column stripes on workers
        # R row stripes are already futures — extract_col_stripe
        # receives them as a list and runs entirely on a worker
        R_col_futures = [
            client.submit(
                extract_col_stripe,
                R_futures,       # list of row-stripe futures
                j, block_cols,
                pure=False,
            )
            for j in range(num_col_blocks)
        ]
        wait(R_col_futures)

        # Update all Q blocks in parallel
        Q_futures = [
            client.submit(
                update_Q_block,
                R_col_futures[j],
                P_full_future,   # future ref — reused across all Q updates
                Q_futures[j],
                alpha, beta,
                pure=False,
            )
            for j in range(num_col_blocks)
        ]
        wait(Q_futures)

        # Convergence check — loss computed on workers
        if it % max(1, iterations // 10) == 0:
            loss_futures = [
                client.submit(
                    compute_loss,
                    R_futures[i],
                    P_futures[i],
                    Q_full_future,   # reuse same future — no resend
                    beta,
                    pure=False,
                )
                for i in range(num_row_blocks)
            ]
            # Only gather the scalar loss values — tiny, not a warning risk
            losses     = client.gather(loss_futures)
            total_loss = sum(losses)
            print(f"  iter {it+1:4d}/{iterations}  "
                  f"loss={total_loss:.4f}  "
                  f"({time.time()-t_it:.3f}s/iter)")

    elapsed = time.time() - t_total
    print(f"\nDone in {elapsed:.2f}s  "
          f"({elapsed/iterations:.3f}s/iter)\n")

    # Gather Q at the end — only once, outside the loop
    Q_blocks = client.gather(Q_futures)
    Q_full   = cp.hstack(Q_blocks)

    print(f"Q shape  : {Q_full.shape}")
    print(f"Q mean   : {float(Q_full.mean()):.6f}")
    print(f"Q std    : {float(Q_full.std()):.6f}")
    print(f"Q[:4,:4] :\n{cp.asnumpy(Q_full[:4, :4])}")

    client.close()
    # cluster.close()
