#include <iostream>
#include <mpi.h>
#include <vector>
#include <mkl_scalapack.h> // Include Intel Scalapack header

extern "C" {
    /* blacs declarations */
    void Cblacs_pinfo(int*, int*);
    void Cblacs_get(int, int, int*);
    void Cblacs_gridinit(int*, const char*, int, int);
    void Cblacs_pcoord(int, int, int*, int*);
    void Cblacs_gridexit(int);
    void Cblacs_barrier(int, const char*);

    MKL_INT numroc_(const MKL_INT*, const MKL_INT*, const MKL_INT*, const MKL_INT*, const MKL_INT*);

    void pdgemm_(char const *transa, char const *transb, const MKL_INT* M, const MKL_INT* N, const MKL_INT* K,
            const double* ALPHA,  double * A, const MKL_INT* IA, const MKL_INT* JA, MKL_INT * DESCA,
            double * B, const MKL_INT* IB, const MKL_INT* JB, MKL_INT * DESCB,
            const double* BETA, double * C, const MKL_INT* IC, const MKL_INT* JC, MKL_INT * DESCC );
    void descinit_(MKL_INT* desc, const MKL_INT* m, const MKL_INT* n, const MKL_INT* bm, const MKL_INT* bn, 
            const MKL_INT* rsrc, const MKL_INT* csrc, const MKL_INT* ctxt, const MKL_INT* lda, MKL_INT* info);
}

int main(int argc, char **argv) {
    const MKL_INT n = 10000, iZero = 0, iOne = 1; // Matrix size
    const MKL_INT nb = 1000; // Block size (adjust as needed)

    // Initialize MPI (required for Scalapack)
    MKL_INT myid, myrow, mycol, rank, procs;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &procs);

    // Create a 2D process grid
    MKL_INT dims[2] = {0, 0};
    MPI_Dims_create(procs, 2, dims);
    const MKL_INT prow = dims[0];
    const MKL_INT pcol = dims[1];

    // Initialize BLACS context
    MKL_INT icontxt;
    Cblacs_pinfo(&rank, &procs);
    Cblacs_get(0, 0, &icontxt);
    Cblacs_gridinit(&icontxt, "R", prow, pcol);
    Cblacs_pcoord(icontxt, myid, &myrow, &mycol);

    // Allocate local memory for matrix A, B, and C
    MKL_INT myArows, myAcols, myBrows, myBcols, myCrows, myCcols;
    myArows = numroc_(&n, &nb, &myrow, &iZero, &prow);
    myAcols = numroc_(&n, &nb, &mycol, &iZero, &pcol);

    myBrows = numroc_(&n, &nb, &myrow, &iZero, &prow);
    myBcols = numroc_(&n, &nb, &mycol, &iZero, &pcol);

    myCrows = numroc_(&n, &nb, &myrow, &iZero, &prow);
    myCcols = numroc_(&n, &nb, &mycol, &iZero, &pcol);

    std::vector<double> myA(myArows * myAcols);
    std::vector<double> myB(myBrows * myBcols);
    std::vector<double> myC(myCrows * myCcols);

    // Fill myA and myB with data (you can distribute them as needed)

    // Perform matrix multiplication
    const char trans = 'N';
    const double alpha = 1.0;
    const double beta = 0.0;
    const MKL_INT irsrc = 0;
    const MKL_INT icsrc = 0;
    MKL_INT descA[9], descB[9], descC[9];
    MKL_INT info;

    descinit_(descA, &n, &n, &nb, &nb, &irsrc, &icsrc, &icontxt, &myArows, &info);
    descinit_(descB, &n, &n, &nb, &nb, &irsrc, &icsrc, &icontxt, &myBrows, &info);
    descinit_(descC, &n, &n, &nb, &nb, &irsrc, &icsrc, &icontxt, &myCrows, &info);

    pdgemm_(&trans, &trans, &n, &n, &n, &alpha, myA.data(), &iOne, &iOne, descA,
            myB.data(), &iOne, &iOne, descB, &beta, myC.data(), &iOne, &iOne, descC);

    // Clean up
    Cblacs_gridexit(icontxt);
    MPI_Finalize();

    // Print a sample result
    if (rank == 0) {
        std::cout << "Result at C[0][0]: " << myC[0] << std::endl;
    }

    return 0;
}
