#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <mkl_scalapack.h>
#include <time.h>

extern "C" {
    /* blacs declarations */
    void blacs_pinfo_(int*, int*);
    void blacs_get_(int*, int*, int*);
    void blacs_gridinit_(int*, const char*, int*, int*);
	void blacs_gridinfo_(int*, int*, int*, int*, int*);
    void blacs_pcoord_(int*, int*, int*, int*);
    void blacs_gridexit_(int*);
	void blacs_exit_(int*);
    void blacs_barrier_(int*, const char*);

    MKL_INT numroc_(const MKL_INT*, const MKL_INT*, const MKL_INT*, const MKL_INT*, const MKL_INT*);

    void pdgemm_(char const *transa, char const *transb, const MKL_INT* M, const MKL_INT* N, const MKL_INT* K,
            const double* ALPHA,  double * A, const MKL_INT* IA, const MKL_INT* JA, MKL_INT * DESCA,
            double * B, const MKL_INT* IB, const MKL_INT* JB, MKL_INT * DESCB,
            const double* BETA, double * C, const MKL_INT* IC, const MKL_INT* JC, MKL_INT * DESCC );
	void pdgeadd_(char const *trans, const MKL_INT* M, const MKL_INT* N, double* ALPHA, double* A,
			MKL_INT * IA, MKL_INT * JA, MKL_INT* DESCA, double * BETA, double * C,
			MKL_INT * IC, MKL_INT * JC, MKL_INT * DESCC );
    void descinit_(MKL_INT* desc, const MKL_INT* m, const MKL_INT* n, const MKL_INT* bm, const MKL_INT* bn, 
            const MKL_INT* rsrc, const MKL_INT* csrc, const MKL_INT* ctxt, const MKL_INT* lda, MKL_INT* info);
}

#define MAX(x, y) ((x)>(y) ? (x):(y))

typedef MKL_INT MDESC[ 9 ];
double zero = 0.0E+0, one = 1.0E+0, two = 2.0E+0, negone = -1.0E+0;
MKL_INT i_zero = 0, i_one = 1, i_four = 4, i_negone = -1;
const char trans = 'N';
const char layout = 'R';

double* gen_mat(int n, int m, double mul)
{
    double *a = (double*)calloc(n*m, sizeof(double));
    int i, j;
    for (i=0; i<n; ++i)
        for (j=0; j<m; ++j)
            a[i+j*n] = i*mul + j;
    return a;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    MKL_INT P, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &P);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	const MKL_INT m = 1;
	const MKL_INT k = 1;
	const MKL_INT n = 1;
	const MKL_INT nb = 1;
	MKL_INT nprow = 1, npcol = 1;

    MKL_INT iam, nprocs, ictxt, myrow, mycol;
    MDESC descA, descB, descC, descA_local, descB_local, descC_local;
	MKL_INT info;
	MKL_INT a_m_local, a_n_local, b_m_local, b_n_local, c_m_local, c_n_local;
	MKL_INT a_lld, b_lld, c_lld;

    blacs_pinfo_( &iam, &nprocs );
    blacs_get_( &i_negone, &i_zero, &ictxt );
    blacs_gridinit_( &ictxt, &layout, &nprow, &npcol );
    blacs_gridinfo_( &ictxt, &nprow, &npcol, &myrow, &mycol );

    double *a = 0;
    double *b = 0;
	double *c = 0;

    if (iam==0)
    {
		a = gen_mat(m, k, 10.0);
		b = gen_mat(k, n, 1.0);
		c = (double*)calloc(m*n, sizeof(double));
    }

    a_m_local = numroc_( &m, &nb, &myrow, &i_zero, &nprow );
    a_n_local = numroc_( &k, &nb, &mycol, &i_zero, &npcol );

	b_m_local = numroc_( &k, &nb, &myrow, &i_zero, &nprow );
	b_n_local = numroc_( &n, &nb, &mycol, &i_zero, &npcol );

    c_m_local = numroc_( &m, &nb, &myrow, &i_zero, &nprow );
	c_n_local = numroc_( &n, &nb, &mycol, &i_zero, &npcol );

    double *A = (double*) calloc( nb * nb, sizeof( double ) );
    double *B = (double*) calloc( nb * nb, sizeof( double ) );
    double *C = (double*) calloc( nb * nb, sizeof( double ) );

    a_lld = MAX( a_m_local, 1 );
	b_lld = MAX( b_m_local, 1 );
	c_lld = MAX( c_m_local, 1 );
	printf("a_m_local = %d\ta_n_local = %d\tb_m_local = %d\tb_n_local = %d\tc_m_local = %d\tc_n_local = %d\n", a_m_local, a_n_local, b_m_local, b_n_local,
		c_m_local, c_n_local);
	printf("a_lld = %d\tb_lld = %d\tc_lld = %d\n", a_lld, b_lld, c_lld);

    descinit_( descA_local, &m, &k, &m, &k, &i_zero, &i_zero, &ictxt, &m, &info );
    descinit_( descB_local, &k, &n, &k, &n, &i_zero, &i_zero, &ictxt, &k, &info );
    descinit_( descC_local, &m, &n, &m, &n, &i_zero, &i_zero, &ictxt, &m, &info );

    descinit_( descA, &m, &k, &nb, &nb, &i_zero, &i_zero, &ictxt, &a_lld, &info );
    descinit_( descB, &k, &n, &nb, &nb, &i_zero, &i_zero, &ictxt, &b_lld, &info );
    descinit_( descC, &m, &n, &nb, &nb, &i_zero, &i_zero, &ictxt, &c_lld, &info );

	printf("Rank %d: start distribute data\n", iam);
    pdgeadd_( &trans, &m, &k, &one, a, &i_one, &i_one, descA_local, &zero, A, &i_one, &i_one, descA );
    pdgeadd_( &trans, &k, &n, &one, b, &i_one, &i_one, descB_local, &zero, B, &i_one, &i_one, descB );

    pdgemm_( "N", "N", &m, &n, &k, &one, A, &i_one, &i_one, descA, B, &i_one, &i_one, descB,
             &zero, C, &i_one, &i_one, descC );
	printf("Rank %d: finished dgemm\n", iam);

	pdgeadd_( &trans, &m, &n, &one, C, &i_one, &i_one, descC, &zero, c, &i_one, &i_one, descC_local);

	free(A);
	free(B);
	free(C);
	if (iam==0)
	{
		free(a);
		free(b);
		free(c);
	}

    blacs_gridexit_( &ictxt );
    blacs_exit_( &i_zero );
	MPI_Finalize();
}