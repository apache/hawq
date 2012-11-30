#include "utils/gpla/gpla.h"

#define dgemv_ f2c_dgemv
#define ddot_ f2c_ddot

/* From BLAS */
long int dgemv_(char *trans, long int *m, long int *n, double *alpha,
				  double *a, long int *lda, double *x, long int *incx,
				  double *beta, double *y, long int *incy);

double ddot_(long int *n, double *dx, long int *incx,
						double *dy, long int *incy);

/* From LAPACK */
void dgesdd_(char *jobz, long int *m, long int *n, double *a, long int *lda,
			 double *s, double *u, long int *ldu, double *vt,
			 long int *ldvt, double *work, long int *lwork, long int *iwork,
			 long int *info);

int
gp_dgemv(char *trans, long int *m, long int *n, double *alpha,
		 double *a, long int *lda, double *x, long int *incx,
		 double *beta, double *y, long int *incy)
{
	return dgemv_(trans, m, n, alpha, a, lda, x, incx, beta, y, incy);
}

double
gp_ddot(long int *n, double *dx, long int *incx, double *dy,
		long int *incy)
{
	return ddot_(n, dx, incx, dy, incy);
}

void
gp_dgesdd(char *jobz, long int *m, long int *n, double *a, long int *lda,
		  double *s, double *u, long int *ldu, double *vt, long int
		  *ldvt, double *work, long int *lwork, long int *iwork,
		  long int *info)
{

	dgesdd_(jobz, m, n, a, lda, s, u, ldu, vt, ldvt, work, lwork, iwork, info);
}
