extern int gp_dgemv(char *trans, long int *m, long int *n, double *alpha,
					double *a, long int *lda, double *x, long int *incx,
					double *beta, double *y, long int *incy);

extern double gp_ddot(long int *n, double *dx, long int *incx,
						  double *dy, long int *incy);
extern void gp_dgesdd(char *jobz, long int *m, long int *n, double *a,
					  long int *lda, double *s, double *u, long int *ldu,
					  double *vt, long int *ldvt, double *work,
					  long int *lwork, long int *iwork, long int *info);
