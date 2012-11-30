/* -----------------------------------------------------------------------------
 *
 * pinv.h
 *
 * @brief Compute the Moore-Penrose pseudo-inverse of a matrix.
 *
 * -------------------------------------------------------------------------- */

#ifndef PINV_H
#define PINV_H

#include "postgres.h"

extern void *load_gpla(void);
extern float8 pinv(long int /* rows */, long int /* columns */,
    float8 * /* A */, float8 * /* Aplus */);

#endif
