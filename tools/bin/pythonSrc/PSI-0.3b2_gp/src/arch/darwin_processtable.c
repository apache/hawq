/* The MIT License
 *
 * Copyright (C) 2009 Erick Tryzelaar
 *
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/* ProcessTable functions for arch: Mac OS X */

#include <Python.h>
#include <sys/sysctl.h>
#include <sys/proc.h>

#include "psi.h"
#include "process.h"


/***** Local declarations *****/
static int psi_sysctl(int *name, int nlen, void **oldval, size_t *oldlenp);


/***** ProcessTable methods *****/


struct psi_proclist *
psi_arch_proclist(void)
{
    static const int    name[] = { CTL_KERN, KERN_PROC, KERN_PROC_ALL, 0 };
    struct kinfo_proc   *result = NULL;
    size_t              length;
    int                 i;
    struct psi_proclist *prl = NULL;

    if (psi_sysctl((int*)name, (sizeof(name) / sizeof(*name)) - 1,
                   (void**)&result, &length) == -1)
        return NULL;
    prl = (struct psi_proclist *) psi_malloc(sizeof(struct psi_proclist));
    if (prl == NULL) {
        psi_free(result);
        return NULL;
    }
    prl->count = length / sizeof(struct kinfo_proc);
    prl->pids = (pid_t *) psi_malloc(prl->count * sizeof(pid_t));
    if (prl->pids == NULL) {
        psi_free(prl);
        psi_free(result);
        return NULL;
    }
    for(i = 0; i < prl->count; i++)
        prl->pids[i] = result[i].kp_proc.p_pid;
    psi_free(result);
    return prl;
}


/***** Local functions *****/


static int
psi_sysctl(int *name, int nlen, void **oldval, size_t *oldlenp)
{
    int  err;
    int  done = 0;

    /* We start by calling sysctl with oldval == NULL and oldlenp == 0.  That
     * will succeed, and set length to the appropriate length.  We then
     * allocate a buffer of that size and call sysctl again with that buffer.
     * If that succeeds, we're done.  If that fails with ENOMEM, we have to
     * throw away our buffer and loop.  Note that the loop causes us to call
     * sysctl with NULL again; this is necessary because the ENOMEM failure
     * case sets length to the amount of data returned, not the amount of data
     * that could have been returned. */
    do {
        /* Call sysctl with a NULL buffer. */
        *oldlenp = 0;
        err = sysctl(name, nlen, NULL, oldlenp, NULL, 0);
        if (err == -1)
            err = errno;

        /* Allocate an appropriately sized buffer based on the results from the
         * previous call. */
        if (err == 0) {
            *oldval = psi_malloc(*oldlenp);
            if (*oldval == NULL)
                err = ENOMEM;
        }

        /* Call sysctl again with the new buffer.  If we get an ENOMEM error,
         * toss away our buffer and start again. */
        if (err == 0) {
            err = sysctl(name, nlen, *oldval, oldlenp, NULL, 0);
            if (err == -1)
                err = errno;
            if (err == 0)
                done = 1;
            else if (err == ENOMEM) {
                assert(*oldval != NULL);
                psi_free(*oldval);
                *oldval = NULL;
                err = 0;
            }
        }
    } while (err == 0 && !done);

    /* Clean up and establish post conditions. */
    if (err != 0) {
        if (*oldval != NULL)
            psi_free(*oldval);
        *oldval = NULL;
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, "sysctl()");
        return -1;
    }
    return 0;
}
