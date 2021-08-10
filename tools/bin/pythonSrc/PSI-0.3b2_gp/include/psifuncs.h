/* The MIT License
 *
 * Copyright (C) 2008-2009 Floris Bruynooghe
 *
 * Copyright (C) 2008-2009 Abilisoft Ltd.
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

#ifndef PSIFUNCS_H
#define PSIFUNCS_H

/** Headers for implementing _psi functions
 *
 * Putting this in psi.h would make it avaialable to all the other PSI modules
 * and that is not what we want.
 *
 * XXX This should maybe be called psimod.h
 */


#include <time.h>


/* There is just one status field currently */
struct loadavginfo {
    double one;
    double five;
    double fifteen;
    int loadavg_status;
};


struct loadavginfo *arch_loadavginfo(void);


/** System boot time
 *
 * Returns 0 if successful, -1 for failure (Python Exception raised).
 */
int arch_boottime(struct timespec *boottime);


/** System uptime
 *
 * Returns 0 if successful, -1 for failure (Python Exception raised).
 */
int arch_uptime(struct timespec *uptime);


/** Create a new PsiTimeSpec object
 *
 * This method creates a new PsiTimeSpec object directly from a C timespec
 * structure bypassing the init method.  This is exposed in the module as a
 * CObject under the _C_API name and is used by PsiTimeSpec_New from
 * psi.h/util.c.  This method is meant to be internal to the _psi module.
 */
PyObject *PsiTimeSpec_InternalNew(const struct timespec *tv);


/** TimeSpec
 *
 * The Python type psi uses to represent time.
 */
extern PyTypeObject PsiTimeSpec_Type;


#endif  /* PSIFUNCS_H */
