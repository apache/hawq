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

#ifndef ARCH_H
#define ARCH_H


/** Architecrue info to be filled in by each architecture
 *
 * The *_status fields are for use with the PSI_STATUS_* constants in psi.h so
 * don't forget to allocate this with psi_calloc().
 */
struct psi_archinfo {
    char *sysname;              /* operating system name */
    char *release;              /* release level of the operating system */
    char *version;              /* version within release level */
    char *machine;              /* hardware we're running on */
    char *nodename;             /* host name */
    int sysname_status;
    int release_status;
    int version_status;
    int machine_status;
    int nodename_status;
};


/** Return a newly allocated psi_archinfo structure
 *
 * This will have to be freed with psi_free() also on all the pointer
 * members.
 */
struct psi_archinfo *psi_arch_archinfo(void);


/** Free an psi_archinfo structure
 *
 * This function will check the pointers inside the psi_archinfo structure and
 * call psi_free() on the non-NULL ones.  For this to work you should have
 * allocated the memory with psi_calloc().
 *
 * The return value is always NULL so you can do `return psi_free_archinfo()'
 * right away.
 */
void *psi_free_archinfo(struct psi_archinfo *archi);


/* The following are for the archmodule only, not for arch implementations. */


/* The Arch*_Type types */
extern PyTypeObject PsiArchBase_Type;
extern PyTypeObject PsiArchDarwin_Type;
extern PyTypeObject PsiArchSunOS_Type;
extern PyTypeObject PsiArchLinux_Type;
extern PyTypeObject PsiArchAIX_Type;


/* Return a new ArchBase instance */
PyObject *PsiArchBase_New(void);


/* Return a new Arch* instance representing the current architecture */
PyObject *PsiArch_New(void);


#endif /* ARCH_H */
