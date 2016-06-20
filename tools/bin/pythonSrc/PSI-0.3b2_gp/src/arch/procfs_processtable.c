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

/* POSIX implementation of the ProcessTable class */


#include <Python.h>

#include <dirent.h>

#include "psi.h"
#include "process.h"


#if (defined(SUNOS5) && SUNOS5_MINOR < 10) || defined(AIX)
#define SCANDIR myscandir
#else
#define SCANDIR scandir
#endif


static void
free_dentlist(struct dirent **dentlist, const int ndirs)
{
    int i;

    for (i = 0; i < ndirs; i++)
        free(dentlist[i]);
    free(dentlist);
}


#if (defined(SUNOS5) && SUNOS5_MINOR < 10) || defined(AIX)

/** Simplified scandir() implementation
 *
 * The scandir() function is hidden somewhere in /usr/ucb/cc on Solaris 8 & 9
 * and not available by default.  On AIX scandir() does not return any entries
 * in /proc for some unknown reason, this version does.
 *
 * In this version the `select' and `cmp' arguments are ignored.
 */
static int
myscandir(const char *dir,
          struct dirent ***namelist,
          int (*select) (const struct dirent *),
          int (*cmp) (const void *, const void *))
{
    DIR *dirp;
    struct dirent **dentlist;
    struct dirent **dlp;
    struct dirent *dentp;
    struct dirent *dcpy;
    int dentlist_size = 100;
    int ndirs = 0;

    dentlist = (struct dirent **)malloc(dentlist_size*sizeof(struct dirent *));
    if (dentlist == NULL)
        return -1;
    dirp = opendir(dir);
    if (dirp == NULL) {
        free(dentlist);
        return -1;
    }
    while ((dentp = readdir(dirp)) != NULL) {
        dcpy = (struct dirent *)malloc(sizeof(struct dirent));
        if (dcpy == NULL) {
            free_dentlist(dentlist, ndirs);
            closedir(dirp);
            return -1;
        }
        memcpy(dcpy, dentp, sizeof(struct dirent));
        dentlist[ndirs] = dcpy;
        ndirs++;
        if (ndirs >= dentlist_size) {
            dentlist_size += 100;
            dlp = (struct dirent **)realloc(
                dentlist, dentlist_size*sizeof(struct dirent *));
            if (dlp == NULL) {
                free_dentlist(dentlist, ndirs);
                closedir(dirp);
                return -1;
            }
            dentlist = dlp;
        }
    }
    closedir(dirp);
    *namelist = dentlist;
    return ndirs;
}

#endif  /* (defined(SUNOS5) && SUNOS5_MINOR < 10) || defined(AIX) */


struct psi_proclist *
psi_arch_proclist(void)
{
    struct psi_proclist *prl;
    struct dirent **dentlist;
    struct dirent *dentp;
    pid_t pid;
    int ndirs;
    int i;

    errno = 0;
    ndirs = SCANDIR("/proc", &dentlist, NULL, NULL);
    if (ndirs == -1)
        return (struct psi_proclist *) PyErr_SetFromErrnoWithFilename(
            PyExc_OSError, "/proc");
    prl = (struct psi_proclist *) psi_malloc(sizeof(struct psi_proclist));
    if (prl == NULL) {
        free_dentlist(dentlist, ndirs);
        return NULL;
    }
    prl->pids = (pid_t *) psi_malloc(ndirs * sizeof(pid_t));
    if (prl->pids == NULL) {
        psi_free(prl);
        free_dentlist(dentlist, ndirs);
        return NULL;
    }
    prl->count = 0;
    for (i = 0; i < ndirs; i++) {
        dentp = dentlist[i];
        if (dentp->d_name[0] == '.')
            continue;           /* skip `.' and `..' */
        errno = 0;
        pid = (pid_t)strtol(dentp->d_name, (char**)NULL, 10);
        if (pid <= 0 || errno)
            continue;           /* not a /proc/<pid> */
        prl->pids[prl->count] = pid;
        prl->count++;
    }
    free_dentlist(dentlist, ndirs);
    return prl;
}
