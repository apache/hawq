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

#include <Python.h>

#include <stdio.h>
#include <sys/mnttab.h>
#include <sys/statvfs.h>
#include <sys/types.h>

#include "psi.h"
#include "mount.h"


/***** Local declarations *****/

static int set_mntent(psi_mountinfo_t *mounti, struct mnttab *mntent);
static int set_vfs(psi_mountinfo_t *mounti);


/***** Public functions *****/

psi_mountlist_t *
psi_arch_mountlist(const int remote)
{
    FILE *mnttab;
    struct mnttab mntent;
    psi_mountlist_t *ml = NULL;
    psi_mountinfo_t *mounti, **mounts;
    int rc;

    /* Open up the system's mount information file */
    if ((mnttab = fopen(MNTTAB, "r")) == NULL) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, MNTTAB);
        return NULL;
    }

    /* Create our (empty) mountlist */
    ml = (psi_mountlist_t *)psi_calloc(sizeof(psi_mountlist_t));
    if (ml == NULL) {
        fclose(mnttab);
        return NULL;
    }

    /* Step through each line in the mount file */
    while ((rc = getmntent(mnttab, &mntent)) == 0) {
        if (!remote && strchr(mntent.mnt_special, ':') != NULL)
            continue;           /* Skip remote filesystems */
        if ((mounti = psi_calloc(sizeof(psi_mountinfo_t))) == NULL) {
            fclose(mnttab);
            psi_free_mountlist(ml);
            return NULL;
        }
        mounts = (psi_mountinfo_t **)psi_realloc(
            ml->mounts,
            (ml->count + 1) * sizeof(psi_mountinfo_t *));
        if (mounts == NULL) {
            fclose(mnttab);
            psi_free_mountinfo(mounti);
            psi_free_mountlist(ml);
            return NULL;
        }
        ml->mounts = mounts;
        ml->mounts[ml->count] = mounti;
        ml->count += 1;

        /* Finally add the information to mounti */
        if (set_mntent(mounti, &mntent) < 0) {
            fclose(mnttab);
            psi_free_mountlist(ml);
            return NULL;
        }
        if (set_vfs(mounti) < 0) {
            fclose(mnttab);
            psi_free_mountlist(ml);
            return NULL;
        }
    }
    fclose(mnttab);
    if (rc != -1) {             /* Uh oh, we had a read error */
        psi_free_mountlist(ml);
        PyErr_Format(PyExc_OSError, "Read error in %s", MNTTAB);
        return NULL;
    }
    return ml;
}


/***** Local Functions *****/


#define STRDUP(dst, src)                    \
    if ((dst = psi_strdup(src)) == NULL)    \
        return -1;                          \
    dst ## _status = PSI_STATUS_OK


static int
set_mntent(psi_mountinfo_t *mounti, struct mnttab *mntent)
{
    char *p;

    p = strchr(mntent->mnt_special, ':');
    if (p == NULL) {            /* Local mount */
        mounti->filesystem_host_status = PSI_STATUS_OK;
        STRDUP(mounti->filesystem_path, mntent->mnt_special);
    } else {
        *p = '\0';
        STRDUP(mounti->filesystem_host, mntent->mnt_special);
        STRDUP(mounti->filesystem_path, p + 1);
    }
    STRDUP(mounti->mount_type, mntent->mnt_fstype);
    STRDUP(mounti->mount_path, mntent->mnt_mountp);
    STRDUP(mounti->mount_options, mntent->mnt_mntopts);
    return 0;
}


/** Set the filesystem size information
 *
 * This information comes from the POSIX statvfs() call.
 */
static int set_vfs(psi_mountinfo_t *mounti)
{
    struct statvfs stat;
    int r;

    Py_BEGIN_ALLOW_THREADS;
    r = statvfs(mounti->mount_path, &stat);
    Py_END_ALLOW_THREADS;
    if (r < 0) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, mounti->mount_path);
        return -1;
    }
    mounti->frsize = stat.f_frsize;
    mounti->total = stat.f_blocks;
    mounti->bfree = stat.f_bfree;
    mounti->bavail = stat.f_bavail;
    mounti->files = stat.f_files;
    mounti->ffree = stat.f_ffree;
    mounti->favail = stat.f_favail;
    mounti->frsize_status = PSI_STATUS_OK;
    mounti->total_status = PSI_STATUS_OK;
    mounti->bfree_status = PSI_STATUS_OK;
    mounti->bavail_status = PSI_STATUS_OK;
    mounti->files_status = PSI_STATUS_OK;
    mounti->ffree_status = PSI_STATUS_OK;
    mounti->favail_status = PSI_STATUS_OK;
    return 0;
}
