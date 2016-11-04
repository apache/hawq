/* The MIT License
 *
 * Copyright (C) 2009 Erick Tryzelaar
 *
 * Copyright (C) 2009 Floris Bruynooghe
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

#include <mntent.h>
#include <stdlib.h>
#include <sys/statvfs.h>

#include "psi.h"
#include "mount.h"
#include "posix_mount.h"


/***** Local declarations *****/

static int set_mntent(psi_mountinfo_t *mounti, struct mntent *mnt);


/***** Public functions *****/

psi_mountlist_t *
psi_arch_mountlist(const int remote)
{
    FILE *mntent;
    struct mntent mnt;
    char buf[PATH_MAX * 3];     /* size used in autofs so I hope it's okay */
    psi_mountlist_t *ml = NULL;
    psi_mountinfo_t *mounti, **mounts;

    /* Open /etc/mtab */
    mntent = setmntent(_PATH_MOUNTED, "r");
    if (mntent == NULL)
        return (psi_mountlist_t *) PyErr_SetFromErrnoWithFilename(
            PyExc_OSError, _PATH_MOUNTED);

    /* Create our (empty) mountlist */
    ml = (psi_mountlist_t *)psi_calloc(sizeof(psi_mountlist_t));
    if (ml == NULL) {
        fclose(mntent);
        return NULL;
    }

    /* Step through each line in the mount file */
    while (getmntent_r(mntent, &mnt, buf, sizeof(buf)) != NULL) {

        /* Skip remote filesystems if not asked for them */
        if (!remote &&
            (strchr(mnt.mnt_fsname, ':') != NULL ||
             strncmp(mnt.mnt_fsname, "//", 2) == 0))
            continue;

        /* Allocate space for the mount information */
        if ((mounti = psi_calloc(sizeof(psi_mountinfo_t))) == NULL) {
            fclose(mntent);
            psi_free_mountlist(ml);
            return NULL;
        }
        
        /* And then allocate more space for the mount list */
        mounts = (psi_mountinfo_t **)psi_realloc(
            ml->mounts,
            (ml->count + 1) * sizeof(psi_mountinfo_t *));
        if (mounts == NULL) {
            fclose(mntent);
            psi_free_mountinfo(mounti);
            psi_free_mountlist(ml);
            return NULL;
        }
        ml->mounts = mounts;
        ml->mounts[ml->count] = mounti;
        ml->count += 1;

        /* Finally add the information to mounti */
        if (set_mntent(mounti, &mnt) < 0) {
            fclose(mntent);
            psi_free_mountlist(ml);
            return NULL;
        }
        if (posix_set_vfs(mounti) < 0) {
            fclose(mntent);
            psi_free_mountlist(ml);
            return NULL;
        }
    }
    if (!feof(mntent)) {        /* Uh oh, we had a read error */
        endmntent(mntent);
        psi_free_mountlist(ml);
        PyErr_Format(PyExc_OSError, "Read error in %s", _PATH_MOUNTED);
        return NULL;
    }
    endmntent(mntent);
    return ml;
}


/***** Local Functions *****/


#define STRDUP(dst, src)                    \
    if ((dst = psi_strdup(src)) == NULL)    \
        return -1;                          \
    dst ## _status = PSI_STATUS_OK


static int
set_mntent(psi_mountinfo_t *mounti, struct mntent *mnt)
{
    char *p;
    int r;

    p = strchr(mnt->mnt_fsname, ':');
    r = strncmp(mnt->mnt_fsname, "//", 2);
    if (p == NULL || r != 0) {  /* Local mount */
        mounti->filesystem_host_status = PSI_STATUS_OK;
        STRDUP(mounti->filesystem_path, mnt->mnt_fsname);
    } else
        if (p != NULL) {
            *p = '\0';
            STRDUP(mounti->filesystem_host, mnt->mnt_fsname);
            STRDUP(mounti->filesystem_path, p + 1);
        } else {
            p = strchr(mnt->mnt_fsname + 2, '/');
            *p = '\0';
            STRDUP(mounti->filesystem_host, mnt->mnt_fsname + 2);
            STRDUP(mounti->filesystem_path, p + 1);
        }
    STRDUP(mounti->mount_type, mnt->mnt_type);
    STRDUP(mounti->mount_path, mnt->mnt_dir);
    STRDUP(mounti->mount_options, mnt->mnt_opts);
    return 0;
}
