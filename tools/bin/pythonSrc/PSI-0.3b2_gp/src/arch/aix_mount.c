/* The MIT License
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

#include <string.h>
#include <sys/types.h>
#include <sys/mntctl.h>
#include <sys/vmount.h>

#include "psi.h"
#include "mount.h"
#include "posix_mount.h"


/***** Local declarations *****/

static int set_vmount(psi_mountinfo_t *mounti, const struct vmount *mnt);
static char *map_fstype(const int gfstype);


/***** Public functions *****/

psi_mountlist_t *
psi_arch_mountlist(const int remote)
{
    psi_mountlist_t *ml;
    psi_mountinfo_t *mounti;
    struct vmount *mnt;
    char *vmounts;
    int vmounts_size = sizeof(struct vmount);
    int nmounts;
    int offset = 0;
    int i;

    vmounts = psi_malloc(vmounts_size);
    if (vmounts == NULL)
        return NULL;
    nmounts = mntctl(MCTL_QUERY, vmounts_size, vmounts);
    if (nmounts == 0) {
        mnt = (struct vmount *)vmounts;
        vmounts_size = mnt->vmt_revision;
        psi_free(vmounts);
        vmounts = psi_malloc(vmounts_size);
        if (vmounts == NULL)
            return NULL;
        nmounts = mntctl(MCTL_QUERY, vmounts_size, vmounts);
    }
    if (nmounts == -1) {
        psi_free(vmounts);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    } else if (nmounts == 0) {
        psi_free(vmounts);
        PyErr_SetString(PyExc_OSError, "Internal race condition");
        return NULL;
    }
    ml = (psi_mountlist_t *)psi_calloc(sizeof(psi_mountlist_t));
    if (ml == NULL) {
        psi_free(vmounts);
        return NULL;
    }
    ml->mounts = (psi_mountinfo_t **)psi_calloc(
        nmounts*sizeof(psi_mountinfo_t *));
    if (ml->mounts == NULL) {
        psi_free(vmounts);
        psi_free(ml);
        return NULL;
    }
    ml->count = 0;
    for (i = 0; i < nmounts; i++) {
        mnt = (struct vmount *)(vmounts + offset);
        if (!remote && strcmp(vmt2dataptr(mnt, VMT_HOST), "-") != 0)
            continue;
        mounti = psi_calloc(sizeof(psi_mountinfo_t));
        ml->mounts[ml->count] = mounti;
        ml->count += 1;
        if (set_vmount(mounti, mnt) < 0) {
            psi_free(vmounts);
            psi_free_mountlist(ml);
            return NULL;
        }
        if (posix_set_vfs(mounti) < 0) {
            psi_free(vmounts);
            psi_free_mountlist(ml);
            return NULL;
        }
    }
    return ml;
}


/***** Local Functions *****/


#define STRDUP(dst, src)                    \
    if ((dst = psi_strdup(src)) == NULL)    \
        return -1;                          \
    dst ## _status = PSI_STATUS_OK


static int
set_vmount(psi_mountinfo_t *mounti, const struct vmount *mnt)
{
    mounti->mount_type = map_fstype(mnt->vmt_gfstype);
    if (mounti->mount_type == NULL)
        return -1;
    mounti->mount_type_status = PSI_STATUS_OK;

    /* XXX Debatable, options should maybe be flags instead of args. */
    STRDUP(mounti->mount_options, vmt2dataptr(mnt, VMT_ARGS));
    STRDUP(mounti->mount_path, vmt2dataptr(mnt, VMT_STUB));
    if (strcmp(vmt2dataptr(mnt, VMT_HOST), "-") != 0)
        STRDUP(mounti->filesystem_host, vmt2dataptr(mnt, VMT_HOST));
    STRDUP(mounti->filesystem_path, vmt2dataptr(mnt, VMT_OBJECT));
    return 0;
}


/** Convert struct vmount member vmt_gfstype into string
 *
 * This is a hardcoded list currently, that seems to be the only way it's
 * defined/available on AIX.
 */
static char *
map_fstype(const int gfstype)
{
    char *type;

    switch (gfstype) {
    case MNT_J2:
        return psi_strdup("jfs2");
    case MNT_NAMEFS:
        return psi_strdup("namefs");
    case MNT_NFS:
        return psi_strdup("nfs");
    case MNT_JFS:
        return psi_strdup("jfs");
    case MNT_CDROM:
        return psi_strdup("cdrom");
    case MNT_PROCFS:
        return psi_strdup("proc");
    case MNT_SFS:
        return psi_strdup("STREAM");
    case MNT_CACHEFS:
        return psi_strdup("cachefs");
    case MNT_NFS3:
        return psi_strdup("NFSv3");
    case MNT_AUTOFS:
        return psi_strdup("automount");
    case MNT_VXFS:
        return psi_strdup("vxfs");
    case MNT_VXODM:
        return psi_strdup("veritas");
    case MNT_UDF:
        return psi_strdup("UDFS");
    case MNT_NFS4:
        return psi_strdup("NFSv4");
    case MNT_RFS4:
        return psi_strdup("NFSv4-pseudo");
    case MNT_CIFS:
        return psi_strdup("SMBFS");
    default:
        psi_asprintf(&type, "%d", gfstype);
        return type;
    }
}
