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

#include <sys/param.h>
#include <sys/ucred.h>
#include <sys/mount.h>

#include "psi.h"
#include "mount.h"
#include "posix_mount.h"


/* Pulled from Apple's mount.c */
static struct opt {
    int o_opt;
    const char *o_name;
} optnames[] = {
	{ MNT_ASYNC,		"asynchronous" },
	{ MNT_EXPORTED,		"NFS exported" },
	{ MNT_LOCAL,		"local" },
	{ MNT_NODEV,		"nodev" },
	{ MNT_NOEXEC,		"noexec" },
	{ MNT_NOSUID,		"nosuid" },
	{ MNT_QUOTA,		"with quotas" },
	{ MNT_RDONLY,		"read-only" },
	{ MNT_SYNCHRONOUS,	"synchronous" },
	{ MNT_UNION,		"union" },
	{ MNT_AUTOMOUNTED,	"automounted" },
	{ MNT_JOURNALED,	"journaled" },
	{ MNT_DEFWRITE, 	"defwrite" },
	{ MNT_IGNORE_OWNERSHIP,	"noowners" },
#ifdef MNT_NOATIME
	{ MNT_NOATIME,		"noatime" },
#endif
#ifdef MNT_QUARANTINE
	{ MNT_QUARANTINE,	"quarantine" },
#endif
	{ 0, 				NULL }
};


#define STRDUP(dst, src)                    \
{                                           \
    if ((dst = psi_strdup(src)) == NULL) {  \
        psi_free_mountlist(ml);             \
    }                                       \
    dst ## _status = PSI_STATUS_OK;         \
}


psi_mountlist_t *
psi_arch_mountlist(const int remote)
{
    struct statfs *mp;
    int count;
    psi_mountlist_t *ml = NULL;
    psi_mountinfo_t *mounti, **mounts;
    char *p;

    /* Get the mount info from the kernel */
    if ((count = getmntinfo(&mp, MNT_NOWAIT)) == 0) {
        return (psi_mountlist_t*) PyErr_SetFromErrnoWithFilename(
            PyExc_OSError, "getmntinfo failed");
    }

    /* Create our mountlist */
    ml = (psi_mountlist_t *)psi_calloc(sizeof(psi_mountlist_t));
    if (ml == NULL) {
        return NULL;
    }

    /* Step through each line in the mount data */
    while (--count >= 0) {
        struct opt *o;
        int flags;
        char options[1024];

        /* Ignore mount type autofs since that filesystem may not actually be
         * mounted */
        if (strcmp(mp->f_fstypename, "autofs") == 0) {
            ++mp;
            continue;
        }

	/* Only include a remote fs if asked for */
	if (!remote && strchr(mp->f_mntfromname, ':') != NULL) {
		++mp;
		continue;
	}

        /* Allocate space for the mount information */
        if ((mounti = psi_calloc(sizeof(psi_mountinfo_t))) == NULL) {
            psi_free_mountlist(ml);
            return NULL;
        }
        
        /* And then allocate more space for the mount list */
        mounts = (psi_mountinfo_t **)psi_realloc(
            ml->mounts,
            (ml->count + 1) * sizeof(psi_mountinfo_t *));
        if (mounts == NULL) {
            psi_free_mountinfo(mounti);
            psi_free_mountlist(ml);
            return NULL;
        }
        ml->mounts = mounts;
        ml->mounts[ml->count] = mounti;
        ml->count += 1;

        /* Finally, add all the mntent information to mounti */
        p = strchr(mp->f_mntfromname, ':');
        if (p == NULL) {
            mounti->filesystem_host_status = PSI_STATUS_OK;
            STRDUP(mounti->filesystem_path, mp->f_mntfromname);
        } else {
            *p = '\0';

            STRDUP(mounti->filesystem_host, mp->f_mntfromname);
            STRDUP(mounti->filesystem_path, p + 1);
        }

        STRDUP(mounti->mount_type, mp->f_fstypename);
        STRDUP(mounti->mount_path, mp->f_mntonname);

        /* Convert the flags into a string */
        flags = mp->f_flags & MNT_VISFLAGMASK;
        p = options;
        for (o = optnames; flags && o->o_opt; ++o) {
            if (flags & o->o_opt) {
                int i = snprintf(p, 1024 - (p-options),
                                 "%s%s", p == options ? "" : ",", o->o_name);
                flags &= ~o->o_opt;
                p += i;
            }
        }
        STRDUP(mounti->mount_options, options);

	/* Set the statvfs items */
	posix_set_vfs(mounti);

        /* Advance the mount pointer */
        ++mp;
    }

    return ml;
}
