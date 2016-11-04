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

#ifndef PSI_MOUNT_H
#define PSI_MOUNT_H


/** Mount info to be filled in by each architecture
 *
 * The *status fields are for use with the PSI_STATUS_* constants in psi.h so
 * don't forget to allocate this with psi_calloc().
 */
typedef struct psi_mountinfo {
    char *mount_type;           /* mount type */
    char *mount_options;        /* mount options */
    char *mount_path;           /* the mountpoint */
    char *filesystem_host;      /* filesystem host, NULL means local host */
    char *filesystem_path;      /* path of filesystem device */
    unsigned long long frsize;  /* fragment size in bytes, unit of blocks */
    unsigned long long total;   /* no. blocks on fs */
    unsigned long long bfree;   /* no. free block */
    unsigned long long bavail;  /* no. available block to non-root users */
    unsigned long long files;   /* no. inodes */
    unsigned long long ffree;   /* no. free inodes */
    unsigned long long favail;  /* no. available indoes to non-root users */
    int mount_type_status;
    int mount_options_status;
    int mount_path_status;
    int filesystem_host_status;
    int filesystem_path_status;
    int frsize_status;
    int total_status;
    int bfree_status;
    int bavail_status;
    int files_status;
    int ffree_status;
    int favail_status;
} psi_mountinfo_t;


/** Free the psi_mountinfo structure
 *
 * This function will check all pointers in the passed psi_mountinfo structure
 * and if they are non-NULL will call psi_free() on them.  At the end it will
 * call psi_free() on the structure itself thus completely freeing the space
 * used by the structure.
 *
 * Note that for this to work you must have allocated the structure with
 * psi_calloc() so that all pointers are initialised to NULL.
 *
 * The return value is always NULL so you can just do `return
 * psi_free_mountinfo()'
 */
void *psi_free_mountinfo(psi_mountinfo_t *mounti);


/** An array with all the mounts
 *
 * The array with contain `count' mountpoints
 */
typedef struct psi_mountlist {
    long count;
    psi_mountinfo_t **mounts;
} psi_mountlist_t;


/** Create the mountlist structure
 *
 * Returns a struct psi_mountlist with all the mounts in or NULL in case of
 * failure.
 */
psi_mountlist_t *psi_arch_mountlist(const int remote);


/** Free the psi_mountlist structure
 *
 * This function will check all pointers in the passed psi_mountlist structure
 * and if they are non-NULL will call psi_free() on them.  At the end it will
 * call psi_free() on the structure itself thus completely freeing the space
 * used by the structure.
 *
 * Note that for this to work you must have allocated the structure with
 * psi_calloc() so that all pointers are initialised to NULL.
 *
 * The return value is always NULL so you can just do `return
 * psi_free_mountlist()'
 */
void *psi_free_mountlist(psi_mountlist_t *prl);


/* The following are for the mountmodule, not for mount
 * implementations. */


/* The Mount*_Type types */
extern PyTypeObject MountBase_Type;
extern PyTypeObject LocalMount_Type;
extern PyTypeObject RemoteMount_Type;


/* Return a new MountBase instance */
PyObject *PsiMount_New(psi_mountinfo_t *mounti);


#endif  /* PSI_MOUNT_H */
