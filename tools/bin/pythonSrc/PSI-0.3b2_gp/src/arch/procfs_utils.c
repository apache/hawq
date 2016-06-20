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

/* Common functions for POSIX process implementations. */


#include <Python.h>

#include <limits.h>
#include <sys/stat.h>

#include "psi.h"
#include "process.h"
#include "procfs_utils.h"


int
procfs_check_pid(pid_t pid)
{
    struct stat stati;
    char *fname;
    int r;

    r = psi_asprintf(&fname, "/proc/%d", pid);
    errno = 0;
    r = stat(fname, &stati);
    if (r == -1) {
        if (errno == EACCES)
            PyErr_SetString(PyExc_OSError, "No read access for /proc");
        else if (errno == ENOENT)
            PyErr_Format(PsiExc_NoSuchProcessError,
                         "No such PID: %ld", (long)pid);
        else
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, fname);
    }
    psi_free(fname);
    return r;
}


int
procfs_read_procfile(char **buf, const pid_t pid, char *fname)
{
    char *path;
    int r;

    *buf = NULL;
    r = psi_asprintf(&path, "/proc/%d/%s", pid, fname);
    if (r == -1)
        return -1;
    r = psi_read_file(buf, path);
    psi_free(path);
    if (r == -1)
        procfs_check_pid(pid);  /* OSError -> NoSuchProcessError if required */
    return r;
}


int
procfs_argv_from_string(char ***argv,
                        char *argstr,
                        const unsigned int argc)
{
    char *ptr;
    char *arg;
    char quote;
    unsigned int i;

    *argv = (char**)psi_calloc(argc*sizeof(char*));
    if (*argv == NULL)
        return -1;
    ptr = argstr;
    for (i=0; i < argc; i++) {
        while (isspace((int)*ptr) && *ptr != '\0')
            ptr++;
        if (*ptr == '\0')
            break;
        arg = ptr;
        if (*ptr == '\'' || *ptr == '"') {
            quote = *ptr;
            arg++;              /* don't include the quote in the arg */
            do {
                ptr++;
                while (*ptr == '\\') /* skip to next unescaped char */
                    ptr += 2;
            } while (*ptr != quote);
            (*argv)[i] = (char*)psi_malloc(ptr - arg + 1);
            if ((*argv)[i] == NULL)
                return -1;
            strncpy((*argv)[i], arg, ptr-arg);
            *((*argv)[i] + (ptr-arg)) = '\0';
        } else {
            do
                ptr++;
            while (!isspace((int)*ptr) && *ptr != '\0');
            (*argv)[i] = (char*)psi_malloc(ptr - arg + 1);
            if ((*argv)[i] == NULL)

                return -1;
            strncpy((*argv)[i], arg, ptr-arg);
            *((*argv)[i] + (ptr-arg)) = '\0';
        }
    }
    if (i > INT_MAX)
        return INT_MAX;
    else
        return (int)i;
}
