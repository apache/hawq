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

/* SunOS helper to retrieve arguments and environment variables from a 64-bit
 * process when Python is compiled as 32-bit. */

/* Note that we avoid reporting errors to stderr since we're not sure where
 * they would appear.  Stdout gets ignored when we return non-zero so that is
 * a good place to report errors to. */

/* For now there is no dynamic allocation and if there was this program is
 * very-short lived anyway.  So any function with an error should just print
 * the error (for debugging purposes) and call `exit(1)'.  `exit(2)' is
 * reserved for when there is a permissions problem. */


#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <procfs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#ifdef _ILP32
#error This helper app should be compiled in 64-bit mode
#endif 


/* Read psinfo into a buffer and return it
 *
 * The psinfo variable will be filled in on success.
 */
static void
read_psinfo(psinfo_t *psinfo, const char *pidstr)
{
    char path[PATH_MAX];
    ssize_t r;
    int fd;

    sprintf(path, "/proc/%s/psinfo", pidstr);
    fd = open(path, O_RDONLY);
    if (fd == -1) {
        printf("Failed to open %s\n", path);
        exit(1);
    }
    r = read(fd, psinfo, sizeof(psinfo_t));
    if (r == -1) {              /* XXX */
        printf("errno %d: %s\n", errno, strerror(errno));
        exit(1);
    }
    if (r != sizeof(psinfo_t)) {
        printf("Read wrong number of bytes from psinfo (%d != %d)\n",
               (int)r, (int)sizeof(psinfo_t));
        exit(1);
    }
}


/* Open the address space (core) of the process
 *
 * The fd parameter will contain the open file descriptor of the core on
 * success.
 */
static void
open_as(int *fd, const char *pidstr)
{
    char path[PATH_MAX];

    sprintf(path, "/proc/%s/as", pidstr);
    *fd = open(path, O_RDONLY);
    if (*fd == -1) {
        if (errno == EACCES || errno == EPERM)
            exit(2);
        else {
            printf("Failed to open core: %d, %s", errno, strerror(errno));
            exit(1);
        }
    }
}


/** Read a string from a process core
 *
 * This keeps reading characters from the core image, open as file descriptor
 * `asfd', starting from `offset' until '\0' is read.  Each character,
 * including the trailing NULL character is written to stdout.
 */
static void
read_str_from_core(const off_t offset, const int asfd)
{
    off_t charptr;
    ssize_t r;
    char c;

    charptr = offset;
    do {
        r = pread(asfd, &c, sizeof(char), charptr++);
        if (r < 0) {
            puts("Failed reading char from core\n");
            exit(1);
        }
        putchar(c);
    } while (c != '\0');
}


/** Retrieve the argument list from process core
 *
 * This prints the argv vector on stdout and will terminate with an extra
 * '\0'.
 */
static void
get_args(const psinfo_t *psinfo, const int asfd)
{
    off_t argvoff = (off_t)psinfo->pr_argv;
    off_t argaddr;              /* not caddr_t because of casting issues */
    ssize_t r;
    int i;

    for (i = 0; i < psinfo->pr_argc; i++) {
        r = pread(asfd, &argaddr, sizeof(off_t), argvoff + i*sizeof(off_t));
        if (r < 0) {
            puts("Failed reading argaddr from core\n");
            exit(1);
        }
        read_str_from_core(argaddr, asfd);
    }
    putchar('\0');
}


static void
get_envp(const psinfo_t *psinfo, const int asfd) {
    off_t envpoff = (off_t)psinfo->pr_envp;
    off_t envaddr;              /* not caddr_t because of casting issues */
    ssize_t r;
    int i = 0;

    r = pread(asfd, &envaddr, sizeof(off_t), envpoff);
    if (r < 0) {
        puts("Failed to read envaddr from core\n");
        exit(1);
    }
    while (envaddr != NULL) {
        read_str_from_core(envaddr, asfd);
        i++;
        r = pread(asfd, &envaddr, sizeof(off_t), envpoff + i*sizeof(off_t));
        if (r < 0) {
            puts("Failed to read envaddr from core (bis)\n");
            exit(1);
        }
    }
    putchar('\0');
}


int
main(int argc, char **argv)
{
    psinfo_t psinfo;
    int fd;

    if (argc != 2) {
        printf("Wrong argument count: %d\n", argc);
        exit(1);
    }
    read_psinfo(&psinfo, argv[1]);
    if (psinfo.pr_dmodel != PR_MODEL_LP64) {
        puts("psinfo.pr_dmodel != PR_MODEL_LP64\n");
        exit(1);
    }
    open_as(&fd, argv[1]);
    get_args(&psinfo, fd);
    get_envp(&psinfo, fd);
    return 0;
}
