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

#ifndef PROCFS_PROCESS_H
#define PROCFS_PROCESS_H


/* Common functions for POSIX process implementations */



/** Check if a PID is valid
 *
 * This checks if a directory entry exists in /proc for the pid.
 *
 * Return 0 if the PID exists, -1 if not.
 */
int procfs_check_pid(pid_t pid);


/** Read a file into a newly created buffer
 *
 * Allocates a new buffer with psi_malloc, fills it with the file's contents
 * and returns it.  Don't forget to free the buffer with psi_free().  The
 * buffer will point to NULL in case of failure.
 *
 * The filename is constructed from the pid using the printf format template
 * "/proc/%d/%s" where %d is the `pid' parameter and %s the `fname' parameter.
 *
 * @param buf: Will point to the new buffer.
 * @param pid: The pid for which to read the file, `%d' from the template.
 *
 * @param fname: The filename to read from, `%s' from the template.  This is
 * really `const' but PyErr_SetFromErrnoWithFilename() is not declared
 * properly for this.
 *
 * @return The used size of the buffer on success, -1 on failure (exception
 * raised) and -2 in case of a premissions problem.
 */
int procfs_read_procfile(char **buf, const pid_t pid, char *fname);


/* XXX What does this have to do with procfs? */
/* XXX This isn not used by anything currently, remove it? */
/** Construct an argv array from a string
 *
 * We need to be careful to split up the arguments taking shell quoting into
 * account.
 *
 * @param argv: The argv array.
 * @param argstr: The string to parse argv from.
 *
 * The `argv' parameter will point to the newly allocated array and need to be
 * free with psi_free().  In case of failure it will point to NULL.
 *
 * The `argstr' parameter is really const but the compiler isn't clever enough
 * (and we don't want to allocate memory and copy it just to make the compiler
 * happy).
 *
 * @return The number of elements in `argv' or -1 in case of failure.  If argc
 *         is greater then INT_MAX, INT_MAX is returned.
 */
int procfs_argv_from_string(char ***argv,
                            char *argstr,
                            const unsigned int argc);


#endif  /* PROCFS_PROCESS_H */
