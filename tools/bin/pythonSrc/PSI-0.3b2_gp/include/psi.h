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

#ifndef PSI_H
#define PSI_H


#include <Python.h>

#include <time.h>


/** Exceptions
 *
 * These are created by the module init function.
 */
extern PyObject *PsiExc_AttrNotAvailableError;
extern PyObject *PsiExc_AttrInsufficientPrivsError;
extern PyObject *PsiExc_AttrNotImplementedError;
extern PyObject *PsiExc_MissingResourceError;
extern PyObject *PsiExc_InsufficientPrivsError;


/** Create a new PsiTimeSpec object
 *
 * This does get the _C_API CObject from the psi._psi module behind the
 * scenes.
 */
PyObject *PsiTimeSpec_New(const struct timespec *tv);



/** Constants representing status information of attributes
 *
 * These constants are for use with the *_status attributes of the *info
 * structures.  The "not implemented" option is set to 0 so that if you
 * allocate the memory with psi_calloc(3) this will be the default.  This
 * means that things won't blow up if you forget a field or one gets added but
 * the arch implementation doesn't have it yet (but don't forget to use calloc
 * instead of malloc for this!).
 */
#define PSI_STATUS_NI 0         /* not implemented */
#define PSI_STATUS_OK 1         /* data collected fine */
#define PSI_STATUS_NA 2         /* not available for this process */
#define PSI_STATUS_PRIVS 3      /* insufficient priviliges */


/** Check the attribute of an *info structure
 *
 * This checks the status attribute passed in against the PSI_STATUS_*
 * constants and will raise the appropriate Python exception if it is not
 * PSI_STATUS_OK.  The name parameter is used in the Python exception.
 *
 * Returns -1 if a Python exception is raised and 0 if not.
 */
int psi_checkattr(const char* name, const int status);


/** Memory management functions
 *
 * These are important since memory needs to be allocated in architecture
 * specific implementations and freed in the global/python part of PSI.
 * Therefore everyone must use the same memory management functions.  By using
 * these functions it is easy to decide at build time if you want to use the
 * libc memory allocator or the Python one etc.
 *
 * Additionally these function will have set the appropriate Python exception
 * if they return NULL.
 */
void *psi_malloc(size_t size);
void *psi_realloc(void *ptr, size_t size);
void psi_free(void *ptr);
void *psi_calloc(size_t size);

#define psi_FREE(p) if (p != NULL) psi_free(p)


/** String functions, allocating memory on the fly
 *
 * These are equivalent to the stdlib ones and their use should be obvious.
 * Again, in case of error NULL is returned and the correct Python exception
 * is raised.
 */
char *psi_strdup(const char *str);
char *psi_strndup(const char *str, size_t n);


/** Store string in a newly created buffer
 *
 * This function is like asprintf() in GNU libc, but if an error occurs a
 * Python exception will be raised.  On success the allocated buffer will have
 * to be freed using psi_free().
 *
 * The return value is the number of characters allocated for the buffer, or
 * -1 in case of an error.  In case of an error ptr will also be a NULL
 * pointer.
 */
int psi_asprintf(char **ptr, const char *template, ...);


/** String conversion and formatting
 *
 * Do not forget to use the PyOS_* functions for string formatting whenever
 * possible.  They are more portable, see
 * http://docs.python.org/c-api/conversion.html for their documentation.
 */



/** Read a file into a newly created buffer
 *
 * Allocates a new buffer with psi_malloc, fills it with the file's contents
 * and returns it.  Don't forget to free the buffer with psi_free().  If it
 * fails buf will point to NULL.
 *
 * `path' is really `const' but PyErr_SetFromErrnoWithFilename is not declared
 * properly.
 *
 * Returns the used size of the buffer on success, a negative value with an
 * OSError raised on failure.  -1 for a generic failure and -2 in case of a
 * permissions problem (interprets the errno value for you).
 */
int psi_read_file(char **buf, char *path);


/** Read a link and allocate space for the string automtically
 *
 * This will allocate the space for the target string using psi_malloc(), call
 * psi_free() on it when you're done.  If it fails target will point to NULL.
 *
 * The result will be stored in `target' and will be `\0' terminated.
 *
 * The `link' parameter is really `const' but PyErr_SetFromErrnoWithFilename
 * is not declared as such.
 *
 * Returns a negative integer on error, with OSError exception raised.  -2 is
 * returned in case of insufficient privileges, -1 for any other errors.  The
 * errno detail will be in the exception.
*/
int psi_readlink(char **target, char *link);


/** Return the number of strings in a buffer
 *
 * This function will count the number of `\0' terminated strings in a buffer.
 *
 * @param buf: The buffer.
 * @param size: The size of the buffer.
 *
 * @returns the number of `\0's found
 */
int psi_strings_count(const char *cmdl, const int size);


/** Return an array of strings build from a list of strings
 *
 * This function will create a new array of strings based on a buffer filled
 * with consecutive strings.  Each element in the returned array must be freed
 * using psi_free() after which the array itself must be freed with
 * psi_free().
 *
 * XXX This sucks, surely the implementation can figure out how large the
 *     array needs to be and then allocate it all at once.  That would be a
 *     lot more natural.
 *
 * @param buf: The buffer, really `const' but compiler is not clever enough.
 * @param count: The number of strings in the buffer.
 */
char **psi_strings_to_array(char *buf, const int count);


/** Deal with Python 3.0
 *
 * The following macros make dealing with Python 3.0 aka Python 3000 aka py3k
 * easier.
 */


#if PY_MAJOR_VERSION >= 3
#define PY3K
#endif


/* We would like to just use unicode in Python 2.x too but the convenient
 * PyUnicode_From*() functions don't exist yet there! */
#ifdef PY3K
#define PyStr_FromString(...) PyUnicode_FromString(__VA_ARGS__)
#define PyStr_FromFormat(...) PyUnicode_FromFormat(__VA_ARGS__)
#else
#define PyStr_FromString(...) PyString_FromString(__VA_ARGS__)
#define PyStr_FromFormat(...) PyString_FromFormat(__VA_ARGS__)
#endif


/** Deal with old Pythons
 *
 * These macro's and definitions help with early Python support.
 */


#ifndef PyDoc_VAR
#define PyDoc_VAR(name) static char name[]
#endif
#ifndef PyDoc_STR
#define PyDoc_STR(str) str
#endif
#ifndef PyDoc_STRVAR
#define PyDoc_STRVAR(name,str) PyDoc_VAR(name) = PyDoc_STR(str)
#endif

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif


#if PY_VERSION_HEX < 0x02030000 /* < 2.3 */
#define PyExc_FutureWarning PyExc_Warning
#endif

#if PY_VERSION_HEX < 0x02040000 /* < 2.4 */
#define Py_CLEAR(op)				\
        do {                            	\
                if (op) {			\
                        PyObject *_py_tmp = (PyObject *)(op);	\
                        (op) = NULL;		\
                        Py_DECREF(_py_tmp);	\
                }				\
        } while (0)
#define Py_RETURN_NONE return Py_INCREF(Py_None), Py_None
#define Py_RETURN_TRUE return Py_INCREF(Py_True), Py_True
#define Py_RETURN_FALSE return Py_INCREF(Py_False), Py_False
double PyOS_ascii_strtod(const char *nptr, char **endptr);
double PyOS_ascii_atof(const char *nptr);
#endif


#if PY_VERSION_HEX < 0x02050000 /* < 2.5 */
typedef int Py_ssize_t;
typedef PyObject *(*ssizeargfunc)(PyObject *, Py_ssize_t);
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#define PyErr_WarnEx(EXC, MSG, LVL) PyErr_Warn(EXC, MSG)
#endif


#if PY_VERSION_HEX < 0x02060000 /* < 2.6 */
#define Py_REFCNT(ob) (((PyObject*)(ob))->ob_refcnt)
#define Py_TYPE(ob)   (((PyObject*)(ob))->ob_type)
#define Py_SIZE(ob)   (((PyVarObject*)(ob))->ob_size)
#define PyVarObject_HEAD_INIT(type, size) PyObject_HEAD_INIT(type) size,
#define PyImport_ImportModuleNoBlock(NAME) PyImport_ImportModule(NAME)
#endif


#if PY_VERSION_HEX < 0x03010000 /* < 3.1 */
double PyOS_string_to_double(const char *s, char **endptr,
                             PyObject *overflow_exception);
#endif


#endif /* PSI_H */
