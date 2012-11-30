#ifndef _GPHDFS_LIBCHURL_H_
#define _GPHDFS_LIBCHURL_H_

#include "postgres.h"
/*
 * CHunked cURL API
 * NOTES: 
 * 1) Does not multi thread
 * 2) Does not talk IPv6
 */
typedef void* CHURL_HEADERS;
typedef void* CHURL_HANDLE;

/* 
 * PUT example
 * -----------
 *
 * CHURL_HEADERS http_headers = churl_headers_init();
 * churl_headers_append(http_headers, "a", "b");
 * churl_headers_append(http_headers, "c", "d");
 *
 * CHURL_HANDLE churl = churl_init_upload("http://127.0.0.1:12345", http_headers);
 * while(have_stuff_to_write())
 * {
 *     churl_write(churl);
 * }
 *
 * churl_cleanup(churl);
 * churl_headers_cleanup(http_headers);
 *
 * GET example
 * -----------
 *
 * CHURL_HEADERS http_headers = churl_headers_init();
 * churl_headers_append(http_headers, "a", "b");
 * churl_headers_append(http_headers, "c", "d");
 *
 * CHURL_HANDLE churl = churl_init_download("http://127.0.0.1:12345", http_headers);
 *
 * char buf[64 * 1024];
 * size_t n = 0;
 * while ((n = churl_read(churl, buf, sizeof(buf))) != 0)
 * {
 *     do_something(buf, n);
 * }
 *
 * churl_cleanup(churl);
 * churl_headers_cleanup(http_headers);
 */

/* 
 * Create a handle for adding headers
 */
CHURL_HEADERS churl_headers_init(void);
/*
 * Add a new header
 * Headers are added in the form 'key: value'
 */
void churl_headers_append(CHURL_HEADERS headers,
						  const char* key,
						  const char* value);
/*
 * Cleanup handle for headers
 */
void churl_headers_cleanup(CHURL_HEADERS headers);

/*
 * Start an upload to url
 * returns a handle to churl transfer
 */
CHURL_HANDLE churl_init_upload(const char* url, CHURL_HEADERS headers);
/*
 * Start a download to url
 * returns a handle to churl transfer
 */
CHURL_HANDLE churl_init_download(const char* url, CHURL_HEADERS headers);

/*
 * Restart a session to a new URL
 * This will use the same headers
 */
void churl_download_restart(CHURL_HANDLE, const char* url);

/*
 * Send buf of bufsize
 */
size_t churl_write(CHURL_HANDLE handle, const char* buf, size_t bufsize);
/*
 * Receive up to max_size into buf
 */
size_t churl_read(CHURL_HANDLE handle, char* buf, size_t max_size);

/*
 * Cleanup churl resources
 */
void churl_cleanup(CHURL_HANDLE handle);

#endif
