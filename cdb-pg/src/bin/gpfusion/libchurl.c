#include "libchurl.h"
#include "lib/stringinfo.h"

/* include libcurl without typecheck.
 * This allows wrapping curl_easy_setopt to be wrapped
 * for readability. O/w an error is generated when anything 
 * other than the expected type is given as parameter
 */
#define CURL_DISABLE_TYPECHECK
#include <curl/curl.h>
#undef CURL_DISABLE_TYPECHECK

///////////////////////////////////////////////////////////////
/* curl multi API handle
 * created a single time and used throughout the
 * life of the process
 */
static CURLM* single_multi_handle = NULL;
/* curl API puts internal errors in this buffer
 * used for error reporting
 */
static char curl_error_buffer[CURL_ERROR_SIZE];

///////////////////////////////////////////////////////////////
/*
 * internal context of libchurl
 */
typedef struct
{
	/* curl easy API handle */
	CURL* curl_handle;

	/* perform() (libcurl API) lets us know
	 * if the session is over using this int
	 */
	int curl_still_running;

	/* internal buffer for read */
	char* ptr;
	int max;
	int bot, top;

	/* holds http error code returned from
	 * remote server
	 */
	char* last_http_reponse;

	/* true on upload, false on download */
	bool upload;
} churl_context;

/*
 * holds http header properties
 */
typedef struct
{
	struct curl_slist* headers;
} churl_settings;

///////////////////////////////////////////////////////////////
churl_context* churl_new_context(void);
void create_curl_handle(churl_context* context);
void set_curl_option(churl_context* context, CURLoption option, const void* data);
size_t read_callback(void *ptr, size_t size, size_t nmemb, void *userdata);
void setup_multi_handle(churl_context* context);
void multi_perform(churl_context* context);
bool internal_buffer_large_enough(churl_context* context, size_t required);
void flush_internal_buffer(churl_context* context);
void enlarge_internal_buffer(churl_context* context, size_t required);
void finish_upload(churl_context* context);
void cleanup_curl_handle(churl_context* context);
void cleanup_curl_header(churl_context* context);
void cleanup_internal_buffer(churl_context* context);
void churl_cleanup_context(churl_context* context);
size_t write_callback(char *buffer, size_t size, size_t nitems, void *userp);
int fill_internal_buffer(churl_context* context, int want);
void churl_headers_set(churl_context* context, CHURL_HEADERS settings);
void check_response_code(churl_context* context);
void clear_error_buffer(void);
size_t header_callback(char *buffer, size_t size, size_t nitems, void *userp);
void free_http_response(churl_context* context);
void compact_internal_buffer(churl_context* context);
void realloc_internal_buffer(churl_context* context, size_t required);
bool handle_special_error(long response);
///////////////////////////////////////////////////////////////

CHURL_HEADERS churl_headers_init(void)
{
	churl_settings* settings = (churl_settings*)palloc0(sizeof(churl_settings));
	return (CHURL_HEADERS)settings;
}

void churl_headers_append(CHURL_HEADERS headers,
						  const char* name,
						  const char* value)
{
	churl_settings* settings = (churl_settings*)headers;

	StringInfoData formatter;
	initStringInfo(&formatter);
	appendStringInfo(&formatter, "%s: %s", name, value);

	settings->headers = curl_slist_append(settings->headers,
										  formatter.data);
	pfree(formatter.data);
}

void churl_headers_cleanup(CHURL_HEADERS headers)
{
	churl_settings* settings = (churl_settings*)headers;
	if (!settings)
		return;

	if (settings->headers)
		curl_slist_free_all(settings->headers);

	pfree(settings);
}

///////////////////////////////////////////////////////////////
CHURL_HANDLE churl_init_upload(const char* url, CHURL_HEADERS headers)
{
	churl_context* context = churl_new_context();
	create_curl_handle(context);
	context->upload = true;
	clear_error_buffer();

	set_curl_option(context, CURLOPT_URL, url);
	set_curl_option(context, CURLOPT_VERBOSE, (const void*)FALSE);
	set_curl_option(context, CURLOPT_IPRESOLVE, (const void*)CURL_IPRESOLVE_V4);
	set_curl_option(context, CURLOPT_UPLOAD, (const void*)TRUE);
	set_curl_option(context, CURLOPT_READFUNCTION, read_callback);
	set_curl_option(context, CURLOPT_READDATA, context);
	set_curl_option(context, CURLOPT_HEADERFUNCTION, header_callback);
	set_curl_option(context, CURLOPT_WRITEHEADER, context);
	churl_headers_append(headers, "Content-Type", "application/octet-stream");
	churl_headers_append(headers, "Transfer-Encoding", "chunked");
	churl_headers_set(context, headers);

	setup_multi_handle(context);

	return (CHURL_HANDLE)context;
}

CHURL_HANDLE churl_init_download(const char* url, CHURL_HEADERS headers)
{
	churl_context* context = churl_new_context();
	create_curl_handle(context);
	context->upload = false;
	clear_error_buffer();

	set_curl_option(context, CURLOPT_URL, url);
	set_curl_option(context, CURLOPT_VERBOSE, (const void*)FALSE);
	set_curl_option(context, CURLOPT_ERRORBUFFER, curl_error_buffer);
	set_curl_option(context, CURLOPT_IPRESOLVE, (const void*)CURL_IPRESOLVE_V4);
	set_curl_option(context, CURLOPT_WRITEFUNCTION, write_callback);
	set_curl_option(context, CURLOPT_WRITEDATA, context);
	set_curl_option(context, CURLOPT_HEADERFUNCTION, header_callback);
	set_curl_option(context, CURLOPT_WRITEHEADER, context);
	churl_headers_set(context, headers);

	setup_multi_handle(context);

	return (CHURL_HANDLE)context;
}

void churl_download_restart(CHURL_HANDLE handle, const char* url)
{
	churl_context* context = (churl_context*)handle;

	Assert(!context->upload);

	/* halt current transfer */
	curl_multi_remove_handle(single_multi_handle, context->curl_handle);
	/* set a new url */
	set_curl_option(context, CURLOPT_URL, url);
	/* restart */
	setup_multi_handle(context);
}

size_t churl_write(CHURL_HANDLE handle, const char* buf, size_t bufsize)
{
	churl_context* context = (churl_context*)handle;
	Assert(context->upload);

	if (!internal_buffer_large_enough(context, bufsize))
	{
		flush_internal_buffer(context);
		if (!internal_buffer_large_enough(context, bufsize))
			enlarge_internal_buffer(context, bufsize);
	}

	memcpy(context->ptr + context->top, buf, bufsize);
	context->top += bufsize;

	return bufsize;
}

size_t churl_read(CHURL_HANDLE handle, char* buf, size_t max_size)
{
	int	n = 0;
	churl_context* context = (churl_context*)handle;
	Assert(!context->upload);

	fill_internal_buffer(context, max_size);
	check_response_code(context);

	n = context->top - context->bot;
	/* TODO: this means we are done.
	 * Should we do something with it?
	 * if (n == 0 && !context->curl_still_running)
	 * context->eof = true;
	 */

	if (n > max_size)
		n = max_size;

	memcpy(buf, context->ptr + context->bot, n);
	context->bot += n;

	return n;
}

void churl_cleanup(CHURL_HANDLE handle)
{
	churl_context* context = (churl_context*)handle;
	if (context->upload)
		finish_upload(context);

	cleanup_curl_handle(context);
	cleanup_internal_buffer(context);
	churl_cleanup_context(context);
}

churl_context* churl_new_context()
{
	churl_context* context = palloc(sizeof(churl_context));
	memset(context, 0, sizeof(churl_context));
	return context;
}

void clear_error_buffer()
{
	curl_error_buffer[0] = 0;
}

void create_curl_handle(churl_context* context)
{
	context->curl_handle = curl_easy_init();
	if (!context->curl_handle)
		elog(ERROR, "internal error: curl_easy_init failed");
}

void set_curl_option(churl_context* context, CURLoption option, const void* data)
{
	int curl_error;
	if (CURLE_OK != (curl_error = curl_easy_setopt(context->curl_handle, option, data)))
		elog(ERROR, "internal error: curl_easy_setopt %d error (%d - %s)",
			 option, curl_error, curl_easy_strerror(curl_error));
}

/*
 * Called by libcurl perform during an upload.
 * Copies data from internal buffer to libcurl's buffer.
 * Once zero is returned, libcurl knows upload is over
 */
size_t read_callback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	churl_context* context = (churl_context*)userdata;

	int written = Min(size * nmemb, context->top - context->bot);
	memcpy(ptr, context->ptr, written);
	context->bot += written;

	return written;
}

/*
 * Setups the libcurl multi API
 */
void setup_multi_handle(churl_context* context)
{
	int curl_error;

	/* Create multi handle on first use */
	if (!single_multi_handle)
		if (!(single_multi_handle = curl_multi_init()))
			elog(ERROR, "internal error: curl_multi_init failed");

	/* add the easy handle to the multi handle */
	/* don't blame me, blame libcurl */
	if (CURLM_OK != (curl_error = curl_multi_add_handle(single_multi_handle, context->curl_handle)))
		if (CURLM_CALL_MULTI_PERFORM != curl_error)
			elog(ERROR, "internal error: curl_multi_add_handle failed (%d - %s)",
				 curl_error, curl_easy_strerror(curl_error));

	multi_perform(context);
}

/*
 * Does the real work. Causes libcurl to do
 * as little work as possible and return.
 * During this functions execution,
 * callbacks are called.
 */
void multi_perform(churl_context* context)
{
	int curl_error;
	while (CURLM_CALL_MULTI_PERFORM ==
		   (curl_error = curl_multi_perform(single_multi_handle, &context->curl_still_running)));

	if (curl_error != CURLM_OK)
		elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)",
			 curl_error, curl_easy_strerror(curl_error));
}

bool internal_buffer_large_enough(churl_context* context, size_t required)
{
	return ((context->top + required) <= context->max);
}

void flush_internal_buffer(churl_context* context)
{
	if (context->top == 0)
		return;

	while((context->curl_still_running != 0) && ((context->top - context->bot) > 0))
	{
		/* TODO: Do I really need this? copied from url.c
		 * Can I call this from custom protocol?
		 * CHECK_FOR_INTERRUPTS();
		 */
		multi_perform(context);
	}

	if ((context->curl_still_running == 0) &&
		((context->top - context->bot) > 0))
		elog(ERROR, "failed sending to remote component");
	check_response_code(context);

	context->top = 0;
	context->bot = 0;
}

void enlarge_internal_buffer(churl_context* context, size_t required)
{
	if (context->ptr != NULL)
		pfree(context->ptr);

	context->max = required + 1024;
	context->ptr = palloc(context->max);
}

/*
 * Let libcurl finish the upload by
 * calling perform repeatedly
 */
void finish_upload(churl_context* context)
{
	if (!single_multi_handle)
		return;

	flush_internal_buffer(context);

	/* allow read_callback to say 'all done'
	 * by returning a zero thus ending the connection
	 */
	while(context->curl_still_running != 0)
		multi_perform(context);
	check_response_code(context);
}

void cleanup_curl_handle(churl_context* context)
{
	if (!context->curl_handle)
		return;
	if (single_multi_handle)
		curl_multi_remove_handle(single_multi_handle, context->curl_handle);
	curl_easy_cleanup(context->curl_handle);
	context->curl_handle = NULL;
}

void cleanup_internal_buffer(churl_context* context)
{
	if (!context->ptr)
		return;

	pfree(context->ptr);
	context->ptr = NULL;
}

void churl_cleanup_context(churl_context* context)
{
	pfree(context);
}

/*
 * Called by libcurl perform during a download.
 * Stores data from libcurl's buffer into the internal buffer.
 * If internal buffer is not large enough, increases it.
 */
size_t write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
    churl_context* context = (churl_context*)userp;
	const int 	nbytes = size * nitems;

	if (!internal_buffer_large_enough(context, nbytes))
	{
		compact_internal_buffer(context);
		if (!internal_buffer_large_enough(context, nbytes))
			realloc_internal_buffer(context, nbytes);
	}

	/* enough space. copy buffer into curl->buf */
	memcpy(context->ptr + context->top, buffer, nbytes);
	context->top += nbytes;

	return nbytes;
}

/*
 * Fills internal buffer up to want bytes.
 * returns when size reached or transfer ended
 */
int fill_internal_buffer(churl_context* context, int want)
{
    fd_set 	fdread;
    fd_set 	fdwrite;
    fd_set 	fdexcep;
    int 	maxfd;
    struct 	timeval timeout;
    int 	nfds, curl_error;

    /* attempt to fill buffer */
	while (context->curl_still_running && ((context->top - context->bot) < want))
    {
        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);

		/* TODO: do we need this?
		 * CHECK_FOR_INTERRUPTS();
		 */

        /* set a suitable timeout to fail on */
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        /* get file descriptors from the transfers */
        if (0 != (curl_error = curl_multi_fdset(single_multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd)))
			elog(ERROR, "internal error: curl_multi_fdset failed (%d - %s)",
						curl_error, curl_easy_strerror(curl_error));

		if (maxfd <= 0)
		{
			context->curl_still_running = 0;
			break;
		}

        if (-1 == (nfds = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout)))
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			elog(ERROR, "internal error: select failed on curl_multi_fdset (maxfd %d) (%d - %s)",
				 maxfd, errno, strerror(errno));
		}

		if (nfds > 0)
			multi_perform(context);
    }

    return 0;
}

void churl_headers_set(churl_context* context,
					   CHURL_HEADERS headers)
{
	churl_settings* settings = (churl_settings*)headers;
	set_curl_option(context, CURLOPT_HTTPHEADER, settings->headers);
}

/*
 * Parses return code from libcurl operation and
 * reports if different than 200 and 100
 */
void check_response_code(churl_context* context)
{
	long response;
	int curl_error;

	if (CURLE_OK != (curl_error = curl_easy_getinfo(context->curl_handle, CURLINFO_RESPONSE_CODE, &response)))
		elog(ERROR, "internal error: curl_easy_getinfo failed(%d - %s)",
			 curl_error, curl_easy_strerror(curl_error));

	if (response != 200 && response != 100)
		if (!handle_special_error(response))
			elog(ERROR, "remote component error: %lu (%s/%s)",
				 response,
				 curl_error_buffer[0] ? curl_error_buffer : "",
				 context->last_http_reponse ? context->last_http_reponse : "");

	free_http_response(context);
}

void free_http_response(churl_context* context)
{
	if (!context->last_http_reponse)
		return;

	pfree(context->last_http_reponse);
	context->last_http_reponse = NULL;
}

/*
 * Called during a perform by libcurl on either download or an upload.
 * Stores the first line of the header for error reporting
 */
size_t header_callback(char *buffer, size_t size,
					   size_t nitems, void *userp)
{
	const int nbytes = size * nitems;
    churl_context* context = (churl_context*)userp;

	if (context->last_http_reponse)
		return nbytes;

	char* p = palloc(nbytes + 1);
	memcpy(p, buffer, nbytes);
	p[nbytes] = 0;
	context->last_http_reponse = p;

	return nbytes;
}

void compact_internal_buffer(churl_context* context)
{
	int n;
	/* no compaction required */
	if (context->bot == 0)
		return;

	n = context->top - context->bot;
	memmove(context->ptr, context->ptr + context->bot, n);
	context->bot = 0;
	context->top = n;
}

void realloc_internal_buffer(churl_context* context, size_t required)
{
	int n;

	n = context->top - context->bot + required + 1024;
	if (context->ptr == NULL)
		context->ptr = palloc(n);
	else
		/* repalloc does not support NULL ptr */
		context->ptr = repalloc(context->ptr, n);

	context->max = n;
}

bool handle_special_error(long response)
{
	switch (response)
	{
		case 404:
			elog(ERROR, "GPHD component not found");
			break;
		default:
			return false;
	}
	return true;
}
