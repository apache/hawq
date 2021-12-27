/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * url_curl.c
 *    Core support for opening external relations via a URL with curl
 *
 */

#include "postgres.h"

#include "access/url.h"

#include <arpa/inet.h>

#include <curl/curl.h>

#include "cdb/cdbsreh.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/uri.h"

#if BYTE_ORDER == BIG_ENDIAN
#define local_htonll(n) (n)
#define local_ntohll(n) (n)
#else
#define local_htonll(n) ((((uint64)htonl(n)) << 32LL) | htonl((n) >> 32LL))
#define local_ntohll(n) \
  ((((uint64)ntohl(n)) << 32LL) | (uint32)ntohl(((uint64)n) >> 32LL))
#endif

#define HOST_NAME_SIZE 100
#define FDIST_TIMEOUT 408
#define MAX_TRY_WAIT_TIME 64

/* we use a global one for convenience */
static CURLM *multi_handle = 0;

/*
 * A helper macro, to call curl_easy_setopt(), and ereport() if it fails.
 */
#define CURL_EASY_SETOPT(h, opt, val)                                        \
  do {                                                                       \
    int e;                                                                   \
    if ((e = curl_easy_setopt(h, opt, val)) != CURLE_OK)                     \
      elog(ERROR, "internal error: curl_easy_setopt \"%s\" error (%d - %s)", \
           CppAsString(opt), e, curl_easy_strerror(e));                      \
  } while (0)

/*
 * header_callback
 *
 * when a header arrives from the server curl calls this routine. In here we
 * extract the information we are interested in from the header, and store it
 * in the passed in callback argument (URL_FILE *) which lives in our
 * application.
 */
static size_t header_callback(void *ptr_, size_t size, size_t nmemb,
                              void *userp) {
  URL_FILE *url = (URL_FILE *)userp;
  char *ptr = ptr_;
  int len = size * nmemb;
  int i;
  char buf[20];

  Assert(size == 1);

  /*
   * parse the http response line (code and message) from
   * the http header that we get. Basically it's the whole
   * first line (e.g: "HTTP/1.0 400 time out"). We do this
   * in order to capture any error message that comes from
   * gpfdist, and later use it to report the error string in
   * check_response() to the database user.
   */
  if (url->u.curl.http_response == 0) {
    int n = nmemb;
    char *p;

    if (n > 0 && 0 != (p = palloc(n + 1))) {
      memcpy(p, ptr, n);
      p[n] = 0;

      if (n > 0 && (p[n - 1] == '\r' || p[n - 1] == '\n')) p[--n] = 0;

      if (n > 0 && (p[n - 1] == '\r' || p[n - 1] == '\n')) p[--n] = 0;

      url->u.curl.http_response = p;
    }
  }

  /*
   * extract the GP-PROTO value from the HTTP header.
   */
  if (len > 10 && *ptr == 'X' && 0 == strncmp("X-GP-PROTO", ptr, 10)) {
    ptr += 10;
    len -= 10;

    while (len > 0 && (*ptr == ' ' || *ptr == '\t')) {
      ptr++;
      len--;
    }

    if (len > 0 && *ptr == ':') {
      ptr++;
      len--;

      while (len > 0 && (*ptr == ' ' || *ptr == '\t')) {
        ptr++;
        len--;
      }

      for (i = 0; i < sizeof(buf) - 1 && i < len; i++) buf[i] = ptr[i];

      buf[i] = 0;
      url->u.curl.gp_proto = strtol(buf, 0, 0);
    }
  }

  return size * nmemb;
}

/*
 * write_callback
 *
 * when data arrives from gpfdist server and curl is ready to write it
 * to our application, it calls this routine. In here we will store the
 * data in the application variable (URL_FILE *)file which is the passed
 * in the forth argument as a part of the callback settings.
 *
 * we return the number of bytes written to the application buffer
 */
static size_t write_callback(char *buffer, size_t size, size_t nitems,
                             void *userp) {
  URL_FILE *file = (URL_FILE *)userp;
  curlctl_t *curl = &file->u.curl;
  const int nbytes = size * nitems;
  int n;

  /*
   * if insufficient space in buffer make more space
   */
  if (curl->in.top + nbytes >= curl->in.max) {
    /* compact ? */
    if (curl->in.bot) {
      n = curl->in.top - curl->in.bot;
      memmove(curl->in.ptr, curl->in.ptr + curl->in.bot, n);
      curl->in.bot = 0;
      curl->in.top = n;
    }

    /* if still insufficient space in buffer, enlarge it */
    if (curl->in.top + nbytes >= curl->in.max) {
      char *newbuf;

      n = curl->in.top - curl->in.bot + nbytes + 1024;

      MemoryContext oldctx;
      oldctx = MemoryContextSwitchTo(TopMemoryContext);
      newbuf = repalloc(curl->in.ptr, n);
      MemoryContextSwitchTo(oldctx);

      curl->in.ptr = newbuf;
      curl->in.max = n;

      Assert(curl->in.top + nbytes < curl->in.max);
    }
  }

  /* enough space. copy buffer into curl->buf */
  memcpy(curl->in.ptr + curl->in.top, buffer, nbytes);
  curl->in.top += nbytes;

  return nbytes;
}

static char *local_strstr(const char *str1, const char *str2) {
  char *cp = (char *)str1;
  char *s1, *s2;

  if (!*str2) return ((char *)str1);

  while (*cp) {
    s1 = cp;
    s2 = (char *)str2;

    while (*s1 && (*s1 == *s2)) s1++, s2++;

    if (!*s2) return (cp);

    cp++;
  }

  return (NULL);
}

/*
 * This function purpose is to make sure that the URL string contains a
 * numerical IP address.  The input URL is in the parameter url. The output
 * result URL is in the output parameter - buf.  When parameter - url already
 * contains a numerical ip, then output parameter - buf will be a copy of url.
 * For this case calling getDnsAddress method inside make_url, will serve the
 * purpose of IP validation.  But when parameter - url will contain a domain
 * name, then the domain name substring will be changed to a numerical ip
 * address in the buf output parameter.
 *
 * Returns the length of the converted URL string, excluding null-terminator.
 */
static int make_url(const char *url, char *buf, bool is_ipv6) {
  char *authority_start = local_strstr(url, "//");
  char *authority_end;
  char *hostname_start;
  char *hostname_end;
  char hostname[HOST_NAME_SIZE];
  char *hostip = NULL;
  char portstr[9];
  int len;
  char *p;
  int port = 80; /* default for http */
  bool domain_resolved_to_ipv6 = false;

  if (!authority_start) elog(ERROR, "illegal url '%s'", url);

  authority_start += 2;
  authority_end = strchr(authority_start, '/');
  if (!authority_end) authority_end = authority_start + strlen(authority_start);

  hostname_start = strchr(authority_start, '@');
  if (!(hostname_start && hostname_start < authority_end))
    hostname_start = authority_start;

  if (is_ipv6) /* IPV6 */
  {
    int len;

    hostname_end = strchr(hostname_start, ']');
    if (hostname_end == NULL)
      ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                      errmsg("unexpected IPv6 format %s", url)));
    hostname_end += 1;

    if (hostname_end[0] == ':') {
      /* port number exists in this url. get it */
      len = authority_end - hostname_end;
      if (len > 8)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("<port> substring size must not exceed 8 characters")));

      memcpy(portstr, hostname_end + 1, len);
      portstr[len] = '\0';
      port = atoi(portstr);
    }

    /* skippping the brackets */
    hostname_end -= 1;
    hostname_start += 1;
  } else {
    hostname_end = strchr(hostname_start, ':');
    if (!(hostname_end && hostname_end < authority_end)) {
      hostname_end = authority_end;
    } else {
      /* port number exists in this url. get it */
      int len = authority_end - hostname_end;
      if (len > 8)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("<port> substring size must not exceed 8 characters")));

      memcpy(portstr, hostname_end + 1, len);
      portstr[len] = '\0';
      port = atoi(portstr);
    }
  }

  if (!port)
    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("<port> substring must contain only digits")));

  if (hostname_end - hostname_start >= sizeof(hostname))
    elog(ERROR, "hostname too long for url '%s'", url);

  memcpy(hostname, hostname_start, hostname_end - hostname_start);
  hostname[hostname_end - hostname_start] = 0;

  hostip = getDnsAddress(hostname, port, ERROR);

  /*
   * test for the case where the URL originaly contained a domain name
   * (so is_ipv6 was set to false) but the DNS resolution in getDnsAddress
   * returned an IPv6 address so know we also have to put the square
   * brackets [..] in the URL string.
   */
  if (strchr(hostip, ':') != NULL && !is_ipv6) domain_resolved_to_ipv6 = true;

  if (!buf) {
    int len = strlen(url) - strlen(hostname) + strlen(hostip);
    if (domain_resolved_to_ipv6) len += 2; /* for the square brackets */
    return len;
  }

  p = buf;
  len = hostname_start - url;
  strncpy(p, url, len);
  p += len;
  url += len;

  len = strlen(hostname);
  url += len;

  len = strlen(hostip);
  if (domain_resolved_to_ipv6) {
    *p = '[';
    p++;
  }
  strncpy(p, hostip, len);
  p += len;
  if (domain_resolved_to_ipv6) {
    *p = ']';
    p++;
  }

  strcpy(p, url);
  p += strlen(url);

  return p - buf;
}

static void extract_http_domain(char *i_path, char *o_domain, int dlen) {
  int domsz, cpsz;
  char *p_st = (char *)local_strstr(i_path, "//");
  p_st = p_st + 2;
  char *p_en = strchr(p_st, '/');

  domsz = p_en - p_st;
  cpsz = (domsz < dlen) ? domsz : dlen;
  memcpy(o_domain, p_st, cpsz);
}

static bool url_has_ipv6_format(char *url) {
  bool is6 = false;
  char *ipv6 = local_strstr(url, "://[");

  if (ipv6) ipv6 = strchr(ipv6, ']');
  if (ipv6) is6 = true;

  return is6;
}

static void set_httpheader(URL_FILE *fcurl, const char *name,
                           const char *value) {
  struct curl_slist *new_httpheader;
  char tmp[1024];

  if (strlen(name) + strlen(value) + 5 > sizeof(tmp))
    elog(ERROR, "set_httpheader name/value is too long. name = %s, value=%s",
         name, value);

  snprintf(tmp, sizeof(tmp), "%s: %s", name, value);

  new_httpheader = curl_slist_append(fcurl->u.curl.x_httpheader, tmp);
  if (new_httpheader == NULL)
    elog(ERROR, "could not set curl HTTP header \"%s\" to \"%s\"", name, value);

  fcurl->u.curl.x_httpheader = new_httpheader;
}

static void replace_httpheader(URL_FILE *file, const char *name,
                               const char *value) {
  struct curl_slist *new_httpheader;
  char tmp[1024];

  if (strlen(name) + strlen(value) + 5 > sizeof(tmp))
    elog(ERROR,
         "replace_httpheader name/value is too long. name = %s, value=%s", name,
         value);

  sprintf(tmp, "%s: %s", name, value);

  /* Find existing header, if any */
  struct curl_slist *p = file->u.curl.x_httpheader;
  while (p != NULL) {
    if (!strncmp(name, p->data, strlen(name))) {
      /*
       * NOTE: p->data is not palloc'd! It is originally allocated
       * by curl_slist_append, so use plain malloc/free here as well.
       */
      char *dupdata = strdup(tmp);

      if (dupdata == NULL) elog(ERROR, "out of memory");

      free(p->data);
      p->data = dupdata;
      return;
    }
    p = p->next;
  }

  /* No existing header, add a new one */

  new_httpheader = curl_slist_append(file->u.curl.x_httpheader, tmp);
  if (new_httpheader == NULL)
    elog(ERROR, "could not append HTTP header \"%s\"", name);
  file->u.curl.x_httpheader = new_httpheader;
}

// callback for request /gpfdist/status for debugging purpose.
static size_t log_http_body(char *buffer, size_t size, size_t nitems,
                            void *userp) {
  char body[256] = {0};
  int nbytes = size * nitems;
  int len = sizeof(body) - 1 > nbytes ? nbytes : sizeof(body) - 1;

  memcpy(body, buffer, len);

  elog(LOG, "gpfdist/status: %s", body);

  return nbytes;
}

// GET /gpfdist/status to get gpfdist status.
static void get_gpfdist_status(URL_FILE *file) {
  CURL *status_handle = NULL;
  char status_url[256];
  char domain[HOST_NAME_SIZE] = {0};
  CURLcode e;

  extract_http_domain(file->url, domain, HOST_NAME_SIZE);
  snprintf(status_url, sizeof(status_url), "http://%s/gpfdist/status", domain);

  do {
    if (!(status_handle = curl_easy_init())) {
      elog(LOG, "internal error: get_gpfdist_status.curl_easy_init failed");
      break;
    }
    if (CURLE_OK !=
        (e = curl_easy_setopt(status_handle, CURLOPT_TIMEOUT, 60L))) {
      elog(LOG,
           "internal error: get_gpfdist_status.curl_easy_setopt "
           "CURLOPT_TIMEOUT error (%d - %s)",
           e, curl_easy_strerror(e));
      break;
    }
    if (CURLE_OK !=
        (e = curl_easy_setopt(status_handle, CURLOPT_URL, status_url))) {
      elog(LOG,
           "internal error: get_gpfdist_status.curl_easy_setopt CURLOPT_URL "
           "error (%d - %s)",
           e, curl_easy_strerror(e));
      break;
    }
    if (CURLE_OK != (e = curl_easy_setopt(status_handle, CURLOPT_WRITEFUNCTION,
                                          log_http_body))) {
      elog(LOG,
           "internal error: get_gpfdist_status.curl_easy_setopt "
           "CURLOPT_WRITEFUNCTION error (%d - %s)",
           e, curl_easy_strerror(e));
      break;
    }
    if (CURLE_OK != (e = curl_easy_perform(status_handle))) {
      elog(LOG, "send status request failed: %s", curl_easy_strerror(e));
    }
  } while (0);

  curl_easy_cleanup(status_handle);
}

/*
 * fill_buffer
 *
 * Attempt to fill the read buffer up to requested number of bytes.
 * We first check if we already have the number of bytes that we
 * want already in the buffer (from write_callback), and we do
 * a select on the socket only if we don't have enough.
 *
 * return 0 if successful; raises ERROR otherwise.
 */
static int fill_buffer(URL_FILE *file, int want) {
  fd_set fdread;
  fd_set fdwrite;
  fd_set fdexcep;
  int maxfd = 0;
  int nfds = 0, e = 0;
  int timeout_count = 0;
  curlctl_t *curl = &file->u.curl;

  /* attempt to fill buffer */
  while (curl->still_running && curl->in.top - curl->in.bot < want) {
    FD_ZERO(&fdread);
    FD_ZERO(&fdwrite);
    FD_ZERO(&fdexcep);

    CHECK_FOR_INTERRUPTS();

    /* set a suitable timeout to fail on */
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;

    /* get file descriptors from the transfers */
    if (0 != (e = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep,
                                   &maxfd))) {
      elog(ERROR, "internal error: curl_multi_fdset failed (%d - %s)", e,
           curl_easy_strerror(e));
    }

    /* When libcurl returns -1 in max_fd, it is because libcurl currently does
     * something that isn't possible for your application to monitor with a
     * socket and unfortunately you can then not know exactly when the current
     * action is completed using select(). You then need to wait a while before
     * you proceed and call curl_multi_perform anyway. How long to wait? Unless
     * curl_multi_timeout gives you a lower number, we suggest 100 milliseconds
     * or so, but you may want to test it out in your own particular conditions
     * to find a suitable value.*/
    if (maxfd == -1) {
      pg_usleep(100);
      nfds = 1;
    } else {
      nfds = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
    }
    if (nfds == -1) {
      if (errno == EINTR || errno == EAGAIN) {
        elog(DEBUG2, "select failed on curl_multi_fdset (maxfd %d) (%d - %s)",
             maxfd, errno, strerror(errno));
        continue;
      }
      elog(ERROR,
           "internal error: select failed on curl_multi_fdset (maxfd %d) (%d - "
           "%s)",
           maxfd, errno, strerror(errno));
    } else if (nfds == 0) {
      // timeout
      timeout_count++;

      if (timeout_count % 12 == 0) {
        elog(LOG,
             "segment has not received data from gpfdist for about 1 minute, "
             "waiting for %d bytes.",
             (want - (curl->in.top - curl->in.bot)));
      }

      if (readable_external_table_timeout != 0 &&
          timeout_count * 5 > readable_external_table_timeout) {
        elog(LOG,
             "bot = %d, top = %d, want = %d, maxfd = %d, nfds = %d, e = %d, "
             "still_running = %d, for_write = %d, error = %d, eof = %d, "
             "datalen = %d",
             curl->in.bot, curl->in.top, want, maxfd, nfds, e,
             curl->still_running, curl->for_write, curl->error, curl->eof,
             curl->block.datalen);
        get_gpfdist_status(file);
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                        errmsg("segment has not received data from gpfdist for "
                               "long time, cancelling the query.")));
        break;
      }
    } else if (nfds > 0) {
      /* timeout or readable/writable sockets */
      /* note we *could* be more efficient and not wait for
       * CURLM_CALL_MULTI_PERFORM to clear here and check it on re-entry
       * but that gets messy */
      while (CURLM_CALL_MULTI_PERFORM ==
             (e = curl_multi_perform(multi_handle, &curl->still_running)))
        ;

      if (e != 0) {
        elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)", e,
             curl_easy_strerror(e));
      }
    } else {
      elog(ERROR, "select return unexpected result");
    }
  }

  if (curl->still_running == 0) {
    elog(LOG,
         "quit fill_buffer due to still_running = 0, bot = %d, top = %d, want "
         "= %d, "
         "for_write = %d, error = %d, eof = %d, datalen = %d, maxfd = %d, nfds "
         "= %d, e = %d",
         curl->in.bot, curl->in.top, want, curl->for_write, curl->error,
         curl->eof, curl->block.datalen, maxfd, nfds, e);
  }

  return 0;
}

/*
 * check_response
 *
 * If got an HTTP response with an error code from the server (gpfdist), report
 * the error code and message it to the database user and abort operation.
 */
static int check_response(URL_FILE *file, int *rc, char **response_string) {
  long response_code;
  char *effective_url = NULL;
  CURL *curl = file->u.curl.handle;
  char buffer[30];

  /* get the response code from curl */
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code) !=
      CURLE_OK) {
    *rc = 500;
    *response_string = pstrdup("curl_easy_getinfo failed");
    return -1;
  }
  *rc = response_code;
  snprintf(buffer, sizeof buffer, "Response Code=%d", (int)response_code);
  *response_string = pstrdup(buffer);

  if (curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url) !=
      CURLE_OK)
    return -1;
  if (effective_url == NULL) effective_url = "";

  if (!(200 <= response_code && response_code < 300)) {
    if (response_code == 0) {
      long oserrno = 0;
      static char connmsg[64];

      /* get the os level errno, and string representation of it */
      if (curl_easy_getinfo(curl, CURLINFO_OS_ERRNO, &oserrno) == CURLE_OK) {
        if (oserrno != 0)
          snprintf(connmsg, sizeof connmsg, "error code = %d (%s)",
                   (int)oserrno, strerror((int)oserrno));
      }
      // When still_running == 0 and os level err = "time out", we will not
      // report error.
      if (!(file->u.curl.still_running == 0 && oserrno == 110)) {
        ereport(
            ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE),
             errmsg("connection with gpfdist failed for \"%s\", effective url: "
                    "\"%s\". %s",
                    file->url, effective_url, (oserrno != 0 ? connmsg : ""))));
      }
    } else if (response_code ==
               FDIST_TIMEOUT)  // gpfdist server return timeout code
    {
      // When still_running == 0 and gpfdist return err = "time out", we will
      // not report error.
      if (file->u.curl.still_running == 0) {
        return 0;
      } else {
        return FDIST_TIMEOUT;
      }
    } else {
      /* we need to sleep 1 sec to avoid this condition:
         1- seg X gets an error message from gpfdist
         2- seg Y gets a 500 error
         3- seg Y report error before seg X, and error message
         in seg X is thrown away.
      */
      pg_usleep(1000000);

      ereport(ERROR,
              (errcode(ERRCODE_CONNECTION_FAILURE),
               errmsg("http response code %ld from gpfdist (%s): %s",
                      response_code, file->url,
                      file->u.curl.http_response ? file->u.curl.http_response
                                                 : "?")));
    }
  }

  return 0;
}

/**
 * Send curl request and check response.
 * If failed, will retry multiple times.
 * Return true if succeed, false otherwise.
 */
static void gp_curl_easy_perform_backoff_and_check_response(URL_FILE *file) {
  int response_code;
  char *response_string = NULL;

  /* retry in case server return timeout error */
  unsigned int wait_time = 1;
  unsigned int retry_count = 0;
  /* retry at most twice(300 seconds * 2) when CURLE_OPERATION_TIMEDOUT happens
   */
  unsigned int timeout_count = 0;

  while (true) {
    /*
     * Use backoff policy to call curl_easy_perform to fix following error
     * when work load is high:
     *  - 'could not connect to server'
     *  - gpfdist return timeout (HTTP 408)
     * By default it will wait at most 127 seconds before abort.
     * 1 + 2 + 4 + 8 + 16 + 32 + 64 = 127
     */
    CURLcode e = curl_easy_perform(file->u.curl.handle);
    if (CURLE_OK != e) {
      elog(WARNING, "%s error (%d - %s)", file->u.curl.curl_url, e,
           curl_easy_strerror(e));
      if (CURLE_OPERATION_TIMEDOUT == e) {
        timeout_count++;
      }
    } else {
      /* check the response from server */
      response_code = check_response(file, &response_code, &response_string);
      switch (response_code) {
        case 0:
          /* Success! */
          return;

        case FDIST_TIMEOUT:
          break;

        default:
          ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                          errmsg("error while getting response from gpfdist on "
                                 "%s (code %d, msg %s)",
                                 file->u.curl.curl_url, response_code,
                                 response_string)));
      }
      if (response_string) pfree(response_string);
      response_string = NULL;
    }

    if (wait_time > MAX_TRY_WAIT_TIME || timeout_count >= 2) {
      ereport(
          ERROR,
          (errcode(ERRCODE_CONNECTION_FAILURE),
           errmsg("error when writing data to gpfdist %s, quit after %d tries",
                  file->u.curl.curl_url, retry_count + 1)));
    } else {
      elog(
          WARNING,
          "failed to send request to gpfdist (%s), will retry after %d seconds",
          file->u.curl.curl_url, wait_time);
      unsigned int for_wait = 0;
      while (for_wait++ < wait_time) {
        pg_usleep(1000000);
        CHECK_FOR_INTERRUPTS();
      }
      wait_time = wait_time + wait_time;
      retry_count++;
    }
  }
}

/*
 * Send an empty POST request, with an added X-GP-DONE header.
 */
static void gp_proto0_write_done(URL_FILE *file) {
  set_httpheader(file, "X-GP-DONE", "1");

  /* use empty message */
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_POSTFIELDS, "");
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_POSTFIELDSIZE, 0);

  /* post away! */
  gp_curl_easy_perform_backoff_and_check_response(file);
}

/*
 * gp_proto0_read
 *
 * get data from the server and handle it according to PROTO 0. In PROTO 0 we
 * expect the content of the file without any kind of meta info. Simple.
 */
static size_t gp_proto0_read(char *buf, int bufsz, URL_FILE *file) {
  int n = 0;
  curlctl_t *curl = &file->u.curl;

  fill_buffer(file, bufsz);

  /* check if there's data in the buffer - if not fill_buffer()
   * either errored or EOF. For proto0, we cannot distinguish
   * between error and EOF. */
  n = curl->in.top - curl->in.bot;
  if (n == 0 && !curl->still_running) curl->eof = 1;

  if (n > bufsz) n = bufsz;

  /* xfer data to caller */
  memcpy(buf, curl->in.ptr, n);
  curl->in.bot += n;

  return n;
}

/*
 * gp_proto1_read
 *
 * get data from the server and handle it according to PROTO 1. In this protocol
 * each data block is tagged by meta info like this:
 * byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 * byte 1-4: length. # bytes of following data block. in network-order.
 * byte 5-X: the block itself.
 */
static size_t gp_proto1_read(char *buf, int bufsz, URL_FILE *file,
                             CopyState pstate, char *buf2) {
  char type;
  int n, len;
  curlctl_t *curl = &file->u.curl;

  /*
   * Loop through and get all types of messages, until we get actual data,
   * or until there's no more data. Then quit the loop to process it and
   * return it.
   */
  while (curl->block.datalen == 0 && !curl->eof) {
    /* need 5 bytes, 1 byte type + 4 bytes length */
    fill_buffer(file, 5);
    n = curl->in.top - curl->in.bot;

    if (n == 0)
      ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                      errmsg("gpfdist error: server closed connection")));

    if (n < 5)
      ereport(ERROR,
              (errcode(ERRCODE_CONNECTION_FAILURE),
               errmsg("gpfdist error: incomplete packet - packet len %d", n)));

    /* read type */
    type = curl->in.ptr[curl->in.bot++];

    /* read len */
    memcpy(&len, &curl->in.ptr[curl->in.bot], 4);
    len = ntohl(len); /* change order */
    curl->in.bot += 4;

    if (len < 0)
      elog(ERROR, "gpfdist error: bad packet type %d len %d", type, len);

    /* Error */
    if (type == 'E') {
      fill_buffer(file, len);
      n = curl->in.top - curl->in.bot;

      if (n > len) n = len;

      if (n > 0) {
        /*
         * cheat a little. swap last char and
         * NUL-terminator. then print string (without last
         * char) and print last char artificially
         */
        char x = curl->in.ptr[curl->in.bot + n - 1];
        curl->in.ptr[curl->in.bot + n - 1] = 0;
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("gpfdist error - %s%c",
                               &curl->in.ptr[curl->in.bot], x)));
      }

      elog(ERROR, "gpfdist error: please check gpfdist log messages.");
    }

    /* Filename */
    if (type == 'F') {
      if (buf != buf2) {
        curl->in.bot -= 5;
        return 0;
      }
      if (len > 256) elog(ERROR, "gpfdist error: filename too long (%d)", len);

      if (-1 == fill_buffer(file, len))
        elog(ERROR, "gpfdist error: stream ends suddenly");

      /*
       * If SREH is used we now update it with the actual file that the
       * gpfdist server is reading. This is because SREH (or the client
       * in general) doesn't know which file gpfdist is reading, since
       * the original URL may include a wildcard or a directory listing.
       */
      if (pstate->cdbsreh) {
        char fname[257];

        memcpy(fname, curl->in.ptr + curl->in.bot, len);
        fname[len] = 0;
        snprintf(pstate->cdbsreh->filename, sizeof pstate->cdbsreh->filename,
                 "%s [%s]", pstate->filename, fname);
      }

      curl->in.bot += len;
      Assert(curl->in.bot <= curl->in.top);
      continue;
    }

    /* Offset */
    if (type == 'O') {
      if (len != 8)
        elog(ERROR, "gpfdist error: offset not of length 8 (%d)", len);

      if (-1 == fill_buffer(file, len))
        elog(ERROR, "gpfdist error: stream ends suddenly");

      curl->in.bot += 8;
      Assert(curl->in.bot <= curl->in.top);
      continue;
    }

    /* Line number */
    if (type == 'L') {
      int64 line_number;

      if (len != 8)
        elog(ERROR, "gpfdist error: line number not of length 8 (%d)", len);

      if (-1 == fill_buffer(file, len))
        elog(ERROR, "gpfdist error: stream ends suddenly");

      /*
       * update the line number of the first line we're about to get from
       * gpfdist. pstate will update the following lines when processing
       * the data
       */
      memcpy(&line_number, curl->in.ptr + curl->in.bot, len);
      line_number = local_ntohll(line_number);
      pstate->cur_lineno = line_number ? line_number - 1 : INT64_MIN;
      curl->in.bot += 8;
      Assert(curl->in.bot <= curl->in.top);
      continue;
    }

    /* Data */
    if (type == 'D') {
      curl->block.datalen = len;
      curl->eof = (len == 0);
      break;
    }

    elog(ERROR, "gpfdist error: unknown meta type %d", type);
  }

  /* read data block */
  if (bufsz > curl->block.datalen) bufsz = curl->block.datalen;

  fill_buffer(file, bufsz);
  n = curl->in.top - curl->in.bot;

  /* if gpfdist closed connection prematurely or died catch it here */
  if (n == 0 && !curl->eof) {
    curl->error = 1;

    if (!curl->still_running)
      ereport(
          ERROR,
          (errcode(ERRCODE_CONNECTION_FAILURE),
           errmsg("gpfdist server closed connection"),
           errhint(
               "The root cause is likely to be an overload of the ETL host or "
               "a temporary network glitch between the database and the ETL "
               "host "
               "causing the connection between the gpfdist and database to "
               "disconnect.")));
  }

  if (n > bufsz) n = bufsz;

  memcpy(buf, curl->in.ptr + curl->in.bot, n);
  curl->in.bot += n;
  curl->block.datalen -= n;
  return n;
}

static size_t curl_fread(char *buf, int bufsz, URL_FILE *file,
                         CopyState pstate) {
  curlctl_t *curl = &file->u.curl;
  char *p = buf;
  char *q = buf + bufsz;
  int n;
  const int gp_proto = curl->gp_proto;

  if (gp_proto != 0 && gp_proto != 1)
    elog(ERROR, "unknown gp protocol %d", curl->gp_proto);

  for (; p < q; p += n) {
    if (gp_proto == 0)
      n = gp_proto0_read(p, q - p, file);
    else
      n = gp_proto1_read(p, q - p, file, pstate, buf);

    if (n <= 0) break;
  }

  return p - buf;
}

/*
 * gp_proto0_write
 *
 * use curl to write data to a the remote gpfdist server. We use
 * a push model with a POST request.
 *
 */
static void gp_proto0_write(URL_FILE *file, CopyState pstate) {
  curlctl_t *curl = &file->u.curl;
  char *buf = curl->out.ptr;
  int nbytes = curl->out.top;
  if (nbytes == 0) return;
  /* post binary data */
  CURL_EASY_SETOPT(curl->handle, CURLOPT_POSTFIELDS, buf);

  /* set the size of the postfields data */
  CURL_EASY_SETOPT(curl->handle, CURLOPT_POSTFIELDSIZE, nbytes);

  /* set sequence number */
  char seq[128] = {0};
  snprintf(seq, sizeof(seq), INT64_FORMAT, file->u.curl.seq_number);

  replace_httpheader(file, "X-GP-SEQ", seq);

  gp_curl_easy_perform_backoff_and_check_response(file);
  file->u.curl.seq_number++;
}

static size_t curl_fwrite(char *buf, int nbytes, URL_FILE *file,
                          CopyState pstate) {
  curlctl_t *curl = &file->u.curl;

  if (!curl->for_write)
    elog(ERROR, "cannot write to a read-mode external table");

  if (curl->gp_proto != 0 && curl->gp_proto != 1)
    elog(ERROR, "unknown gp protocol %d", curl->gp_proto);

  /*
   * if buffer is full (current item can't fit) - write it out to
   * the server. if item still doesn't fit after we emptied the
   * buffer, make more room.
   */
  if (curl->out.top + nbytes >= curl->out.max) {
    /* item doesn't fit */
    if (curl->out.top > 0) {
      /* write out existing data, empty the buffer */
      gp_proto0_write(file, pstate);
      curl->out.top = 0;
    }

    /* does it still not fit? enlarge buffer */
    if (curl->out.top + nbytes >= curl->out.max) {
      int n = nbytes + 1024;
      char *newbuf;

      MemoryContext oldctx;
      oldctx = MemoryContextSwitchTo(TopMemoryContext);
      newbuf = repalloc(curl->out.ptr, n);
      MemoryContextSwitchTo(oldctx);

      if (!newbuf) elog(ERROR, "out of memory (curl_fwrite)");

      curl->out.ptr = newbuf;
      curl->out.max = n;

      Assert(nbytes < curl->out.max);
    }
  }

  /* copy buffer into file->buf */
  memcpy(curl->out.ptr + curl->out.top, buf, nbytes);
  curl->out.top += nbytes;

  return nbytes;
}

URL_FILE *url_curl_fopen(char *url, bool forwrite, extvar_t *ev,
                         CopyState pstate) {
  Assert(IS_HTTP_URI(url) || IS_GPFDIST_URI(url));

  MemoryContext oldctx;
  oldctx = MemoryContextSwitchTo(TopMemoryContext);

  URL_FILE *file = NULL;

  bool is_ipv6 = url_has_ipv6_format(url);
  int sz = make_url(url, NULL, is_ipv6);
  if (sz < 0) elog(ERROR, "illegal URL: %s", url);

  file = (URL_FILE *)palloc0(sizeof(URL_FILE));
  file->type = CFTYPE_CURL;
  file->url = pstrdup(url);
  file->u.curl.for_write = forwrite;

  file->u.curl.curl_url = (char *)palloc0(sz + 1);
  make_url(file->url, file->u.curl.curl_url, is_ipv6);
  /*
   * We need to call is_url_ipv6 for the case where inside make_url function
   * a domain name was transformed to an IPv6 address.
   */
  if (!is_ipv6) is_ipv6 = url_has_ipv6_format(file->u.curl.curl_url);

  if (IS_GPFDIST_URI(file->u.curl.curl_url)) {
    /* replace gpfdist:// with http:// or gpfdists:// with https://
     * by overriding 'dist' with 'http' */
    unsigned int tmp_len = strlen(file->u.curl.curl_url) + 1;
    memmove(file->u.curl.curl_url, file->u.curl.curl_url + 3, tmp_len - 3);
    memcpy(file->u.curl.curl_url, "http", 4);
    pstate->header_line = 0;
  }

  /* initialize a curl session and get a libcurl handle for it */
  if (!(file->u.curl.handle = curl_easy_init()))
    elog(ERROR, "internal error: curl_easy_init failed");

  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_URL, file->u.curl.curl_url);

  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_VERBOSE, 0L /* FALSE */);

  /* set callback for each header received from server */
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_HEADERFUNCTION,
                   header_callback);

  /* 'file' is the application variable that gets passed to header_callback */
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_WRITEHEADER, file);

  /* set callback for each data block arriving from server to be written to
   * application */
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_WRITEFUNCTION, write_callback);

  /* 'file' is the application variable that gets passed to write_callback */
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_WRITEDATA, file);

  int ip_mode;
  if (!is_ipv6)
    ip_mode = CURL_IPRESOLVE_V4;
  else
    ip_mode = CURL_IPRESOLVE_V6;
  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_IPRESOLVE, ip_mode);

  /*
   * set up a linked list of http headers. start with common headers
   * needed for read and write operations, and continue below with
   * more specifics
   */
  Assert(file->u.curl.x_httpheader == NULL);

  /*
   * support multihomed http use cases. see MPP-11874
   */
  if (IS_HTTP_URI(url)) {
    char domain[HOST_NAME_SIZE] = {0};

    extract_http_domain(file->url, domain, HOST_NAME_SIZE);
    set_httpheader(file, "Host", domain);
  }

  set_httpheader(file, "X-GP-XID", ev->GP_XID);
  set_httpheader(file, "X-GP-CID", ev->GP_CID);
  set_httpheader(file, "X-GP-SN", ev->GP_SN);
  set_httpheader(file, "X-GP-SEGMENT-ID", ev->GP_SEGMENT_ID);
  set_httpheader(file, "X-GP-SEGMENT-COUNT", ev->GP_SEGMENT_COUNT);

  if (forwrite) {
    // TIMEOUT for POST only, GET is single HTTP request,
    // probablity take long time.
    CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_TIMEOUT, 300L);

    /*init sequence number*/
    file->u.curl.seq_number = 1;

    /* write specific headers */
    set_httpheader(file, "X-GP-PROTO", "0");
    set_httpheader(file, "X-GP-SEQ", "1");
    set_httpheader(file, "Content-Type", "text/xml");
  } else {
    /* read specific - (TODO: unclear why some of these are needed) */
    set_httpheader(file, "X-GP-PROTO", "1");
    set_httpheader(file, "X-GP-MASTER_HOST", ev->GP_MASTER_HOST);
    set_httpheader(file, "X-GP-MASTER_PORT", ev->GP_MASTER_PORT);
    set_httpheader(file, "X-GP-CSVOPT", ev->GP_CSVOPT);
    set_httpheader(file, "X-GP_SEG_PG_CONF", ev->GP_SEG_PG_CONF);
    set_httpheader(file, "X-GP_SEG_DATADIR", ev->GP_SEG_DATADIR);
    set_httpheader(file, "X-GP-DATABASE", ev->GP_DATABASE);
    set_httpheader(file, "X-GP-USER", ev->GP_USER);
    set_httpheader(file, "X-GP-SEG-PORT", ev->GP_SEG_PORT);
    set_httpheader(file, "X-GP-SESSION-ID", ev->GP_SESSION_ID);
  }

  {
    /*
     * MPP-13031
     * copy #transform fragment, if present, into X-GP-TRANSFORM header
     */
    char *p = local_strstr(file->url, "#transform=");
    if (p && p[11]) set_httpheader(file, "X-GP-TRANSFORM", p + 11);
  }

  CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_HTTPHEADER,
                   file->u.curl.x_httpheader);

  if (!multi_handle) {
    if (!(multi_handle = curl_multi_init()))
      elog(ERROR, "internal error: curl_multi_init failed");
  }

  /* Allocate input and output buffers. */
  file->u.curl.in.ptr = palloc(1024); /* 1 kB buffer initially */
  file->u.curl.in.max = 1024;
  file->u.curl.in.bot = file->u.curl.in.top = 0;

  if (forwrite) {
    int bufsize = writable_external_table_bufsize * 1024;
    file->u.curl.out.ptr = (char *)palloc(bufsize);
    file->u.curl.out.max = bufsize;
    file->u.curl.out.bot = file->u.curl.out.top = 0;
  }

  MemoryContextSwitchTo(oldctx);

  /*
   * lets check our connection.
   * start the fetch if we're SELECTing (GET request), or write an
   * empty message if we're INSERTing (POST request)
   */
  if (!forwrite) {
    int e;
    int response_code;
    char *response_string;

    if (CURLE_OK !=
        (e = curl_multi_add_handle(multi_handle, file->u.curl.handle))) {
      if (CURLM_CALL_MULTI_PERFORM != e)
        elog(ERROR, "internal error: curl_multi_add_handle failed (%d - %s)", e,
             curl_easy_strerror(e));
    }
    while (CURLM_CALL_MULTI_PERFORM ==
           (e = curl_multi_perform(multi_handle, &file->u.curl.still_running)))
      ;

    if (e != CURLE_OK)
      elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)", e,
           curl_easy_strerror(e));

    /* read some bytes to make sure the connection is established */
    fill_buffer(file, 1);

    /* check the connection for GET request */
    // When other vseg has read all data and this vseg attend to read 1 byte to
    // check connection, it may get error "timed out".
    // If error is not "timed out", we will still report error.
    if (check_response(file, &response_code, &response_string))
      ereport(ERROR,
              (errcode(ERRCODE_CONNECTION_FAILURE),
               errmsg("could not open \"%s\" for reading", file->url),
               errdetail("Unexpected response from gpfdist server: %d - %s",
                         response_code, response_string)));

    if (file->u.curl.still_running == 0) {
      elog(LOG,
           "session closed when checking the connection in url_curl_fopen, "
           "http_response = \"%s\".",
           file->u.curl.http_response);
    }

  } else {
    /* use empty message */
    CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_POSTFIELDS, "");
    CURL_EASY_SETOPT(file->u.curl.handle, CURLOPT_POSTFIELDSIZE, 0);

    /* post away and check response, retry if failed (timeout or * connect
     * error) */
    gp_curl_easy_perform_backoff_and_check_response(file);
    file->u.curl.seq_number++;
  }

  return file;
}

void url_curl_fclose(URL_FILE *file, bool failOnError, const char *relname) {
  /*
   * if WET, send a final "I'm done" request from this segment.
   */
  if (file->u.curl.for_write && file->u.curl.handle != NULL)
    gp_proto0_write_done(file);

  if (file->u.curl.x_httpheader) {
    curl_slist_free_all(file->u.curl.x_httpheader);
    file->u.curl.x_httpheader = NULL;
  }

  /* make sure the easy handle is not in the multi handle anymore */
  if (file->u.curl.handle) {
    curl_multi_remove_handle(multi_handle, file->u.curl.handle);
    /* cleanup */
    curl_easy_cleanup(file->u.curl.handle);
    file->u.curl.handle = NULL;
  }

  /* free any allocated buffer space */
  if (file->u.curl.in.ptr) {
    pfree(file->u.curl.in.ptr);
    file->u.curl.in.ptr = NULL;
  }

  if (file->u.curl.curl_url) {
    pfree(file->u.curl.curl_url);
    file->u.curl.curl_url = NULL;
  }

  if (file->u.curl.out.ptr) {
    Assert(file->u.curl.for_write);
    pfree(file->u.curl.out.ptr);
    file->u.curl.out.ptr = NULL;
  }

  file->u.curl.gp_proto = 0;
  file->u.curl.error = file->u.curl.eof = 0;
  memset(&file->u.curl.in, 0, sizeof(file->u.curl.in));
  memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));

  pfree(file->url);

  pfree(file);
}

bool url_curl_feof(URL_FILE *file, int bytesread) {
  return (file->u.curl.eof != 0);
}

bool url_curl_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen) {
  return (file->u.curl.error != 0);
}

size_t url_curl_fread(void *ptr, size_t size, URL_FILE *file,
                      CopyState pstate) {
  /* get data (up size) from the http/gpfdist server */
  return curl_fread(ptr, size, file, pstate);
}

size_t url_curl_fwrite(void *ptr, size_t size, URL_FILE *file,
                       CopyState pstate) {
  /* write data to the gpfdist server via curl */
  return curl_fwrite(ptr, size, file, pstate);
}

void url_curl_fflush(URL_FILE *file, CopyState pstate) {
  gp_proto0_write(file, pstate);
}

void url_curl_rewind(URL_FILE *file, const char *relname) {
  /* halt transaction */
  curl_multi_remove_handle(multi_handle, file->u.curl.handle);

  /* restart */
  curl_multi_add_handle(multi_handle, file->u.curl.handle);

  /* ditch buffer - write will recreate - resets stream pos*/
  if (file->u.curl.in.ptr) pfree(file->u.curl.in.ptr);

  file->u.curl.gp_proto = 0;
  file->u.curl.error = file->u.curl.eof = 0;
  memset(&file->u.curl.in, 0, sizeof(file->u.curl.in));
  memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));
}
