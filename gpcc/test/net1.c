#include <gpcc_net.h>
#include <apr_getopt.h>
#include <stdio.h>
#include <stdlib.h>


void checkerr(int e, const char* fline)
{
    if (e) {
	fprintf(stderr, "%s: error %d\n", fline, e);
	exit(1);
    }
}


void tick(apr_time_t now)
{
    static int start_sec = 0;
    int sec = apr_time_sec(now);
    if (! start_sec)
	start_sec = sec;
    sec -= start_sec;
    printf("tick: %d\n", sec);
}


void svr_recvfrom(const char* peeraddr, const char* buf, int buflen)
{
    int e;
    printf("---- svr_recvfrom --------------------------\n");
    printf(" -- from: %s\n", peeraddr);
    printf(" -- msg: %s\n", buf);
    e = gpcc_net_sendto(peeraddr, buf, buflen);
    checkerr(e, FLINE);
}



void svr_opened(gpcc_net_conn_t conn, const char* peer_addr,
		gpcc_net_header_t* to_peer, gpcc_net_header_t* from_peer,
		apr_uint64_t arg)
{
    printf("----- svr_opened --------------------------\n");
    gpcc_fatal(FLINE, "INTERNAL ERROR - server does not open\n");
}

void svr_accepted(gpcc_net_conn_t conn, const char* peer_addr, gpcc_net_header_t* from_peer)
{
    int e;
    char* s = "SERVER ACCEPTED\r\n\r\n";
    const char** p;
    const char** q;
    printf("----- svr_accepted --------------------------\n");
    printf("  peer_addr: %s\n", peer_addr);
    printf("  > ");
    for (p = from_peer->argv; *p; p++) {
	printf("%s ", *p);
    }
    printf("\n");

    for (p = from_peer->hname, q = from_peer->hvalue; *p; p++, q++) {
	printf("  > %s: %s\n", *p, *q);
    }
    printf("\n");
    
    /* send a header */
    e = gpcc_net_send(conn, s, strlen(s));
    checkerr(e, FLINE);
    /*gpcc_net_close(conn);*/
}

int svr_received(gpcc_net_conn_t conn, const char* buf, int buflen, apr_uint64_t arg)
{
    int e;
    int i;
    printf("----- svr_received --------------------------\n");
    if (buflen <= 0) {
	gpcc_fatal(FLINE, "INTERNAL ERROR - buflen %d", buflen);
    }
    for (i = 0; i < buflen; i++) {
	putchar(buf[i]);
    }
    e = gpcc_net_send(conn, buf, buflen);
    checkerr(e, FLINE);
    return buflen;
}

void received_packet(gpcc_net_conn_t conn, gpcc_net_packet_t* pkt, apr_uint64_t arg)
{
    printf("----- received packet--------------------------\n");
    gpcc_fatal(FLINE, "INTERNAL ERROR - this test does not exercise packet logic");
}



void svr_closed(gpcc_net_conn_t conn, apr_uint64_t arg)
{
    printf("----- svr_closed --------------------------\n");
}


void do_send(gpcc_net_conn_t conn)
{
    static int count = 0;
    char buf[100];
    int e;
    sprintf(buf, " -- hello %d -- \n", ++count);
    e = gpcc_net_send(conn, buf, strlen(buf));
    checkerr(e, FLINE);
}


void do_sendto(const char* peer)
{
    static int count = 0;
    char buf[100];
    int e;
    sprintf(buf, " -- hello %d -- \n", ++count);
    e = gpcc_net_sendto(peer, buf, strlen(buf));
    checkerr(e, FLINE);
}

void clnt_opened(gpcc_net_conn_t conn, const char* peer_addr, 
		 gpcc_net_header_t* to_peer, gpcc_net_header_t* from_peer,
		 apr_uint64_t arg)
{
    printf("----- clnt_opened --------------------------\n");
    printf("   peer_addr: %s\n", peer_addr);
    do_send(conn);
}



void clnt_accepted(gpcc_net_conn_t conn, const char* peer_addr, gpcc_net_header_t* from_peer)
{
    printf("----- clnt_accepted --------------------------\n");
    gpcc_fatal(FLINE, "INTERNAL ERROR - client does not accept");
}

int clnt_received(gpcc_net_conn_t conn, const char* buf, int buflen, apr_uint64_t arg)
{
    const char* q = buf + buflen;
    const char* p = buf;
    printf("----- clnt_received --------------------------\n");
    while (p < q) {
	if (q[-1] == '\n')
	    break;
	q--;
    }
    if (p < q) {
	int ch;
	while (p < q) {
	    ch = *p++;
	    putchar(ch);
	}
	printf("Press [RETURN] to send the next line: ");
	fflush(stdout);
	ch = getchar();
	do_send(conn);
    }
    return p - buf;
}

void clnt_recvfrom(const char* peer, const char* buf, int buflen)
{
    int ch;
    printf("----- clnt_recvfrom --------------------------\n");
    printf("   -- peer: %s\n", peer);
    printf("   -- msg; %s\n", buf);

    printf("Press [RETURN] to send the next line: ");
    fflush(stdout);
    ch = getchar();
    do_sendto(peer);
}


void clnt_closed(gpcc_net_conn_t conn, apr_uint64_t arg)
{
    printf("----- clnt_closed --------------------------\n");
}



void usage(const char* pname)
{
    fprintf(stderr, "usage: %s [-u] [-s remote] port\n\n", pname);
    fprintf(stderr, "\t-u        : Use UDP protocol\n");
    fprintf(stderr, "\t-s remote : Name or IP of server\n");
    fprintf(stderr, "\tport      : If '-s' is not specified, then this is the local port\n");
    fprintf(stderr, "\t            and %s will function as an echo server. \n", pname);
    fprintf(stderr, "\t            Otherwise, %s will function as a client, and sends some \n", pname);
    fprintf(stderr, "\t            strings to the remote server\n");
    exit(1);
}


int main(int argc, const char* argv[])
{
    int e;
    const char* svr = 0;
    int port = 0;
    int udp = 0;
    apr_pool_t* pool;
    apr_getopt_t* opt;

    e = apr_initialize();
    checkerr(e, FLINE);

    atexit(apr_terminate);

    e = apr_pool_create(&pool, 0);
    checkerr(e, FLINE);

    e = apr_getopt_init(&opt, pool, argc, argv);
    checkerr(e, FLINE);

    for (;;) {
	char ch;
	const char* arg;
	e = apr_getopt(opt, "us:", &ch, &arg);
	if (e == APR_EOF)
	    break;
	checkerr(e, FLINE);
	if (ch == 'u') udp = 1;
	else if (ch == 's') svr = arg;
    }

    if (opt->ind != opt->argc - 1)
	usage(argv[0]);

    port = strtol(argv[opt->ind], 0, 0);
    if (! (0 < port && port < (1 << 16)))
	usage(argv[0]);

    if (! svr) {
	e = gpcc_net_init(port, svr_recvfrom, svr_opened, svr_accepted, svr_received, received_packet, svr_closed, tick);
	checkerr(e, FLINE);
	printf("Listening on port %d\n", port);
    }
    else {
	gpcc_net_conn_t conn;
	static const char* av[] = {"HELLO", "1", "2", "3" };
	static const char* hn[] = { "N1", "N2", "N3" };
	static const char* hv[] = { "V1", "V2", "V3" };
	char addr[100];

	e = gpcc_net_init(0, clnt_recvfrom, clnt_opened, clnt_accepted, clnt_received, received_packet, clnt_closed, tick);
	checkerr(e, FLINE);

	sprintf(addr, "%s:%d", svr, port);

	if (udp) {
	    do_sendto(addr);
	}
	else {
	    printf("Connecting to %s\n", addr);
	    e = gpcc_net_open(&conn, addr, 3, av, 3, hn, hv, 0);
	    checkerr(e, FLINE);
	}
    }

    while (1) {
	e = gpcc_net_once(1);
	checkerr(e, FLINE);
    }

    return 0;
}

