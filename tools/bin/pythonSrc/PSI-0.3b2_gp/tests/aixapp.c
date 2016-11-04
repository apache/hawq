/** Program to test AIX priority values
 *
 * This program will schedule itself on the SCHED_RR scheduler and set it's
 * priority to 42.  Then it will print it's pid and do nothing for 10 minutes.
 * You can finish it earlier by sending a SIGTERM.
 
 * Since it changes the scheduler it will have to run as root and it will only
 * run on AIX due to the AIX specific setpri() system call. */


#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/sched.h>
#include <unistd.h>


int
main(int argc, char **argv)
{
    int i;

    if (setpri(0, 42) < 0) {
        printf("%d: %s\nAre you root?\n", errno, strerror(errno));
        return 1;
    }

    printf("%ld\n", (long)getpid());
    fflush(stdout);

    for (i = 0; i < 60; i++)
        sleep(10);
    return 0;
}
