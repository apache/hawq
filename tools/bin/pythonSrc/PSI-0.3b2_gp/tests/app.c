/* Simple executable that will print it's pid and then do nothing for 10
 * minutes.  You can finish it earlier by sending a SIGTERM. */

#include <stdio.h>
#include <unistd.h>


int
main(int argc, char **argv)
{
    int i;

    printf("%ld\n", (long)getpid());
    fflush(stdout);

    for (i = 0; i < 60; i++)
        sleep(10);
    return 0;
}
