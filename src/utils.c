#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netdb.h>

#include "brubeck.h"

#ifdef __gnu_linux__
#define LARGE_SOCK_SIZE 33554431
#else
#define LARGE_SOCK_SIZE 4096
#endif

void sock_setnonblock(int fd)
{
	int flags;

	flags = fcntl(fd, F_GETFL);
	flags |= O_NONBLOCK;

	if (fcntl(fd, F_SETFL, flags) < 0)
		die("Failed to set O_NONBLOCK");
}

void sock_setreuse(int fd, int reuse)
{
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1)
		die("Failed to set SO_REUSEADDR");
}

void sock_enlarge_in(int fd)
{
	int bs = LARGE_SOCK_SIZE;

	if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bs, sizeof(bs)) == -1)
		die("Failed to set SO_RCVBUF");
}

void sock_enlarge_out(int fd)
{
	int bs = LARGE_SOCK_SIZE;

	if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bs, sizeof(bs)) == -1)
		die("Failed to set SO_SNDBUF");
}

void sock_setreuse_port(int fd, int reuse)
{
#ifdef SO_REUSEPORT
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) == -1)
		die("failed to set SO_REUSEPORT");
#endif
}

void url_to_inaddr2(struct sockaddr_in *addr, const char *url, int port)
{
	memset(addr, 0x0, sizeof(struct sockaddr_in));

	if (url) {
		struct addrinfo hints;
		struct addrinfo *result, *rp;

		memset(&hints, 0, sizeof(struct addrinfo));
		hints.ai_family = AF_INET;

		if (getaddrinfo(url, NULL, &hints, &result) != 0)
			die("failed to resolve address '%s'", url);

		/* Look for the first IPv4 address we can find */
		for (rp = result; rp; rp = rp->ai_next) {
			if (result->ai_family == AF_INET &&
				result->ai_addrlen == sizeof(struct sockaddr_in))
				break;
		}

		if (!rp)
			die("address format not supported");

		memcpy(addr, rp->ai_addr, rp->ai_addrlen);
		addr->sin_port = htons(port);

		freeaddrinfo(result);
	} else {
		addr->sin_family = AF_INET;
		addr->sin_port = htons(port);
		addr->sin_addr.s_addr = htonl(INADDR_ANY);
	}
}

int brubeck_itoa(char *ptr, uint32_t number)
{
	char *origin = ptr;
	int size;

	do {
		*ptr++ = '0' + (number % 10);
		number /= 10;
	} while (number);

	size = ptr - origin;
	ptr--;

	while (origin < ptr) {
		char t = *ptr;
		*ptr-- = *origin;
		*origin++ = t;
	}

	return size;
}
int brubeck_ftoa(char *outbuf, double f) {
  int n = sprintf(outbuf, "%.3f", f);
  if (n < 1) return n;
  int sub = 0;
  char *p = outbuf + n - 1;
  while (*p == '0') {
    sub++;
    p--;
  }
  if (*p == '.') {
    sub++;
    p--;
  }
  *++p = '\0';
  return n - sub;
}
