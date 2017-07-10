#include <fcntl.h>
#include <netinet/tcp.h>
#include <stddef.h>
#include <string.h>
#include "brubeck.h"

static void set_blocking_mode(int sock, bool blocking)
{
	int flags = fcntl(sock, F_GETFL, 0);
	if (flags < 0) {
		log_splunk_errno("backend=carbon event=F_GETFL error");
		return;
	}
	if (blocking) {
		flags = flags & ~O_NONBLOCK;

	} else {
		flags = flags | O_NONBLOCK;
	}
	if (fcntl(sock, F_SETFL, flags) < 0) {
		log_splunk_errno("backend=carbon event=F_SETFL error");
		return;
	}
}

static void prepare_socket(int sock)
{
	// int optval = 1;
	// if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof optval) < 0) {
	// 	log_splunk_errno("backend=carbon event=keep-alive error");
	// }
	// int keep_alive_time = 10;
	// if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, &keep_alive_time, sizeof keep_alive_time) < 0) {
	// 	log_splunk_errno("backend=carbon event=keep-alive-time error");
	// }
	// int keep_alive_count = 2;
	// if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, &keep_alive_count, sizeof keep_alive_count) < 0) {
	// 	log_splunk_errno("backend=carbon event=keep-alive-cnt error");
	// }
	// int keep_alive_interval = 5;
	// if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, &keep_alive_interval, sizeof keep_alive_interval) < 0) {
	// 	log_splunk_errno("backend=carbon event=keep-alive-intl error");
	// }
	int user_timeout = 5;
	if (setsockopt(sock, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout, sizeof user_timeout) < 0) {
		log_splunk_errno("backend=carbon event=tcp-user-timeout error");
	}

}

static int check_timeout(int sock, time_t timeout)
{
	struct timeval tv;
	int valopt;
	fd_set ss;
	tv.tv_sec = timeout;
	tv.tv_usec = 0;
	FD_ZERO(&ss);
	FD_SET(sock, &ss);
	if (select(sock+1, NULL, &ss, NULL, &tv) > 0) {
	   socklen_t len = sizeof(valopt);
	   getsockopt(sock, SOL_SOCKET, SO_ERROR, &valopt, &len);
	   if (valopt) {
		errno = valopt;
		return -1;
	   }
	} else {
	   errno = ETIMEDOUT;
	   return -1;
	}
	return 0;
}

static bool carbon_is_connected(void *backend)
{
	struct brubeck_carbon *self = (struct brubeck_carbon *)backend;
	return (self->out_sock >= 0);
}

static int carbon_connect(void *backend)
{
	struct brubeck_carbon *self = (struct brubeck_carbon *)backend;

	if (carbon_is_connected(self))
		return 0;

	self->out_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	prepare_socket(self->out_sock);

	if (self->out_sock >= 0) {
		set_blocking_mode(self->out_sock, false);
		int rc = connect(self->out_sock,
				(struct sockaddr *)&self->out_sockaddr,
				sizeof(self->out_sockaddr));

		if (rc == 0) {
			log_splunk("backend=carbon event=connected");
			sock_enlarge_out(self->out_sock);
			return 0;
		}
		if (rc == -1 && errno == EINPROGRESS) {
			if (check_timeout(self->out_sock, self->timeout) == 0) {
				set_blocking_mode(self->out_sock, true);
				log_splunk("backend=carbon event=connected (T)");
				sock_enlarge_out(self->out_sock);
				return 0;
			}
		}

		close(self->out_sock);
		self->out_sock = -1;
	}

	log_splunk_errno("backend=carbon event=failed_to_connect");
	return -1;
}

static void carbon_disconnect(struct brubeck_carbon *self)
{
	log_splunk_errno("backend=carbon event=disconnected");

	close(self->out_sock);
	self->out_sock = -1;
}

static void plaintext_each(
	const char *key,
	value_t value,
	void *backend)
{
	struct brubeck_carbon *carbon = (struct brubeck_carbon *)backend;
	char buffer[1024];
	char *ptr = buffer;
	size_t key_len = strlen(key);
	ssize_t wr;

	if (!carbon_is_connected(carbon))
		return;

	memcpy(ptr, key, key_len);
	ptr += key_len;
	*ptr++ = ' ';

	ptr += brubeck_ftoa(ptr, value);
	*ptr++ = ' ';

	ptr += brubeck_itoa(ptr, carbon->backend.tick_time);
	*ptr++ = '\n';

	wr = write_in_full(carbon->out_sock, buffer, ptr - buffer);
	if (wr < 0) {
		carbon_disconnect(carbon);
		return;
	}

	carbon->sent += wr;
}

static inline size_t pickle1_int32(char *ptr, void *_src)
{
	*ptr = 'J';
	memcpy(ptr + 1, _src, 4);
	return 5;
}

static inline size_t pickle1_double(char *ptr, void *_src)
{
	uint8_t *source = _src;

	*ptr++ = 'G';

	ptr[0] = source[7];
	ptr[1] = source[6];
	ptr[2] = source[5];
	ptr[3] = source[4];
	ptr[4] = source[3];
	ptr[5] = source[2];
	ptr[6] = source[1];
	ptr[7] = source[0];

	return 9;
}

static void pickle1_push(
		struct pickler *buf,
		const char *key,
		uint8_t key_len,
		uint32_t timestamp,
		value_t value)
{
	char *ptr = buf->ptr + buf->pos;

	*ptr++ = '(';

	*ptr++ = 'U';
	*ptr++ = key_len;
	memcpy(ptr, key, key_len);
	ptr += key_len;

	*ptr++ = 'q';
	*ptr++ = buf->pt++;

	*ptr++ = '(';

	ptr += pickle1_int32(ptr, &timestamp);
	ptr += pickle1_double(ptr, &value);

	*ptr++ = 't';
	*ptr++ = 'q';
	*ptr++ = buf->pt++;

	*ptr++ = 't';
	*ptr++ = 'q';
	*ptr++ = buf->pt++;

	buf->pos = (ptr - buf->ptr);
}

static inline void pickle1_init(struct pickler *buf)
{
	static const uint8_t lead[] = { ']', 'q', 0, '(' };

	memcpy(buf->ptr + 4, lead, sizeof(lead));
	buf->pos = 4 + sizeof(lead);
	buf->pt = 1;
}

static void pickle1_flush(void *backend)
{
	static const uint8_t trail[] = {'e', '.'};

	struct brubeck_carbon *carbon = (struct brubeck_carbon *)backend;
	struct pickler *buf = &carbon->pickler;

	uint32_t *buf_lead;
	ssize_t wr;

	if (buf->pt == 1 || !carbon_is_connected(carbon))
		return;

	memcpy(buf->ptr + buf->pos, trail, sizeof(trail));
	buf->pos += sizeof(trail);

	buf_lead = (uint32_t *)buf->ptr;
	*buf_lead = htonl((uint32_t)buf->pos - 4);

	wr = write_in_full(carbon->out_sock, buf->ptr, buf->pos);

	pickle1_init(&carbon->pickler);
	if (wr < 0) {
		carbon_disconnect(carbon);
		return;
	}

	carbon->sent += wr;
}

static void pickle1_each(
	const char *key,
	value_t value,
	void *backend)
{
	struct brubeck_carbon *carbon = (struct brubeck_carbon *)backend;
	uint8_t key_len = (uint8_t)strlen(key);

	if (carbon->pickler.pos + PICKLE1_SIZE(key_len)
		>= PICKLE_BUFFER_SIZE) {
		pickle1_flush(carbon);
	}

	if (!carbon_is_connected(carbon))
		return;

	pickle1_push(&carbon->pickler, key, key_len,
		carbon->backend.tick_time, value);
}

struct brubeck_backend *
brubeck_carbon_new(struct brubeck_server *server, json_t *settings, int shard_n)
{
	struct brubeck_carbon *carbon = xcalloc(1, sizeof(struct brubeck_carbon));
	char *address;
	int port, frequency, pickle = 0;
	int timeout = CONN_TIMEOUT;

	json_unpack_or_die(settings,
		"{s:s, s:i, s?:b, s:i, s?:i}",
		"address", &address,
		"port", &port,
		"pickle", &pickle,
		"frequency", &frequency,
		"timeout", &timeout);

	carbon->backend.type = BRUBECK_BACKEND_CARBON;
	carbon->backend.shard_n = shard_n;
	carbon->backend.connect = &carbon_connect;
	carbon->backend.is_connected = &carbon_is_connected;

	if (pickle) {
		carbon->backend.sample = &pickle1_each;
		carbon->backend.flush = &pickle1_flush;
		carbon->pickler.ptr = malloc(PICKLE_BUFFER_SIZE);
		pickle1_init(&carbon->pickler);
	} else {
		carbon->backend.sample = &plaintext_each;
		carbon->backend.flush = NULL;
	}

	carbon->backend.sample_freq = frequency;
	carbon->timeout = timeout;
	carbon->backend.server = server;
	carbon->out_sock = -1;
	url_to_inaddr2(&carbon->out_sockaddr, address, port);

	brubeck_backend_run_threaded((struct brubeck_backend *)carbon);
	log_splunk("backend=carbon event=started");

	return (struct brubeck_backend *)carbon;
}
