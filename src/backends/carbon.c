#include <stddef.h>
#include <string.h>
#include "brubeck.h"

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

	if (self->out_sock >= 0) {
		int rc = connect(self->out_sock,
				(struct sockaddr *)&self->out_sockaddr,
				sizeof(self->out_sockaddr));
		
		if (rc == 0) {
			log_splunk("backend=carbon event=connected");
			sock_enlarge_out(self->out_sock);
			return 0;
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



struct brubeck_backend *
brubeck_carbon_new(struct brubeck_server *server, json_t *settings, int shard_n)
{
	struct brubeck_carbon *carbon = xcalloc(1, sizeof(struct brubeck_carbon));
	char *address;
	int port, frequency;

	json_unpack_or_die(settings,
		"{s:s, s:i, s?:b, s:i}",
		"address", &address,
		"port", &port,
		"frequency", &frequency);

	carbon->backend.type = BRUBECK_BACKEND_CARBON;
	carbon->backend.shard_n = shard_n;
	carbon->backend.connect = &carbon_connect;
	carbon->backend.is_connected = &carbon_is_connected;


	carbon->backend.sample = &plaintext_each;
	carbon->backend.flush = NULL;

	carbon->backend.sample_freq = frequency;
	carbon->backend.server = server;
	carbon->out_sock = -1;
	url_to_inaddr2(&carbon->out_sockaddr, address, port);

	brubeck_backend_run_threaded((struct brubeck_backend *)carbon);
	log_splunk("backend=carbon event=started");

	return (struct brubeck_backend *)carbon;
}
