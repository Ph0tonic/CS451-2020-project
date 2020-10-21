#pragma once
#include "parser.hpp"
#include <pthread.h>
#include <endian.h>
// #include <atomic.h>

int configureUdpSocket(Parser::Host const &server);

// TODO: In order
// Reliable link -> en cours
// Uniform reliable broadcast
// FIFO Broadcast
// LCausal Broadcast

void broadcastToAll(std::vector<Parser::Host> const &hosts, int const fd);

/* PACKET AND MESSAGE DEFINITION */

/* Structure of message required -> minimized size
  packet_id
  source_id or instead reversed ip and port source ??? 
  message, is it present ?
*/
struct message
{
  int32_t packet_id;
  int32_t source_id;
  sockaddr_in addr;
  //TODO: No addition content than the packet_id and source_id
};

struct packet
{
  char buffer[8];
};

struct packet *encode_message(message *m)
{
  packet *p = (packet *)malloc(sizeof(packet));
  strncpy(p->buffer, (char *)htonl(m->packet_id), sizeof(int32_t));
  strncpy(p->buffer + sizeof(int32_t), (char *)htonl(m->packet_id), sizeof(int32_t));
  return p;
}

struct message *decode_packet(packet *p)
{
  message *m = (message *)malloc(sizeof(message));
  m->packet_id = nltoh((int32_t *)p->buffer);
  m->source_id = nltoh(((int32_t *)p->buffer) + 1);
  return m;
}

/* Data Structures */

// TODO: Concurrent Queue Single Producer/ Single Consumer
typedef struct scsp_queue
{
  volatile size_t head;
  volatile size_t tail;
  size_t size;
  message **circular_buffer;
} scsp_queue_t;

typedef struct scmp_queue
{
  volatile bool new_data;
  scsp_queue *queues;
  size_t nb_queues;
} scmp_queue_t;

scmp_queue_t *new_scmp_queue(size_t nb_producers)
{
  scmp_queue_t *queue = (scmp_queue_t *)malloc(sizeof(scmp_queue_t *));
  queue->new_data = 0;
  queue->queues = (scsp_queue_t *)malloc(sizeof(scsp_queue_t *) * nb_producers);
  return queue;
}

scsp_queue_t *new_scsp_queue(size_t size)
{
  scsp_queue_t *queue = (scsp_queue *)malloc(sizeof(scsp_queue_t *));
  queue->head = 0;
  queue->tail = 0;
  queue->size = size;
  queue->circular_buffer = (message **)malloc(sizeof(message *) * size);
  return queue;

  // TODO: Free memory
}

void push(scsp_queue_t *queue, message *msg)
{
  queue->circular_buffer[queue->head] = msg;
  //atomic_set(); // TODO:
  queue->head = (queue->head + 1) % queue->size;
}

void push(scmp_queue_t *queue, size_t id, message *msg)
{
  push(&queue[id], msg);
  queue->new_data = true;
}

message *pop(scmp_queue_t *queue, size_t id)
{
  return pop(queue[id]);
}


message *pop(scsp_queue_t *queue, size_t id)
{
  return pop(queue[id]);
}

/* NETWORK */

// Concurrently add messages to this list.
// One thread per client for managing
struct host_messages
{
  int hostId;
  concurrent_linked_list messages;
  int nb_message_sent;
  int windows_size;
};

int connect(in_port_t port)
{
  // Create UDP socket
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0)
  {
    perror("socket creation failed");
    exit(EXIT_FAILURE);
  }

  // Allocate server address
  struct sockaddr_in server;
  memset(&server, 0, sizeof(server)); //TODO: Test without this line

  // Filling server information
  server.sin_family = AF_INET; // IPv4
  server.sin_addr.s_addr = INADDR_ANY;
  server.sin_port = htons(port);

  // Bind the socket with the server address
  if (bind(fd, reinterpret_cast<struct sockaddr *>(&server), sizeof(server)) < 0)
  {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }

  return fd;
}

static void *sender(void *p_data)
{
  // TODO: Receive those variables via parameters
  int sockfd;
  scmp_queue_t *queues;

  while (true)
  {
    // Sending part
    for (int i = 0; i < queues->nb_queues; ++i)
    {
      // Just send all packets
      scsp_queue_t *queue = queues[i];
      struct packet *msg;

      while (msg = pop(queue))
      {
        struct packet *packet = encode_message(msg);
        sendto(sockfd, (const char *)packet->buffer, strlen(packet->size),
               NULL, (const struct sockaddr *)&msg->addr,
               sizeof(msg->addr));
        free(packet->buffer);
      }
    }
  }
}

static void *listener(void *p_data)
{
  //TODO: Receive from p_data
  int sockfd = 0;
  scmp_queue_t *receiving_queues;

  // TODO: Choose what is the max size
  const size_t MAXLINE = 64;
  char buffer[MAXLINE];

  struct sockaddr_in cliaddr;
  socklen_t len = sizeof(cliaddr);

  while (true)
  {
    // WAIT_ALL wait for all bytes to arrive
    size_t n = recvfrom(sockfd, (char *)buffer, MAXLINE,
                        MSG_WAITALL, (struct sockaddr *)&cliaddr,
                        &len);
    if (n == 0)
    {
      //TODO: Socket closed
    }

    // Manage received message
    struct packet p;
    p->buffer = malloc(n);
    p->size = n;
    p->addr = cliaddr;
    strncpy(p->buffer, buffer, n);

    message *msg = decode_packet(&p);

    // Dispatch message to be treated
    push(receiving_queues->queues[msg->source_id - 1], msg);
  }
}

struct link_info
{
  pthread_t p_listener = NULL;
  pthread_t p_sender = NULL;
  int sock_id;
  scmp_queue_t receive;
  scmp_queue_t send;
};

// Base init
struct link_info *rbInit(Parser::Host const &host)
{
  struct link_info *link = malloc(sizeof(struct link_info));

  // Create receiver and sender thread
  int ret1 = pthread_create(&link->p_listener, NULL, listener, NULL);
  int ret2 = pthread_create(&link->p_sender, NULL, sender, NULL);

  /* Creation des threads des clients si celui du magasin a reussi. */
  if (!ret1 || !ret2)
  {
  }
}

static void *rbListener(void *p_data)
{
}

void rbStop()
{
  pthread_cancel(p_listener);
  p_listener = NULL;
}

void rlSend()
{
  // TODO: Implement a version of TCP
}

void rlDeliver()
{
  // TODO: Implement a version of TCP
}

void rbBroadcast(std::vector<Parser::Host> const &hosts, int const fd)
{
  struct sockaddr_in client;
  std::memset(&client, 0, sizeof(client));

  for (auto &host : hosts)
  {
    client.sin_family = AF_INET;
    client.sin_addr.s_addr = host.ip;
    client.sin_port = host.port;

    char data_buffer[500]; //TODO: Decide format of data
    size_t size = 123;

    if (sendto(fd, data_buffer, size, 0, (struct sockaddr *)&client, sizeof(client)))
    {
      // TODO: error while sending data
    }

    char dummy;
    if (recv(fd, &dummy, sizeof(dummy), 0) < 0)
    {
      // TODO: error while sending data
    }
  }

  close(fd);
}

// RB4. Agreement:For any message m, if a correct process delivers m, then every correct process delivers m

void ubBroadcast()
{
  //TODO: Nothing to do yet
}

void ubDeliver()
{
  //TODO: Nothing to do yet
}

void rbDeliver()
{
}

void fifoBroadcast()
{
}

void fifoDeliver()
{
}