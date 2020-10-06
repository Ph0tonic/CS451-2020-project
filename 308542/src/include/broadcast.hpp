#pragma once
#include "parser.hpp"
#include <pthread.h>

int configureUdpSocket(Parser::Host const &server);

// TODO: In order
// Reliable link
// Uniform reliable broadcast
// FIFO Broadcast
// LCausal Broadcast

void broadcastToAll(std::vector<Parser::Host> const &hosts, int const fd);

/* Structure of message required -> minimized size
  packet_id
  source_id or instead reversed ip and port source ??? 
  message, is it present ?
*/
struct message {
  int packet_id;
  int source_id;
  size_t message_size;
  char* message;
};

static void * rbListener (void * p_data) {
  while(true) {
    // TODO: Listener
  }
}

pthread_t listener = NULL; 

// Base init
int rbInit(Parser::Host const &host) {
  struct sockaddr_in server;

  // Create UDP socket
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    throw std::runtime_error("Could not create the udp socket: " +
                             std::string(std::strerror(errno)));
  }

  server.sin_family = AF_INET;
  server.sin_addr.s_addr = host.ip;
  server.sin_port = host.port;
  if (bind(fd, reinterpret_cast<struct sockaddr *>(&server),
              sizeof(server)) < 0) {
    throw std::runtime_error("Could not bind the server to it's address: " +
                            std::string(std::strerror(errno)));
  }

  int ret = pthread_create (
    &listener, NULL,
    rbListener, NULL
  );
 
   /* Creation des threads des clients si celui du magasin a reussi. */
   if (! ret)
   {

   }
}

void rbStop() {
  pthread_cancel(listener);
  listener = NULL;
}

void rlSend() {
  // TODO: Implement a version of TCP  
}

void rlDeliver() {
  // TODO: Implement a version of TCP  
}

void rbBroadcast(std::vector<Parser::Host> const &hosts, int const fd) {
  struct sockaddr_in client;
  std::memset(&client, 0, sizeof(client));

  for (auto &host : hosts) {
    client.sin_family = AF_INET;
    client.sin_addr.s_addr = host.ip;
    client.sin_port = host.port;

    char data_buffer[500]; //TODO: Decide format of data
    size_t size = 123;

    if (sendto(fd, data_buffer, size, 0, (struct sockaddr *)&client, sizeof(client))) {
      // TODO: error while sending data
    }

    char dummy;
    if (recv(fd, &dummy, sizeof(dummy), 0) < 0) {
      // TODO: error while sending data
    }
  }

  close(fd);
}

// RB4. Agreement:For any message m, if a correct process delivers m, then every correct process delivers m


void ubBroadcast() {
  //TODO: Nothing to do yet
}

void ubDeliver() {
  //TODO: Nothing to do yet
}

void rbDeliver() {

}

void fifoBroadcast() {

}

void fifoDeliver() {

}