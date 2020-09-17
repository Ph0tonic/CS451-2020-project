#pragma once
#include "parser.hpp"

int configureUdpSocket(Parser::Host const &server);

void broadcastToAll(std::vector<Parser::Host> const &hosts, int const fd);

/* Structure of message required -> minimized size
  packet_id
  source_id or instead reversed ip and port source ??? 
  message, is it present ?
*/
struct message {
  int packet_id;
  int source_id;
  int message;
};

int configureUdpSocket(Parser::Host const &host) {
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
}

void broadcastToAll(std::vector<Parser::Host> const &hosts, int const fd) {
  struct sockaddr_in server;
  std::memset(&server, 0, sizeof(server));

  for (auto &host : hosts) {
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = host.ip;
    server.sin_port = host.port;
    if (connect(fd, reinterpret_cast<struct sockaddr *>(&server),
                sizeof(server)) < 0) {
      throw std::runtime_error("Could not connect to the barrier: " +
                              std::string(std::strerror(errno)));
    }

    char dummy;
    if (recv(fd, &dummy, sizeof(dummy), 0) < 0) {
      throw std::runtime_error("Could not read from the barrier socket: " +
                              std::string(std::strerror(errno)));
    }
  }

  close(fd);
}