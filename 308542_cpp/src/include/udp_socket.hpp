#pragma once

#include <thread>
#include <endian.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <map>
#include <vector>
#include <cstring>

#include "parser.hpp"
#include "blocking_queue.hpp"
#include "udp_socket.hpp"
#include "message.hpp"

#define BUFFER_SIZE sizeof(int8_t) * 4 + sizeof(uint32_t)

struct Packet {
    char buffer[BUFFER_SIZE];
};

class UdpSocketDeliver {
public:
    virtual void deliver(Message *msg) = 0;
};

class UdpSocket {
private:
    BlockingQueue *sendingQueue;

    UdpSocketDeliver *receiver;
    struct sockaddr_in *hosts;

    int sockfd;
    struct sockaddr_in server;
    struct sockaddr_in *addresses;

    socklen_t lenServer;
    volatile bool stopped;

    std::thread *thread_sender;
    std::thread *thread_listener;

public:
    UdpSocket(const Parser::Host host, std::vector <Parser::Host> hosts, UdpSocketDeliver *receiver) :
            receiver(receiver) {
        createSocket(host.ip, host.port);
        stopped = false;
        sendingQueue = new BlockingQueue();

        int nbHost = static_cast<int>(hosts.size());
        addresses = new struct sockaddr_in[nbHost];
        for (auto const &h: hosts) {
            uint8_t i = h.id & 0xff;
            std::cout << "Init address for " << i - 1 << std::endl;
            addresses[i - 1].sin_family = AF_INET;
            addresses[i - 1].sin_addr.s_addr = h.ip;
            addresses[i - 1].sin_port = htons(h.port);
        }

        startThreads();
    }

    ~UdpSocket() {
        delete sendingQueue;
        thread_sender->join();
        thread_listener->join();
        delete thread_sender;
        delete thread_listener;
    }

    void send(Message *msg) {
        sendingQueue->push(msg);
    }

    void stop() {
        stopped = true;
    }

private:
    void startThreads() {
        // Sender
        auto sender = [](UdpSocket *socket) noexcept {

            // TODO: Choose what is the max size
            char buffer[BUFFER_SIZE];

            // Do Something
            while (!socket->stopped) {
                Message *message = socket->sendingQueue->pop();

                if (socket->stopped) {
                    perror("Stop udp socket sender");
                    return;
                }

                uint8_t destinationId = message->ack ? message->source_id : message->destination_id;

                Packet *packet = socket->encode_message(message);
                std::cout << "Message to send : " << unsigned(message->origin_id) << " "
                          << unsigned(message->message_id) << " "
                          << unsigned(message->source_id) << " " << unsigned(message->destination_id) << " "
                          << message->ack << " " << std::endl;

                std::cout << "Send message to " << socket->addresses[destinationId - 1].sin_port << std::endl;
                long int n = sendto(socket->sockfd, packet->buffer, BUFFER_SIZE, 0,
                                    reinterpret_cast<struct sockaddr *>(&socket->addresses[destinationId - 1]),
                                    sizeof(socket->addresses[destinationId - 1]));

                if (n < 0) {
                    std::cout << "ERROR Sending message " << n << std::endl;
                    close(socket->sockfd);
                    perror("Cannot send message but why ???");
                    return;
                }
                delete packet;
            }
        };

        // Listener
        auto listener = [](UdpSocket *socket) noexcept {
            std::cout << "UDP SOCKET LISTENER" << std::endl;

            // TODO: Choose what is the max size
            Packet *packet = new Packet();

            while (!socket->stopped) {
                // WAIT_ALL wait for all bytes to arrive
                size_t n = recvfrom(socket->sockfd, packet->buffer, BUFFER_SIZE,
                                    MSG_WAITALL, reinterpret_cast<struct sockaddr *>(&socket->server),
                                    &socket->lenServer);

                if (n == 0 || socket->stopped) {
                    perror("Stop udp socket listener error ?");
                    return; // Socket closed
                }

                // Decode message
                Message *message = socket->decode_packet(packet);
                std::cout << "Message received : " << unsigned(message->origin_id) << " "
                          << unsigned(message->message_id) << " "
                          << unsigned(message->source_id) << " " << unsigned(message->destination_id) << " "
                          << message->ack << " " << std::endl;

                socket->receiver->deliver(message);
            }
            perror("Stop udp socket listener ?");
        };

        std::cout << "Create UDP threads" << std::endl;
        thread_sender = new std::thread(sender, this);
        thread_listener = new std::thread(listener, this);
        std::cout << "Done creating UDP threads" << std::endl;
    }

    void createSocket(in_addr_t address, short port) {
        // Create UDP socket
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) {
            perror("socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Allocate server address
        memset(&server, 0, sizeof(server)); //TODO: Test without this line

        // Filling server information
        server.sin_addr.s_addr = address;
        server.sin_family = AF_INET; // IPv4
        server.sin_port = htons(port);
        lenServer = sizeof(server);

        // Bind the socket with the server address
        if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&server), lenServer) < 0) {
            perror("bind failed");
            exit(EXIT_FAILURE);
        }
    }

    Packet *encode_message(Message *m) {
        Packet *p = new Packet();
        uint8_t ack = m->ack ? 1 : 0;
        memcpy(p->buffer, &ack, sizeof(uint8_t));
        memcpy(p->buffer + sizeof(uint8_t), &m->source_id, sizeof(uint8_t));
        memcpy(p->buffer + 2 * sizeof(uint8_t), &m->destination_id, sizeof(uint8_t));
        memcpy(p->buffer + 3 * sizeof(uint8_t), &m->origin_id, sizeof(uint8_t));
        memcpy(p->buffer + 4 * sizeof(uint8_t), &m->message_id, sizeof(uint32_t));
        return p;
    }

    Message *decode_packet(Packet *p) {
        Message *m = new Message();
        uint8_t ack = 0;
        memcpy(&ack, p->buffer, sizeof(uint8_t));
        memcpy(&m->source_id, p->buffer + sizeof(uint8_t), sizeof(uint8_t));
        memcpy(&m->destination_id, p->buffer + 2 * sizeof(uint8_t), sizeof(uint8_t));
        memcpy(&m->origin_id, p->buffer + 3 * sizeof(uint8_t), sizeof(uint8_t));
        memcpy(&m->message_id, p->buffer + 4 * sizeof(uint8_t), sizeof(uint32_t));
        m->ack = ack == 1;
        return m;
    }
};