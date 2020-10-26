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

#define BUFFER_SIZE sizeof(int8_t) * 4 + sizeof(uint32_t)

struct message
{
    uint32_t message_id;
    uint8_t source_id;
    uint8_t destination_id;
    uint8_t origin_id;
    bool ack;
};

struct packet
{
    char buffer[BUFFER_SIZE];
};

struct packet *encode_message(message *m)
{
    packet *p = (packet *)malloc(sizeof(packet));
    uint8_t ack = m->ack;
    memcpy(p->buffer, &ack, sizeof(uint8_t));
    memcpy(p->buffer + sizeof(uint8_t), &m->source_id, sizeof(uint8_t));
    memcpy(p->buffer + 2 * sizeof(uint8_t), &m->destination_id, sizeof(uint8_t));
    memcpy(p->buffer + 3 * sizeof(uint8_t), &m->origin_id, sizeof(uint8_t));
    memcpy(p->buffer + 4 * sizeof(uint8_t), &m->message_id, sizeof(uint32_t));
    return p;
}

struct message *decode_packet(packet *p)
{
    message *m = (message *)malloc(sizeof(message));
    uint8_t ack = 0;
    memcpy(&ack, p->buffer, sizeof(uint8_t));
    memcpy(&m->source_id, p->buffer + sizeof(uint8_t), sizeof(uint8_t));
    memcpy(&m->destination_id, p->buffer + 2 * sizeof(uint8_t), sizeof(uint8_t));
    memcpy(&m->origin_id, p->buffer + 3 * sizeof(uint8_t), sizeof(uint8_t));
    memcpy(&m->message_id, p->buffer + 4 * sizeof(uint8_t), sizeof(uint32_t));
    m->ack = ack;
    return m;
}

class UdpSocket
{
private:
    BlockingQueue sendingQueue;

    std::map<int, PerfectLink*> links;
    std::map<uint8_t, struct sockaddr_in> hosts;

    int sockfd;
    struct sockaddr_in server;
    size_t lenServer;

public:
    UdpSocket(struct Host host, std::vector<struct Host>)
    {
        createSocket(host.ip, host.port);
        startThreads();
    }

    ~UdpSocket()
    {
    }

    void send(struct message *msg)
    {
        //TODO: Send message
        sendingQueue.push(msg);
    }

private:
    void startThreads()
    {
        // Sender
        auto sender = [](UdpSocket *socket) {
            // TODO: Choose what is the max size
            char buffer[BUFFER_SIZE];

            sockaddr_in cliaddr;
            socklen_t len = sizeof(cliaddr);

            // Do Something
            while (true)
            {
            }
        };

        // Listener
        auto listener = [](UdpSocket *socket) {
            // TODO: Choose what is the max size
            struct packet packet;

            while (true)
            {
                // WAIT_ALL wait for all bytes to arrive
                size_t n = recvfrom(socket->sockfd, packet.buffer, BUFFER_SIZE,
                                    MSG_WAITALL, (struct sockaddr *)&socket->server,
                                    (socklen_t *)&socket->lenServer);
                if (n == 0)
                {
                    //TODO: Socket closed
                }

                // Decode message
                struct message* msg = decode_packet(&packet);

                socket->links[msg->ack ? msg->source_id : msg->destination_id]->receive(msg);
            }
        };

        std::thread thread_sender(sender, this);
        std::thread thread_listener(listener, this);
    }

    void createSocket(in_addr_t address, short port)
    {
        // Create UDP socket
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0)
        {
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
        if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&server), lenServer) < 0)
        {
            perror("bind failed");
            exit(EXIT_FAILURE);
        }
    }
};