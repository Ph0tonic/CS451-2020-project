#pragma once

#include <ppl.h>
#include <concurrent_vector.h>

#include "udp_socket.hpp"

class PerfectLink
{

private:
    volatile bool stop;
    uint8_t id;
    UdpSocket *socket;
    BlockingQueue socket_message;

    std::vector<struct message> messageToReceive;
    std::mutex mtxMessages;

    std::thread sender;
    std::thread listener;

public:
    PerfectLink(uint8_t pid, UdpSocket *socket)
    {
        this.id = pid;
        this->socket = socket;
        //TODO:
    }

    ~PerfectLink()
    {
        //TODO:
    }

    void receive(struct message *msg)
    {
        socket_message.push(msg);
    }

    void send(uint8_t originId, uint32_t messageId, uint8_t sourceId)
    {
        struct message *msg = new struct message();
        msg->sourceId = sourceId;
        msg->id = id;
        msg->messageId = messageId;
        msg->originId = originId;
        msg->ack = false;
        
        mtxMessages.lock();
        messageToReceive.push_back(msg);
        mtxMessages.unlock();
        socket->send(msg);
    }

    void stop()
    {
        stop = true;
        sender.join();
        listener.join();
    }

private:
    void launchThread()
    {
        // Listener
        auto listener = [](PerfectLink *link) {
            struct message *msg;
            while (!stop)
            {
                msg = socket_message.pop();
                if (stop)
                {
                    return; //Imediate stop
                }
                if (msg.ack)
                {
                    mtxMessages.lock();
                    messageToReceive.erase(
                        std::remove_if(messageToReceive.begin(), messageToReceive.end(), [&](struct message const &msg_it) {
                            return msg_it.sourceId == msg.sourceId &&
                                   msg_it.destinationId == msg.destinationId &&
                                   msg_it.originId == msg.originId &&
                                   msg_it.messageId == msg.messageId;
                        }),
                        pets.end());
                    messagesToReceive.remove(msg);
                    mtxMessages.unlock();
                }
                else
                {
                    msg.ack = true;
                    socket.send(msg);
                    broadcast.receive(msg.originId, msg.messageId, msg.sourceId);
                }
            }
        };

        // Sender
        auto sender = [](PerfectLink *link) {
            while (!stop)
            {
                sleep(0.2);
                if (stop)
                {
                    return;
                }
                for (std::vector<struct message>::iterator it = link->messageToReceive.begin(); it != link->messageToReceive.end(); ++it)
                {
                    //TODO: Check concurrence
                    link->socket->send(*it);
                }
            }
        };

        sender = std::thread(sender, this);
        listener = std::thread(listener, this);
    }
};