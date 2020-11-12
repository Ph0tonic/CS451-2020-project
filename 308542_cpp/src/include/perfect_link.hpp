#pragma once

#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>

#include "parser.hpp"
#include "udp_socket.hpp"

class PerfectLinkDeliver {
public:
    virtual void deliver(uint8_t originId, uint32_t messageId, uint8_t destinationId) = 0;
};

class PerfectLink : public UdpSocketDeliver {

private:
    PerfectLinkDeliver *receiver;
    volatile bool stopped;
    uint8_t id;
    UdpSocket *socket;
    BlockingQueue **socket_receive;

    // TODO: Change type
    std::vector<Message *> **messageToReceive;
    std::mutex mtxMessages;

    std::thread *sender;
    std::thread **listeners;
    int nbReceiver;
    int nbHost;
    int window_size;

public:
    PerfectLink(const Parser::Host host, std::vector <Parser::Host> hosts, PerfectLinkDeliver *receiver) :
            receiver(receiver), stopped(false) {
        int nbThread = 6; //TODO: Choose this value !!!

        nbHost = static_cast<int>(hosts.size());
        nbReceiver = std::max(std::min(nbThread - 1, nbHost), 1);
        window_size = 10000 / nbHost;

        //TODO: messageReceive type
        messageToReceive = new std::vector < Message * > *[nbHost];
        for (auto &h : hosts) {
            //todo: ConcurrentSkipList
            messageToReceive[h.id - 1] = new std::vector<Message *>();
        }

        // Init blocking queues
        socket_receive = new BlockingQueue *[nbReceiver];
        for (int i = 0; i < nbReceiver; ++i) {
            socket_receive[i] = new BlockingQueue();
        }

        this->socket = new UdpSocket(host, hosts, this);

        std::cout << "PL before launch thread" << std::endl;
        launchThread();
    }

    ~PerfectLink() {
        //TODO: destructeur
    }

    void deliver(Message *msg) override {
        std::cout << "Deliver at perfect link" << std::endl;
        socket_receive[msg->origin_id % nbReceiver]->push(msg);
    }

    void send(uint8_t originId, uint32_t messageId, uint8_t sourceId, uint8_t destinationId) {
        Message *msg = new Message();
        msg->source_id = sourceId;
        msg->message_id = messageId;
        msg->origin_id = originId;
        msg->destination_id = destinationId;
        msg->ack = false;

        mtxMessages.lock();
        messageToReceive[destinationId - 1]->push_back(msg);
        mtxMessages.unlock();
        socket->send(msg);
    }

    void stop() {
        stopped = true;
        sender->join();

        for (int i = 0; i < nbReceiver; ++i) {
            listeners[i]->join();
        }
    }

private:
    void launchThread() {
        // Listener
        auto listenerThread = [](PerfectLink *link, const int index) {
            try {
                std::cout << "PERFECT LINK listener : " << index << std::endl;
                Message *msg;
                while (!link->stopped) {
                    msg = link->socket_receive[index]->pop();
                    std::cout << "PL received : " << unsigned(msg->origin_id) << " "
                                << unsigned(msg->message_id) << " "
                                << unsigned(msg->source_id) << " " << unsigned(msg->destination_id) << " "
                                << msg->ack << " " << std::endl;

                    if (link->stopped) {
                        perror("Stop link listener");
                        return; //Immediate stop
                    }

                    if (msg->ack) {
                        std::cout << "PL received ack : " << unsigned(msg->origin_id) << " "
                                  << unsigned(msg->message_id) << " "
                                  << unsigned(msg->source_id) << " " << unsigned(msg->destination_id) << " "
                                  << msg->ack << " " << std::endl;
                        link->mtxMessages.lock();
                        std::cout << "PL received ack acquire lock : " << unsigned(msg->origin_id) << std::endl;
                        link->messageToReceive[index]->erase(
                                std::remove_if(link->messageToReceive[index]->begin(),
                                               link->messageToReceive[index]->end(),
                                               [&](Message *const &msg_it) {
                                                   return msg_it->source_id == msg->source_id &&
                                                          msg_it->destination_id == msg->destination_id &&
                                                          msg_it->origin_id == msg->origin_id &&
                                                          msg_it->message_id == msg->message_id;
                                               }),
                                link->messageToReceive[index]->end());
                        link->mtxMessages.unlock();
                        std::cout << "PL received ack second step: " << unsigned(msg->origin_id) << std::endl;
                        link->receiver->deliver(msg->origin_id, msg->message_id, msg->destination_id);
                        std::cout << "PL received ack end : " << unsigned(msg->origin_id) << std::endl;
                        delete msg;
                    } else {
                        std::cout << "PL send ack : " << std::endl;
                        msg->ack = true;
                        link->socket->send(msg);
                        link->receiver->deliver(msg->origin_id, msg->message_id, msg->source_id);
                    }
                }
            } catch (const std::exception &ex) {
                std::cerr << ex.what() << std::endl;
                perror("Exception - ");
            } catch (...) {
                std::cout << "Catch an exception in PL listener" << std::endl;
                perror("Exception - 2");
            }
        };

        // Sender
        auto senderThread = [](PerfectLink *link) {
            try {
                std::cout << "PERFECT LINK sender threads" << std::endl;
                while (!link->stopped) {
                    //TODO: Config sleep duration
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    if (link->stopped) {
                        perror("Stop link sender");
                        return;
                    }

                    for (int i = 0; i < link->nbHost; ++i) {
                        int nb = 0;
                        for (auto message : *link->messageToReceive[i]) {
                            nb++;
                            if (nb > link->window_size) {
                                break;
                            }
                            std::cout << "Resend message" << std::endl;
                            link->socket->send(message);
                        }
                    }
                }
            } catch (const std::exception &ex) {
                std::cerr << ex.what() << std::endl;
                perror("Exception");
            } catch (...) {
                std::cout << "Catch an exception in PL listener" << std::endl;
                perror("Exception2");
            }
        };

        sender = new std::thread(senderThread, this);
        listeners = new std::thread *[nbReceiver];
        for (int i = 0; i < nbReceiver; ++i) {
            const int j = i;
            listeners[i] = new std::thread(listenerThread, this, j);
        }
    }
};