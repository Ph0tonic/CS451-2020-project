#pragma once

#include <fstream>
#include <set>
#include <vector>

#include "reliable_broadcast.hpp"
#include "parser.hpp"

class FifoBroadcastDeliver {
public:
    virtual void deliver(uint8_t origin_id, uint32_t message_id) = 0;
};

class FifoBroadcast : public ReliableBroadcastDeliver {

private:
    struct MessageTracking {
        uint32_t nextId;
        std::set <uint32_t> *received;
    };

    FifoBroadcastDeliver *receiver;
    ReliableBroadcast *rbBroadcast;
    MessageTracking **received;

public:
    FifoBroadcast(uint8_t id, uint32_t nbMessages, std::vector <Parser::Host> hosts, FifoBroadcastDeliver *receiver) :
            receiver(receiver) {
        int nbHost = static_cast<int>(hosts.size());
        received = new MessageTracking *[nbHost];
        for (int i = 0; i < nbHost; ++i) {
            received[i] = new MessageTracking();
            received[i]->nextId = 1;
            received[i]->received = new std::set<uint32_t>();
        }

        rbBroadcast = new ReliableBroadcast(id, nbMessages, hosts, this);
    }

    ~FifoBroadcast() {
        //TODO: Destructeur
    }

    void deliver(uint8_t origin_id, uint32_t message_id) override {
        MessageTracking *tracking = received[origin_id - 1];
        std::cout << "Deliver at fifo broadcast " << unsigned(origin_id) << " messageId " << unsigned(message_id)
                  << " nextId " << unsigned(tracking->nextId) << std::endl;
//        if (originId == id) {
//            windowsLatches.release();
//        }

        if (tracking->nextId != message_id) {
            std::cout << "FIFO wait for next deliver" << std::endl;
            tracking->received->insert(message_id);
            std::cout << "FIFO YEP" << std::endl;
        } else {
            std::cout << "FIFO Delivered pre" << std::endl;
            std::cout << "d " << unsigned(origin_id) << " " << unsigned(tracking->nextId) << std::endl;
            receiver->deliver(origin_id, tracking->nextId);
            std::cout << "FIFO Delivered after" << std::endl;
            tracking->nextId++;

            auto size = tracking->received->size();
            tracking->received->erase(tracking->nextId);

            while (size != tracking->received->size()) {
                std::cout << "d " << unsigned(origin_id) << " " << unsigned(tracking->nextId) << std::endl;
                receiver->deliver(origin_id, tracking->nextId);
                tracking->nextId++;

                size = tracking->received->size();
                tracking->received->erase(tracking->nextId);
            }
        }
    }

    void broadcast(uint32_t message_id) {
        rbBroadcast->broadcast(message_id);
    }

    void stop() {
        rbBroadcast->stop();
    }
};