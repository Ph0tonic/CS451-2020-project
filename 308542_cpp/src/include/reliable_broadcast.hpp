#pragma once

#include <fstream>
#include <set>
#include <vector>
#include <map>

#include "fifo_broadcast.hpp"
#include "perfect_link.hpp"
#include "udp_socket.hpp"
#include "parser.hpp"

class ReliableBroadcastDeliver {
public:
    virtual void deliver(uint8_t originId, uint32_t messageId) = 0;
};

class ReliableBroadcast : public PerfectLinkDeliver {
private:
    uint8_t id;
    ReliableBroadcastDeliver *receiver;

    int nbHost;
    UdpSocket *socket;

    PerfectLink *link;
    // originId -> messageId -> sourceId
    std::set <uint8_t> ***linkDelivered;

public:
    ReliableBroadcast(uint8_t id, uint32_t nbMessage, std::vector <Parser::Host> hosts,
                      ReliableBroadcastDeliver *receiver) : id(id), receiver(receiver) {
        nbHost = static_cast<int>(hosts.size());

        const Parser::Host host;

        bool found = false;
        for (auto const &h : hosts) {
            if (h.id == id) {
                found = true;
                link = new PerfectLink(h, hosts, this);
                break;
            }
        }
        if (!found) {
            std::cout << "Error while searching for host";
            return;
        }

        linkDelivered = new std::set <uint8_t> **[nbHost];
        for (int i = 0; i < nbHost; ++i) {
            linkDelivered[i] = new std::set <uint8_t> *[nbMessage];
            for (uint32_t j = 0; j < nbMessage; ++j) {
                linkDelivered[i][j] = new std::set<uint8_t>();
            }
        }
    }

    void deliver(uint8_t originId, uint32_t messageId, uint8_t sourceId) override {
        std::cout << "Deliver at reliable broadcast" << std::endl;
        std::set <uint8_t> *ids = linkDelivered[originId - 1][messageId - 1];

        int size = static_cast<int>(ids->size());
        ids->insert(sourceId);
        int newSize = static_cast<int>(ids->size());
        if (newSize == size) {
            return;
        }

        if (newSize == 1 && originId != id) {
            for (uint8_t i = 1; i <= nbHost; i++) {
                if (i != id && ids->find(i) == ids->end()) {
                    std::cout << "reliable resend to " << unsigned(sourceId) << " source from "
                              << unsigned(sourceId) << " origin " << unsigned(originId) << std::endl;
                    link->send(originId, messageId, sourceId, i);
                }
            }
        }

        if (isReadyToDeliver(newSize)) {
            // Deliver this message
            std::cout << "RB deliver" << std::endl;
            receiver->deliver(originId, messageId);
        }
    }

    void broadcast(uint32_t messageId) {
        for (uint8_t i = 1; i <= nbHost; i++) {
            if (i != id) {
                link->send(id, messageId, id, i);
            }
        }
    }

    void stop() {
        socket->stop();
        link->stop();
    }

private:
    bool isReadyToDeliver(int nb) {
        //TODO: Check if the +1 to take into account this process is correct
        return nb >= nbHost / 2. && nb - 1 < nbHost / 2.;
    }
};