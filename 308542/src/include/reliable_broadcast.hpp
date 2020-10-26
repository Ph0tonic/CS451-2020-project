#pragma once

#include <fstream>
#include <set>
#include <vector>
#include <map>

#include "fifo_broadcast.hpp"
#include "perfect_link.hpp"
#include "udp_socket.hpp"
#include "parser.hpp"

class ReliableBroadcast
{
private:
    UdpSocket *socket;
    uint8_t pid;

    uint8_t nbHosts;

    std::map<uint8_t, PerfectLink *> links;
    std::map<uint8_t, std::map<uint32_t, std::set<uint8_t>>> linkDelivered;

    ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>>> linkDelivered; // originId -> messageId -> sourceId
    FifoBroadcast broadcast;

public:
    ReliableBroadcast(uint8_t pid, uin32_t nbMessage, std::vector<struct Host> hosts, FifoBroadcast *broadcast)
    {
        this.socket = new UdpSocket() this.pid = pid;
        this.nbHosts = hosts.size();

        struct *host host = nullptr;
        for (auto const &host : hosts)
        {
            if (host.id == pid) //TODO: Check conversion
            {
                host = &host;
            }
            links.insert(host.id, new PerfectLink())
        }
    }

    void receive(uint8_t originId, uint32_t messageId, uint8_t sourceId)
    {
        auto map = links.at(originId);
        auto set = map.get(messageId)
                       set.push_back(sourceId);

        if (isReadyToDeliver(set.size()))
        {
            // Deliver this message
            rbBroadcast.receive(originId, messageId);
        }
        else
        {
            for (auto const &[key, link] : links)
            {
                if(key != pid && !map.contains(key)){
                    link.send(originId, messageId, sourceId);
                }
            }
        }
    }

    void broadcast(uint32_t messageId)
    {
        for (auto const &[key, link] : links)
        {
            link->stop();
        }
    }

    void stop()
    {
        socket.stop();
        for (auto const &[key, link] : links)
        {
            link->stop();
        }
    }

private:
    bool isReadyToDeliver(int nb)
    {
        //TODO: Check if the +1 to take into account this process is correct
        return nb + 1 > nbHosts / 2;
    }
};