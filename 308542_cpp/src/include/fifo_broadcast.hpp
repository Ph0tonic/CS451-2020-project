#pragma once

#include <fstream>
#include <set>
#include <vector>

#include "reliable_broadcast.hpp"
#include "parser.hpp"

class FifoBroadcast
{
private:
    ReliableBroadcast* rbBroadcast;
    volatile uint32_t nextID;


    // std::set<
public:
    FifoBroadcast(uint8_t pid, uint32_t nbMessages, std::vector<struct Host> hosts)
    {
        rbBroadcast = new ReliableBroadcast(pid, nbMessages, hosts, this);
        nextID = 0;
    }

    void receive(uint8_t originId, uint32_t mmessageId)
    {
        //TODO:
    }

    void broadcast(uint32_t messageId)
    {
        rbBroadcast.broadcast(messageId);
    }

    void stop() {
        rbBroadcast.stop();
    }
};