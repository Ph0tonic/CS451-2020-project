#pragma once

#include <iostream>
#include <sstream>
#include <string>

#include "fifo_broadcast.hpp"

class FifoLogger : public FifoBroadcastDeliver {
private:
    Logger *logger;
    FifoBroadcastDeliver *receiver;

public:
    FifoLogger(Logger *logger, FifoBroadcastDeliver *receiver) : logger(logger), receiver(receiver) {}

    void deliver(uint8_t origin_id, uint32_t message_id) override {
        std::stringstream ss;
        ss << "d " << unsigned(origin_id) << " " << unsigned(message_id) << "\n";
        logger->log(ss.str());
        receiver->deliver(origin_id, message_id);
    }
};
