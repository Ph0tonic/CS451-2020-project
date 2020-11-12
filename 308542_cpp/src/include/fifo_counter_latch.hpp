#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

#include "fifo_broadcast.hpp"

class FifoCounterLatch : public FifoBroadcastDeliver {
private:
    int id;
    int nbMessage;

    volatile int counter;
    std::mutex m;
    std::condition_variable cv;

public:
    FifoCounterLatch(uint8_t id, uint32_t nbMessage) : id(id), nbMessage(nbMessage), counter(0) {}

    void deliver(uint8_t origin_id, uint32_t message_id) override {
        if (origin_id == id) {
            std::unique_lock <std::mutex> lock(m);
            counter++;
            if (counter == nbMessage) {
                cv.notify_all();
            }
        }
    }

    void wait() {
        // Wait until main() sends data
        std::unique_lock <std::mutex> lock(m);
        while (counter != nbMessage) cv.wait(lock);
        std::cout << "latch done" << std::endl;
    }
};
