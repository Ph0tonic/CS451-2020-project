#pragma once

// THIS CODE IS AN UPDATED VERSION OF :
// https://stackoverflow.com/questions/12805041/c-equivalent-to-javas-blockingqueue

#include <mutex>
#include <condition_variable>
#include <deque>

#include "message.hpp"

class BlockingQueue
{
private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::deque<struct message*> d_queue;

public:
    void push(struct message *value)
    {
        {
            std::unique_lock<std::mutex> lock(this->d_mutex);
            d_queue.push_front(value);
        }
        this->d_condition.notify_one();
    }

    struct message *pop()
    {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        struct message* rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();
        return rc;
    }
};