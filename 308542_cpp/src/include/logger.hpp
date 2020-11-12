#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <mutex>

class Logger {
private:
    std::vector <std::string> logs;
    std::mutex m;

    std::string logFile;

public:
    Logger(std::string logFile) : logFile(logFile) {}

    void log(std::string log) {
        m.lock();
        logs.push_back(log);
        m.unlock();
    }

    void dump() {
        std::cout << "Dumping logs " << std::endl;
        m.lock();
        std::ofstream log;
        std::cout << "Dumping logs open" << std::endl;
        log.open(logFile, std::ofstream::out | std::ofstream::trunc);
        std::cout << "Dumping logs opened" << std::endl;
        if (log.is_open()) {
            std::cout << "Dumping logs " << std::endl;
            for (auto const &l: logs) {
                log << l;
            }
            log.close();
        }
        std::cout << "Dumping logs done" << std::endl;
        m.unlock();
    }
};
