#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <string>
#include <sstream>
#include <signal.h>

#include "barrier.hpp"
#include "parser.hpp"
#include "logger.hpp"
#include "fifo_logger.hpp"
#include "fifo_broadcast.hpp"
#include "fifo_counter_latch.hpp"

static FifoBroadcast *broadcast = nullptr;
static Logger *logger = nullptr;

static void stop(int) {
    // reset signal handlers to default
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    // immediately stop network packet processing
    std::cout << "Immediately stopping network packet processing.\n";
    if (broadcast != nullptr) {
        broadcast->stop();
    }

    // write/flush output file if necessary
    std::cout << "Writing output.\n";
    if (logger != nullptr) {
        logger->dump();
    }

    // exit directly from signal handler
    exit(0);
}

int main(int argc, char **argv) {
    signal(SIGTERM, stop);
    signal(SIGINT, stop);

    // `true` means that a config file is required.
    // Call with `false` if no config file is necessary.
    bool requireConfig = true;

    Parser parser(argc, argv, requireConfig);
    parser.parse();

    std::cout << std::endl;

    std::cout << "My PID: " << getpid() << "\n";
    std::cout << "Use `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
              << getpid() << "` to stop processing packets\n\n";

    std::cout << "My ID: " << parser.id() << "\n\n";

    std::cout << "Path to hosts:\n";
    std::cout << "==============\n";
    std::cout << parser.hostsPath() << "\n\n";

    std::cout << "List of resolved hosts is:\n";
    std::cout << "==========================\n";
    auto hosts = parser.hosts();
    for (auto &host : hosts) {
        std::cout << host.id << "\n";
        std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
        std::cout << "Machine-readable IP: " << host.ip << "\n";
        std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
        std::cout << "Machine-readbale Port: " << host.port << "\n";
        std::cout << "\n";
    }
    std::cout << "\n";

    std::cout << "Barrier:\n";
    std::cout << "========\n";
    auto barrier = parser.barrier();
    std::cout << "Human-readable IP: " << barrier.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << barrier.ip << "\n";
    std::cout << "Human-readbale Port: " << barrier.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << barrier.port << "\n";
    std::cout << "\n";

    std::cout << "Signal:\n";
    std::cout << "========\n";
    auto signal = parser.signal();
    std::cout << "Human-readable IP: " << signal.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << signal.ip << "\n";
    std::cout << "Human-readbale Port: " << signal.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << signal.port << "\n";
    std::cout << "\n";

    std::cout << "Path to output:\n";
    std::cout << "===============\n";
    std::cout << parser.outputPath() << "\n\n";

    if (requireConfig) {
        std::cout << "Path to config:\n";
        std::cout << "===============\n";
        std::cout << parser.configPath() << "\n\n";
    }

    std::cout << "Doing some initialization...\n\n";
    std::ifstream file(parser.configPath());
    std::string str;
    int nbMessage = 0;

    while (std::getline(file, str)) {
        std::stringstream intValue(str);
        intValue >> nbMessage;
        break;
    }
    if (nbMessage == 0) {
        perror("Error while reading config file");
        return -1;
    }
    std::cout << "Nb messages : " << nbMessage << std::endl;

    logger = new Logger(parser.outputPath());
    auto counter = new FifoCounterLatch(parser.id() & 0xff, nbMessage);
    broadcast = new FifoBroadcast(parser.id() & 0xff, nbMessage, parser.hosts(), new FifoLogger(logger, counter));

    Coordinator coordinator(parser.id(), barrier, signal);

    std::cout << "Waiting for all processes to finish initialization\n\n";
    coordinator.waitOnBarrier();

    std::cout << "Broadcasting messages...\n\n";

    //TODO: Broadcast messages
    std::stringstream ss;

    for (int i = 1; i <= nbMessage; ++i) {
        broadcast->broadcast(i);
        ss << "b " << i << "\n";
        logger->log(ss.str());
        ss.str("");
        ss.clear();
    }

    counter->wait();

    std::cout << "Signaling end of broadcasting messages\n\n";
    coordinator.finishedBroadcasting();
    logger->dump();

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }

    return 0;
}
