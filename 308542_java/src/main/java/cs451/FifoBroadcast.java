package cs451;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FifoBroadcast {

    private final ReliableBroadcast reliableBroadcast;

    //originId -> messageId
    private ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>> received;
    private volatile int nextID;

    private FifoReceive fifoReceive;

    public FifoBroadcast(int pid, int nbMessages, List<Host> hosts, FifoReceive fifoReceive) throws InterruptedException {
        try {
            reliableBroadcast = new ReliableBroadcast(pid, nbMessages, hosts, this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }
        received = new ConcurrentHashMap<>();
        hosts.forEach(h -> received.put(h.getId(), new ConcurrentSkipListSet<>()));
        nextID = 1;
        this.fifoReceive = fifoReceive;
    }

    public synchronized void receive(int originId, int messageId) {
        ConcurrentSkipListSet<Integer> set = received.get(originId);
        boolean found = nextID == originId;
        do {
            if (found) {
                this.fifoReceive.receive(originId, messageId);
            } else {
                nextID++;
                found = set.contains(nextID);
            }
        } while (found);
    }

    public void broadcast(int messageId) {
        reliableBroadcast.broadcast(messageId);
    }

    public void stop() {
        reliableBroadcast.stop();
    }
}
