package cs451;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class FifoBroadcast {

    private final ReliableBroadcast reliableBroadcast;

    // originId -> messageId
    private ConcurrentMap<Integer, MessageTracking> received;
    private FifoReceive fifoReceive;

    public FifoBroadcast(int pid, int nbMessages, List<Host> hosts, FifoReceive fifoReceive) throws InterruptedException {
        try {
            reliableBroadcast = new ReliableBroadcast(pid, nbMessages, hosts, this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }
        received = new ConcurrentHashMap<>();
        hosts.forEach(h -> received.put(h.getId(), new MessageTracking()));
        this.fifoReceive = fifoReceive;
    }

    public synchronized void receive(int originId, int messageId) {
        System.out.println("FIFO RECEIVE " + originId + " " + messageId);
        MessageTracking tracking = received.get(originId);
        tracking.received.add(messageId);
        while (tracking.received.contains(tracking.nextId)) {
            this.fifoReceive.receive(originId, tracking.nextId);
            tracking.nextId++;
        }
    }

    public void broadcast(int messageId) {
        reliableBroadcast.broadcast(messageId);
    }

    public void stop() {
        reliableBroadcast.stop();
    }

    //TODO: Change into non-concurrent once ok
    private static class MessageTracking {
        public int nextId = 1;
        public ConcurrentSkipListSet<Integer> received = new ConcurrentSkipListSet<>();
    }
}