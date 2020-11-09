package cs451;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;

public class FifoBroadcast {
    // TODO: Decide what to do about that
    private static final int WINDOWS_SIZE = 250;

    private final ReliableBroadcast reliableBroadcast;
    // private Semaphore windowsLatches;

    // originId -> messageId
    private MessageTracking[] received;
    private FifoReceive fifoReceive;
    private int id;

    public FifoBroadcast(int id, int nbMessages, List<Host> hosts, FifoReceive fifoReceive) throws InterruptedException {
        int nbHosts = hosts.size();
        try {
            reliableBroadcast = new ReliableBroadcast(id, nbMessages, hosts, this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }

        received = new MessageTracking[nbHosts];
        hosts.forEach(h -> received[h.getId() - 1] = new MessageTracking());
        this.fifoReceive = fifoReceive;
        this.id = id;

        // windowsLatches = new Semaphore(WINDOWS_SIZE);
    }

    public void receive(int originId, int messageId) {
        MessageTracking tracking = received[originId - 1];
        tracking.received.add(messageId);
        // System.out.println(Thread.currentThread().getName() + " FIFO RECEIVE " + originId + " " + messageId + " -> " + tracking.nextId);
        while (tracking.received.remove(tracking.nextId)) {
            // System.out.println("FIFO REMOVE " + originId + " " + tracking.nextId);
            fifoReceive.receive(originId, tracking.nextId);
            tracking.nextId++;
            if (originId == id) {
//                windowsLatches.release();
            }
        }
    }

    public void broadcast(int messageId) {
//        try {
//            windowsLatches.acquire();
        reliableBroadcast.broadcast(messageId);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public void stop() {
        reliableBroadcast.stop();
    }

    private static class MessageTracking {
        //TODO: Could be improved
        public int nextId = 1;
        public Set<Integer> received = new HashSet<>();
    }
}
