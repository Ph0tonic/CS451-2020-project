package cs451;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class FifoBroadcast implements ReliableBroadcastReceive {
    private final ReliableBroadcast reliableBroadcast;
    // TODO: Decide what to do about that
    private int windowsSize;
    private Semaphore windowsLatches;

    // originId -> messageId
    private MessageTracking[] received;
    private FifoReceive fifoReceive;
    private int id;

    private int messageId;

    public FifoBroadcast(int id, int nbMessages, List<Host> hosts, FifoReceive fifoReceive) throws InterruptedException {
        messageId = 1;
        int nbHosts = hosts.size();
        windowsSize = Math.max(50000 / (nbHosts * 1), 1);
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

        windowsLatches = new Semaphore(windowsSize);
    }

    @Override
    public void receive(int originId, int messageId, byte[] data) {
        MessageTracking tracking = received[originId - 1];
        tracking.received.putIfAbsent(messageId, data);

        if (originId == id) {
            windowsLatches.release();
        }

        data = tracking.received.remove(tracking.nextId);
        while (data != null) {
            fifoReceive.receive(data);
            tracking.nextId++;
            data = tracking.received.remove(tracking.nextId);
        }
    }

    public void broadcast(byte[] data) {
        try {
            windowsLatches.acquire();
            reliableBroadcast.broadcast(messageId, data);
            messageId++;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        reliableBroadcast.stop();
    }

    private static class MessageTracking {
        //TODO: Could be improved
        public int nextId = 1;
        public Map<Integer, byte[]> received = new HashMap<>();
    }
}
