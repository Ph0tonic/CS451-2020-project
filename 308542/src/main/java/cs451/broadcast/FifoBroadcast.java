package cs451.broadcast;

import cs451.config.Host;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * FifoBroadcast implementation
 *
 * @author Wermeille Bastien
 */
public class FifoBroadcast implements UniformReliableBroadcastReceive, Broadcast {
    private final UniformReliableBroadcast urbBroadcast;

    private final Semaphore windowsLatches;

    // originId -> messageId
    private final MessageTracking[] received;
    private final BroadcastReceive receiver;
    private final int id;

    private int messageId;

    public FifoBroadcast(int id, int nbMessages, List<Host> hosts, BroadcastReceive receiver) throws InterruptedException {
        messageId = 0;
        int nbHosts = hosts.size();
        int windowsSize = Math.max(50000 / (nbHosts * nbHosts), 1);
        try {
            urbBroadcast = new UniformReliableBroadcast(id, nbMessages, hosts, this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }

        received = new MessageTracking[nbHosts];
        hosts.forEach(h -> received[h.getId() - 1] = new MessageTracking());
        this.receiver = receiver;
        this.id = id;

        windowsLatches = new Semaphore(windowsSize);
    }

    @Override
    public void deliver(int originId, int messageId, byte[] data) {
        MessageTracking tracking = received[originId - 1];
        tracking.received.putIfAbsent(messageId, data);

        if (originId == id) {
            windowsLatches.release();
        }

        data = tracking.received.remove(tracking.nextId);
        while (data != null) {
            receiver.deliver(originId, data);
            tracking.nextId++;
            data = tracking.received.remove(tracking.nextId);
        }
    }

    @Override
    public void broadcast(byte[] data) {
        try {
            messageId++;
            windowsLatches.acquire();
            urbBroadcast.broadcast(messageId, data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        urbBroadcast.stop();
    }

    private static class MessageTracking {
        public int nextId = 1;
        public Map<Integer, byte[]> received = new HashMap<>();
    }
}
