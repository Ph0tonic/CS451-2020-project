package cs451.broadcast;

import cs451.config.Host;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CausalOrderBroadcast implements UniformReliableBroadcastReceive, Broadcast {

    private final int id;
    private final int[] nbCausal;
    private final int[] vectorClock;
    private final int[][] causals;
    private final List<PendingMessage> pending;

    private final UniformReliableBroadcast broadcast;
    private final BroadcastReceive receiver;
    
    private final BlockingQueue<PendingMessage> received;
    private volatile boolean stopped;
    private Thread listener;

    public CausalOrderBroadcast(int id, int nbMessages, List<Host> hosts, BroadcastReceive receiver, int[][] causals) throws Exception {
        this.receiver = receiver;
        this.causals = causals;
        this.id = id;
        stopped = false;

        received = new LinkedBlockingQueue<>();

        nbCausal = new int[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            nbCausal[i] = causals[i].length;
        }

        vectorClock = new int[hosts.size()];
        Arrays.fill(vectorClock, 1);

        pending = new LinkedList<>();
        broadcast = new UniformReliableBroadcast(id, nbMessages, hosts, this);

        startThread();
    }

    private void startThread() {
        listener = new Thread(() -> {
            while (!stopped) {
                try {
                    var message = received.take();

                    if (canDeliver(message)) {
                        receiver.deliver(message.originId, message.data);
                        vectorClock[message.originId - 1]++;

                        boolean removed;
                        do {
                            removed = false;
                            Iterator<PendingMessage> it = pending.iterator();
                            while (it.hasNext()) {
                                PendingMessage m = it.next();
                                if (canDeliver(m)) {
                                    receiver.deliver(m.originId, m.data);
                                    it.remove();
                                    removed = true;
                                }
                            }
                        } while (removed);

                    } else {
                        pending.add(message);
                    }
                } catch (InterruptedException e) {
                    // Stop listener
                    System.out.println("Causal listener stopped");
                    return;
                }
            }
        });
        listener.start();
    }

    @Override
    public void deliver(int originId, int messageId, byte[] data) {
        if (originId == id) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data, 0, data.length);
        var payload = Arrays.copyOfRange(buffer.array(), nbCausal[originId - 1], data.length);

        int[] vc = new int[nbCausal[originId - 1]];
        for (int i = 0; i < nbCausal[originId - 1]; ++i) {
            vc[i] = buffer.getInt();
        }
        received.add(new PendingMessage(originId, messageId, vc, payload));

    }

    private boolean canDeliver(PendingMessage message) {
        // Fifo check fo delivery
        if (message.messageId != vectorClock[message.originId - 1]) {
            return false;
        }

        // Check that each causality is ok
        int j = 0;
        for (int i : causals[message.originId - 1]) {
            if (message.vectorClock[j] < vectorClock[i - 1]) {
                return false;
            }
            j++;
        }
        return true;
    }

    @Override
    public void broadcast(byte[] data) {
        int messageId = vectorClock[id - 1];
        receiver.deliver(id, data);

        byte[] newData = new byte[nbCausal[id - 1] * 4 + data.length];
        ByteBuffer buffer = ByteBuffer.wrap(newData, 0, newData.length);
        for (int i : causals[id - 1]) {
            buffer.putInt(vectorClock[i]);
        }
        buffer.put(data);

        broadcast.broadcast(messageId, buffer.array());
        vectorClock[id - 1]++;
    }

    @Override
    public void stop() {
        stopped = true;
        pending.add(new PendingMessage(0, 0, null, null));
        broadcast.stop();

        try {
            listener.join();
        } catch (InterruptedException e) {
            System.out.println("Listener already closed");
        }
    }

    private static final class PendingMessage {
        public int originId;
        public int messageId;
        public int[] vectorClock;
        public byte[] data;

        public PendingMessage(int originId, int messageId, int[] vectorClock, byte[] data) {
            this.vectorClock = vectorClock;
            this.messageId = messageId;
            this.data = data;
            this.originId = originId;
        }
    }
}
