package cs451;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

class PerfectLinks {

    private final LinkedBlockingQueue<Message>[] socketReceive;

    private final ConcurrentSkipListSet<Message>[] messagesToReceives;
    public int nbReceivers;
    public int nbHosts;

    private UdpSocket socket;
    private ReliableBroadcast broadcast;
    private volatile boolean stop = false;

    public PerfectLinks(List<Host> hosts, int nbThread) {
        nbReceivers = nbThread - 1;
        nbHosts = hosts.size();

        messagesToReceives = new ConcurrentSkipListSet[nbHosts];
        hosts.forEach(h -> messagesToReceives[h.getId() - 1] = new ConcurrentSkipListSet<>());

        socketReceive = new LinkedBlockingQueue[nbReceivers];
        for (int i = 0; i < nbReceivers; ++i) {
            socketReceive[i] = new LinkedBlockingQueue<>();
        }

        startReceivers();
        startRetransmitter();
    }

    private void startReceivers() {
        for (int i = 0; i < this.nbReceivers; ++i) {
            final int j = i;
            new Thread(() -> {
                while (!stop) {
                    try {
                        Message message = socketReceive[j].take();
                        if (stop) {
                            return;
                        }
                        if (message.ack) {
                            messagesToReceives[message.destinationId-1].remove(message);
                            broadcast.receive(message.originId, message.messageId, message.destinationId);
                        } else {
                            message.ack = true;
                            socket.send(message);
                            broadcast.receive(message.originId, message.messageId, message.sourceId);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    private void startRetransmitter() {
        new Thread(() -> {
            while (!stop) {
                try {
                    Thread.sleep(500);
                    if (stop) {
                        return;
                    }
                    // TODO: Think about how to resend with the less impact -> maybe only one thread
                    for (int i = 0; i < this.nbHosts; ++i) {
                        messagesToReceives[i].stream().forEach(socket::send);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void init(UdpSocket socket, ReliableBroadcast broadcast) {
        this.socket = socket;
        this.broadcast = broadcast;
    }

    public void receive(Message message) {
        socketReceive[(message.ack ? message.destinationId : message.sourceId) % nbReceivers].add(message);
    }

    public void send(int originId, int messageId, int sourceId, int destinationId) {
        Message message = new Message(originId, messageId, sourceId, destinationId, false);
        messagesToReceives[destinationId - 1].add(message);
        socket.send(message);
    }

    public void stop() {
        stop = true;
    }
}
