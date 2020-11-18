package cs451.link;

import cs451.config.Host;
import cs451.network.UdpSocket;
import cs451.network.UdpSocketReceive;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implementation of a perfect link
 * This perfect link do not generate it's own messagesId such as to reuse the one of a potential upper layer
 * which allow to save 4 byte of an additional id
 *
 * @author Wermeille Bastien
 */
public class PerfectLinks implements UdpSocketReceive {

    private static final long RETRANSMIT_TIMEOUT_MS = 300;
    private static final long RETRANSMIT_BREAK_MS = 50;

    private final LinkedBlockingQueue<Message>[] socketReceive;
    private final ConcurrentSkipListSet<Message>[] messagesToReceives;
    private final HashSet<Integer>[][] delivered; //originid -> messageId -> sourceId
    private final int nbReceiver;
    private final int nbHost;
    private final int retransmitWindows;

    private final UdpSocket socket;
    private final PerfectLinksReceive broadcast;
    private volatile boolean stop = false;

    private Thread retransmitter;
    private Thread[] listeners;

    public PerfectLinks(int id, int nbMessage, List<Host> hosts, PerfectLinksReceive receiver) throws Exception {
        int nbThread = Runtime.getRuntime().availableProcessors() - 2;
        broadcast = receiver;

        nbReceiver = Math.max(Math.min(nbThread - 1, hosts.size()), 1);
        nbHost = hosts.size();
        retransmitWindows = Math.max(5000 / (nbHost * nbHost), 1);
        System.out.println("Using nb receivers " + nbReceiver);

        messagesToReceives = new ConcurrentSkipListSet[nbHost];
        hosts.forEach(h -> messagesToReceives[h.getId() - 1] = new ConcurrentSkipListSet<>());

        socketReceive = new LinkedBlockingQueue[nbReceiver];
        for (int i = 0; i < nbReceiver; ++i) {
            socketReceive[i] = new LinkedBlockingQueue<>();
        }

        delivered = new HashSet[nbHost][nbMessage];
        for (int i = 0; i < nbHost; ++i) {
            for (int j = 0; j < nbMessage; ++j) {
                delivered[i][j] = new HashSet<>();
            }
        }

        Optional<Host> host = hosts.stream().filter(x -> x.getId() == id).findFirst();
        if (host.isEmpty()) {
            throw new Exception("Not able to find host info");
        }

        this.socket = new UdpSocket(host.get().getIp(), host.get().getPort(), this, hosts);

        startReceivers();
        startRetransmitter();
    }

    private void startReceivers() {
        listeners = new Thread[nbReceiver];
        for (int i = 0; i < nbReceiver; ++i) {
            final int j = i;
            System.out.println("Start thread " + j);

            listeners[i] = new Thread(() -> {
                while (!stop) {
                    try {
                        Message message = socketReceive[j].take();

                        if (stop) {
                            System.out.println("Stopped");
                            return;
                        }

                        int sourceId = message.ack ? message.destinationId : message.sourceId;
                        if (message.ack) {
                            for (Message m : messagesToReceives[sourceId - 1]) {
                                if (m.equals(message)) {
                                    message.data = m.data;
                                    break;
                                }
                            }
                            messagesToReceives[message.destinationId - 1].remove(message);
                        } else {
                            message.ack = true;
                            socket.send(message);
                        }

                        if (!delivered[message.originId - 1][message.messageId - 1].add(sourceId)) {
                            continue; // Duplicated received message
                        }

                        broadcast.deliver(message.originId, message.messageId, sourceId, message.data);
                    } catch (InterruptedException e) {
                        System.out.println("Stopped !");
                        e.printStackTrace();
                    }
                }
            });
            listeners[i].start();
        }
    }

    private void startRetransmitter() {
        System.out.println("Start retransmitter with windows " + retransmitWindows);
        retransmitter = new Thread(() -> {
            while (!stop) {
                try {
                    Thread.sleep(RETRANSMIT_BREAK_MS);
                    if (stop) {
                        return;
                    }
                    for (int i = 0; i < this.nbHost; ++i) {
                        long now = System.currentTimeMillis();
                        messagesToReceives[i].stream().filter(m -> now > m.time).limit(retransmitWindows).forEach(m -> {
                            socket.send(m);
                            m.count++;
                            m.time = now + (long) (Math.random() * 100) + RETRANSMIT_TIMEOUT_MS * (long) Math.pow(1.2, m.count);
                        });
                    }
                } catch (InterruptedException e) {
                    System.out.println("Retransmitter stopped");
                }
            }
        });
        retransmitter.start();
    }

    @Override
    public void deliver(Message message) {
        socketReceive[message.originId % nbReceiver].add(message);
    }

    public void send(int originId, int messageId, int sourceId, int destinationId, byte[] data) {
        Message message = new Message(originId, messageId, sourceId, destinationId, false, System.currentTimeMillis() + RETRANSMIT_TIMEOUT_MS, data);
        messagesToReceives[destinationId - 1].add(message);
        socket.send(message);
    }

    public void stop() {
        stop = true;
        socket.stop();
        for (int i = 0; i < nbReceiver; ++i) {
            socketReceive[i].add(new Message(0, 0, 0, 0, false, 0, null));
        }
        for (int i = 0; i < nbReceiver; ++i) {
            try {
                listeners[i].join();
            } catch (InterruptedException ignored) {
                System.out.println("listener " + i + " already closed");
            }
        }
        try {
            retransmitter.join();
        } catch (InterruptedException ignored) {
            System.out.println("retransmitter already closed");
        }
    }
}
