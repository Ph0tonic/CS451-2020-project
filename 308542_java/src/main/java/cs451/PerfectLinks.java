package cs451;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

class PerfectLinks implements UdpSocketReceive {

    private final LinkedBlockingQueue<Message>[] socketReceive;
    private final ConcurrentSkipListSet<Message>[] messagesToReceives;
    public int nbReceiver;
    public int nbHost;
    long retransmitTimeoutMS = 250;
    long resendFrequencyMS = 50;
    private int retransmitWindows;

    private UdpSocket socket;
    private PerfectLinksReceive broadcast;
    private volatile boolean stop = false;

    public PerfectLinks(int id, List<Host> hosts, PerfectLinksReceive receiver) throws Exception {
        int nbThread = Runtime.getRuntime().availableProcessors() - 2;
        broadcast = receiver;

        nbReceiver = Math.max(Math.min(nbThread - 1, hosts.size()), 1);
        nbHost = hosts.size();
        retransmitWindows = Math.max(5000 / (nbHost * 1), 1);
        System.out.println("Using nb receivers " + nbReceiver);

        messagesToReceives = new ConcurrentSkipListSet[nbHost];
        hosts.forEach(h -> messagesToReceives[h.getId() - 1] = new ConcurrentSkipListSet<>());

        socketReceive = new LinkedBlockingQueue[nbReceiver];
        for (int i = 0; i < nbReceiver; ++i) {
            socketReceive[i] = new LinkedBlockingQueue<>();
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
        for (int i = 0; i < nbReceiver; ++i) {
            final int j = i;
            System.out.println("Start thread " + j);

            new Thread(() -> {
                while (!stop) {
                    try {
                        Message message = socketReceive[j].take();
                        if (stop) {
                            return;
                        }
                        if (message.ack) {
                            for (Message m : messagesToReceives[message.destinationId - 1]) {
                                if (m.equals(message)) {
                                    message.data = m.data;
                                    break;
                                }
                            }
                            messagesToReceives[message.destinationId - 1].remove(message);
                            broadcast.receive(message.originId, message.messageId, message.destinationId, message.data);
                        } else {
                            message.ack = true;
                            socket.send(message);
                            broadcast.receive(message.originId, message.messageId, message.sourceId, message.data);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    private void startRetransmitter() {
        System.out.println("Start retransmitter with windows " + retransmitWindows);
        new Thread(() -> {
            System.out.println("Started");
            while (!stop) {
                try {
                    Thread.sleep(resendFrequencyMS);
                    if (stop) {
                        return;
                    }
                    // TODO: Improve this by minimising number of messages ressent
                    for (int i = 0; i < this.nbHost; ++i) {
                        long now = System.currentTimeMillis();
                        messagesToReceives[i].stream().filter(m -> now > m.time).limit(retransmitWindows).forEach(m -> {
                            socket.send(m);
                            m.count++;
                            m.time = now + m.count * retransmitTimeoutMS;
                        });
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void receive(Message message) {
        socketReceive[message.originId % nbReceiver].add(message);
    }

    public void send(int originId, int messageId, int sourceId, int destinationId, byte[] data) {
        Message message = new Message(originId, messageId, sourceId, destinationId, false, System.currentTimeMillis() + retransmitTimeoutMS, data);
        messagesToReceives[destinationId - 1].add(message);
        socket.send(message);
    }

    public void stop() {
        stop = true;
        for (int i = 0; i < nbReceiver; ++i) {
            socketReceive[i].add(new Message(0, 0, 0, 0, false, 0, null));
        }
        socket.stop();
    }
}
