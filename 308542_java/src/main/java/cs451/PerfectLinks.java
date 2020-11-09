package cs451;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

class PerfectLinks implements UdpSocketReceive {

    private final LinkedBlockingQueue<Message>[] socketReceive;
    private final ConcurrentSkipListSet<Message>[] messagesToReceives;
    public int nbReceivers;
    public int nbHosts;
    private int retransmitWindows;
    private int id;
    private UdpSocket socket;
    private PerfectLinksReceive broadcast;
    private volatile boolean stop = false;

    public PerfectLinks(List<Host> hosts, int nbThread, int id) {
        nbReceivers = Math.max(Math.min(nbThread - 1, hosts.size()), 1);
        nbHosts = hosts.size();
        retransmitWindows = 10000 / nbHosts;
        System.out.println("NB Receivers " + nbReceivers);

        messagesToReceives = new ConcurrentSkipListSet[nbHosts];
        hosts.forEach(h -> messagesToReceives[h.getId() - 1] = new ConcurrentSkipListSet<>());

        socketReceive = new LinkedBlockingQueue[nbReceivers];
        for (int i = 0; i < nbReceivers; ++i) {
            socketReceive[i] = new LinkedBlockingQueue<>();
        }
    }

    private void startReceivers() {
        for (int i = 0; i < nbReceivers; ++i) {
            final int j = i;
            System.out.println("Start thread " + j);

            new Thread(() -> {
                int nb = 0;
                int nbAck = 0;

                while (!stop) {
                    try {
//                        System.out.println("Receiver " + j + " " + socketReceive[j].size());
                        Message message = socketReceive[j].take();
//                        long start = System.nanoTime();
                        if (stop) {
                            return;
                        }
                        if (message.ack) {
                            nbAck++;
                            boolean removed = messagesToReceives[message.destinationId - 1].remove(message);
                            // System.out.println("REMOVE ELEMENT ACK : "+removed);
                            broadcast.receive(message.originId, message.messageId, message.destinationId);
                        } else {
                            nb++;
                            message.ack = true;
                            socket.send(message);
                            broadcast.receive(message.originId, message.messageId, message.sourceId);
                        }

//                        if (message.messageId % 100 == 1) {
//                            System.out.println("Nb treated "+nb+" ack "+nbAck);
//                            System.out.println(Thread.currentThread().getName() + " Treat message on " + j + " from " + message.originId);
//                        }
//                        System.out.println("Time elapsed : " + (System.nanoTime() - start));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        System.out.println("EEEEEEEEEEEEE " + e);
                    }
                }
                System.out.println("OUT ------------------------------");
            }).start();
        }
    }

    private void startRetransmitter() {
        System.out.println("Start retransmitter with windows " + retransmitWindows);
        new Thread(() -> {
            System.out.println("Started");
            while (!stop) {
                try {
                    Thread.sleep(100);
                    if (stop) {
                        return;
                    }
                    // TODO: Improve this by minimising number of messages ressent
                    for (int i = 0; i < this.nbHosts; ++i) {
                        messagesToReceives[i].stream().limit(retransmitWindows).forEach(socket::send);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void init(UdpSocket socket, PerfectLinksReceive broadcast) {
        this.socket = socket;
        this.broadcast = broadcast;

        startReceivers();
        startRetransmitter();
    }

    public void receive(Message message) {
        socketReceive[message.originId % nbReceivers].add(message);
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
