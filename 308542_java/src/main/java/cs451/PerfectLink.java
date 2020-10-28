package cs451;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

class PerfectLink implements Runnable {

    private final BlockingQueue<Message> socketReceive;

    private final ConcurrentSkipListSet<Message> messagesToReceive;

    private final int id; //destinationId
    private UdpSocket socket;
    private ReliableBroadcast broadcast;

    private volatile boolean stop = false;

    public PerfectLink(Host host) {
        this.id = host.getId();

        messagesToReceive = new ConcurrentSkipListSet<>();
        socketReceive = new LinkedBlockingQueue<>();
    }

    public void init(UdpSocket socket, ReliableBroadcast broadcast) {
        this.socket = socket;
        this.broadcast = broadcast;
    }

    public void receive(Message message) {
        socketReceive.add(message);
    }

    public void send(int originId, int messageId, int sourceId) {
        Message message = new Message(originId, messageId, sourceId, id, false);
        messagesToReceive.add(message);
        socket.send(message);
    }

    public void run() {
        // receive
        new Thread(() -> {
            while (!stop) {
                try {
                    Message message = socketReceive.take();
                    if (stop) {
                        return;
                    }
                    if (message.ack) {
                        messagesToReceive.remove(message);
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

        // send
        new Thread(() -> {
            while (!stop) {
                try {
                    Thread.sleep(500);
                    if (stop) {
                        return;
                    }
                    messagesToReceive.parallelStream().forEach(socket::send);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void stop() {
        stop = true;
    }
}
