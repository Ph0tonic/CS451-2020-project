package cs451;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

class PerfectLink implements Runnable {

    private BlockingQueue<Message> socketReceive;

    private ConcurrentSkipListSet<Message> messagesToReceive;

    private int id;
    private String ip;
    private int port;

    private UdpSocket socket;
    private ReliableBroadcast broadcast;

    public PerfectLink(Host host) {
        this.id = host.getId();
        this.ip = host.getIp();
        this.port = host.getPort();

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
        Message message = new Message(sourceId, id, messageId, originId, false);
        messagesToReceive.add(message);
        socket.send(message);
    }

    public void run() {
        // receive
        new Thread(() -> {
            while (true) {
                try {
                    Message message = socketReceive.take();
                    if (message.ack) {
                        messagesToReceive.remove(message);
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
            while (true) {
                try {
                    Thread.sleep(500);
                    messagesToReceive.parallelStream().forEach(socket::send);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
