package cs451;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpSocket {

    private ByteArrayOutputStream baos;
    private ObjectOutputStream oos;
    private ByteArrayInputStream bais;
    private ObjectInputStream ois;

    private DatagramSocket socket;
    private PerfectLink[] links;

    private BlockingQueue<Message> socketSend;

    private SocketAddress[] hosts;

    public UdpSocket(String ip, int port, PerfectLink[] links, List<Host> hosts) {
        this.links = links;
        try {
            this.socket = new DatagramSocket(port);
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }

        socketSend = new LinkedBlockingQueue<>();

//        baos = new ByteArrayOutputStream();
//        oos = new ObjectOutputStream(baos);
//        bais = new ByteArrayInputStream(new byte[50960]);
//        ois = new ObjectInputStream(bais);

        this.hosts = new SocketAddress[hosts.size()];
        hosts.forEach(h -> this.hosts[h.getId() - 1] = new InetSocketAddress(h.getIp(), h.getPort()));

        listener();
        sender();
    }

    public void send(Message message) {
        socketSend.add(message);
    }

    private void listener() {
        new Thread(() -> {
            byte[] incomingData = new byte[1024];
            while (true) {
                try {
                    DatagramPacket incomingPacket = new DatagramPacket(incomingData, incomingData.length);
                    socket.receive(incomingPacket);

                    bais = new ByteArrayInputStream(incomingPacket.getData());
//                    bais.reset();
//                    bais.read(incomingPacket.getData());

                    ois = new ObjectInputStream(bais);
                    Message message = (Message) ois.readObject();
                    links[(message.ack ? message.destinationId : message.sourceId) - 1].receive(message);
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }).start();
    }

    private void sender() {
        new Thread(() -> {
            while (true) {
                try {
                    Message message = socketSend.take();

                    baos = new ByteArrayOutputStream();
                    oos = new ObjectOutputStream(baos);
//                    baos.reset();
                    oos.writeObject(message);

                    byte[] serializedMessage = baos.toByteArray();
                    int destinationId = message.ack ? message.sourceId : message.destinationId;
                    DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, hosts[destinationId-1]);
                    socket.send(packet);
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }).start();
    }

    public void stop() {
        socket.close();
    }
}
