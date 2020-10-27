package cs451;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class UdpSocket {

    private ByteArrayOutputStream baos;
    private ObjectOutputStream oos;
    private ByteArrayInputStream bais;
    private ObjectInputStream ois;

    private DatagramSocket socket;
    private PerfectLink[] links;

    private BlockingQueue<Message> socketSend;

    private HashMap<Integer, SocketAddress> hosts;

    public UdpSocket(String ip, int port, PerfectLink[] links, List<Host> hosts) throws IOException {
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

        this.hosts = (HashMap<Integer, SocketAddress>) hosts
                .stream()
                .collect(Collectors.toMap(Host::getId, h -> (SocketAddress) new InetSocketAddress(h.getIp(), h.getPort())));

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
                    links[(message.ack ? message.destinationId : message.sourceId)-1].receive(message);
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
                    DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, hosts.get(destinationId));
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
