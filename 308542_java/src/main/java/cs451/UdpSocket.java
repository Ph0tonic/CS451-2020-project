package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpSocket {

    private final PerfectLinks links;
    private final BlockingQueue<Message> socketSend;
    private final ByteBuffer outputData = ByteBuffer.allocate(128).order(ByteOrder.BIG_ENDIAN);
    private final DatagramPacket inputDatagram;
    private final DatagramPacket[] outputDatagrams;
    private ByteBuffer inputData = ByteBuffer.allocate(128).order(ByteOrder.nativeOrder());
    private DatagramSocket socket;

    //TODO: remove
    private List<Host> hosts;

    public UdpSocket(String ip, int port, PerfectLinks links, List<Host> hosts) {
        this.hosts = hosts;
        this.links = links;
        try {
            this.socket = new DatagramSocket(port);
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }

        inputDatagram = new DatagramPacket(new byte[128], 128, new InetSocketAddress(ip, port));
        inputData = ByteBuffer.wrap(inputDatagram.getData(), 0, 128);

        socketSend = new LinkedBlockingQueue<>();

        outputDatagrams = new DatagramPacket[hosts.size()];
        hosts.forEach(h -> outputDatagrams[h.getId() - 1] = new DatagramPacket(outputData.array(), 0, new InetSocketAddress(h.getIp(), h.getPort())));

        startListener();
        startSender();
    }

    public void send(Message message) {
        socketSend.add(message);
    }

    private void startListener() {
        new Thread(() -> {
            while (true) {
                try {
                    socket.receive(inputDatagram);
                    inputData = ByteBuffer.wrap(inputDatagram.getData(), 0, 128);
                    inputData.clear();

                    int originId = inputData.getInt();
                    int messageId = inputData.getInt();
                    int sourceId = inputData.getInt();
                    int destinationId = inputData.getInt();
                    boolean ack = inputData.getInt() == 1;

                    links.receive(new Message(originId, messageId, sourceId, destinationId, ack));
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }).start();
    }

    private void startSender() {
        new Thread(() -> {
            while (true) {
                try {
                    Message message = socketSend.take();
                    outputData.clear();
                    outputData.putInt(message.originId);
                    outputData.putInt(message.messageId);
                    outputData.putInt(message.sourceId);
                    outputData.putInt(message.destinationId);
                    outputData.putInt(message.ack ? 1 : 0);

                    int destinationId = message.ack ? message.sourceId : message.destinationId;
                    DatagramPacket packet = outputDatagrams[destinationId - 1];
                    packet.setData(outputData.array(), 0, outputData.position());

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
