package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpSocket {

    private final PerfectLinks links;
    private final BlockingQueue<Message> socketSend;
    private final ByteBuffer inputData = ByteBuffer.allocate(8192).order(ByteOrder.nativeOrder());
    private final ByteBuffer outputData = ByteBuffer.allocate(8192).order(ByteOrder.nativeOrder());
    private final DatagramPacket inputDatagram;
    private final DatagramPacket[] outputDatagrams;
    private DatagramSocket socket;

    public UdpSocket(String ip, int port, PerfectLinks links, List<Host> hosts) {
        this.links = links;
        try {
            this.socket = new DatagramSocket(port);
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }

        inputDatagram = new DatagramPacket(inputData.array(), 0, new InetSocketAddress(ip, port));

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
                    inputData.reset();
                    socket.receive(inputDatagram);
                    boolean ack = inputData.getInt() == 1;
                    int destinationId = inputData.getInt();
                    int sourceId = inputData.getInt();
                    int messageId = inputData.getInt();
                    int originId = inputData.getInt();

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
