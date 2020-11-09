package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpSocket {

    private final UdpSocketReceive receiver;
    private final BlockingQueue<Message> socketSend;
    private final ByteBuffer outputData = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
    private final DatagramPacket inputDatagram;
    private final DatagramPacket[] outputDatagrams;
    private final InetSocketAddress[] addresses;
    private ByteBuffer inputData = ByteBuffer.allocate(32).order(ByteOrder.nativeOrder());
    private DatagramSocket socket;

    //TODO: remove
    private volatile boolean close;
    private List<Host> hosts;

    public UdpSocket(String ip, int port, UdpSocketReceive receiver, List<Host> hosts) {
        // try {
        //     Process process = Runtime.getRuntime().exec("sysctl -w net.core.rmem_max=20000000", null, null);
        //     process.waitFor();
        //     System.out.println();
        // } catch (IOException | InterruptedException e) {
        //     System.out.println("Process Error : " + e);
        // }
        this.hosts = hosts;
        this.receiver = receiver;
        try {
            this.socket = new DatagramSocket(port);
            socket.setReceiveBufferSize(200000000); // 100 Mo -> 10 000 000
            System.out.println("Buffer receive size : " + socket.getReceiveBufferSize());
        } catch (SocketException e) {
            System.out.println("Socket Error : " + e);
        }

        inputDatagram = new DatagramPacket(new byte[20000], 20000, new InetSocketAddress(ip, port));
        inputData = ByteBuffer.wrap(inputDatagram.getData(), 0, 20000);

        socketSend = new LinkedBlockingQueue<>();

        outputDatagrams = new DatagramPacket[hosts.size()];
        hosts.forEach(h -> outputDatagrams[h.getId() - 1] = new DatagramPacket(outputData.array(), 0, new InetSocketAddress(h.getIp(), h.getPort())));

        addresses = new InetSocketAddress[hosts.size()];
        hosts.forEach(h -> addresses[h.getId() - 1] = new InetSocketAddress(h.getIp(), h.getPort()));

        startListener();
        startSender();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void send(Message message) {
        socketSend.add(message);
    }

    private void startListener() {
        new Thread(() -> {
            while (!close) {
                try {
                    socket.receive(inputDatagram);
                    inputData = ByteBuffer.wrap(inputDatagram.getData(), inputDatagram.getOffset(), inputDatagram.getLength());

                    inputData.clear();

                    int originId = inputData.getInt();
                    int messageId = inputData.getInt();
                    int sourceId = inputData.getInt();
                    int destinationId = inputData.getInt();
                    boolean ack = inputData.getInt() == 1;

                    receiver.receive(new Message(originId, messageId, sourceId, destinationId, ack));
                } catch (IOException e) {
                    System.out.println("DONE LISTENING EXC !!!");
                    e.printStackTrace();
                    return;
                }
            }
            System.out.println("DONE LISTENING !!!");
        }).start();
    }

    private void startSender() {
        new Thread(() -> {
            while (!close) {
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
                    System.out.println("DONE SENDING !!! exc");
                    e.printStackTrace();
                    return;
                }
            }
            System.out.println("DONE SENDING !!!");
        }).start();
    }

    public void stop() {
        socket.close();
        close = true;
    }
}
