package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpSocket {

    private static final int HEADER_SIZE = 8;
    private static final int BUFFER_SIZE = HEADER_SIZE + 5000;

    private final UdpSocketReceive receiver;
    private final BlockingQueue<Message> socketSend;
    private final ByteBuffer outputData = ByteBuffer.allocate(BUFFER_SIZE).order(ByteOrder.BIG_ENDIAN);
    private final DatagramPacket inputDatagram;
    private final DatagramPacket[] outputDatagrams;
    private ByteBuffer inputData;
    private DatagramSocket socket;

    //TODO: remove
    private volatile boolean close;

    public UdpSocket(String ip, int port, UdpSocketReceive receiver, List<Host> hosts) {
        this.receiver = receiver;
        try {
            this.socket = new DatagramSocket(port);
            socket.setReceiveBufferSize(200000000); // 100 Mo -> 10 000 000
            System.out.println("Buffer receive size : " + socket.getReceiveBufferSize());
        } catch (SocketException e) {
            System.out.println("Socket Error : " + e);
        }

        inputDatagram = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, new InetSocketAddress(ip, port));
        inputData = ByteBuffer.wrap(inputDatagram.getData(), 0, BUFFER_SIZE);

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
            while (!close) {
                try {
                    socket.receive(inputDatagram);
                    inputData = ByteBuffer.wrap(inputDatagram.getData(), inputDatagram.getOffset(), inputDatagram.getLength());
                    inputData.clear();

                    int originId = inputData.get();
                    int messageId = inputData.getInt();
                    int sourceId = inputData.get();
                    int destinationId = inputData.get();
                    boolean ack = inputData.get() == 1;
                    byte[] data = null;
                    if (!ack) {
                        data = Arrays.copyOfRange(inputData.array(), HEADER_SIZE, inputDatagram.getLength());
                    }

                    receiver.receive(new Message(originId, messageId, sourceId, destinationId, ack, 0, data));
                } catch (IOException e) {
                    System.out.println("Close socket");
                    return;
                }
            }
        }).start();
    }

    private void startSender() {
        new Thread(() -> {
            while (!close) {
                try {
                    Message message = socketSend.take();
                    outputData.clear();
                    outputData.put((byte) message.originId);
                    outputData.putInt(message.messageId);
                    outputData.put((byte) message.sourceId);
                    outputData.put((byte) message.destinationId);
                    outputData.put((byte) (message.ack ? 1 : 0));
                    if (!message.ack) {
                        outputData.put(message.data);
                    }

                    int destinationId = message.ack ? message.sourceId : message.destinationId;
                    DatagramPacket packet = outputDatagrams[destinationId - 1];
                    packet.setData(outputData.array(), 0, outputData.position());

                    socket.send(packet);
                } catch (InterruptedException | IOException e) {
                    System.out.println("Close socket");
                    return;
                }
            }
        }).start();
    }

    public void stop() {
        socket.close();
        close = true;
    }
}
