package cs451;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class MultiplexingBroadcast implements FifoReceive {

    private static int MULTIPLEX_MESSAGE_NUMBER = 1000;
    private static int DATA_SIZE = 5;

    byte[] data;
    ByteBuffer wrapper;
    int cachedMessages = 0;
    private FifoBroadcast broadcast;
    private MultiplexingReceive receiver;

    MultiplexingBroadcast(int id, int nbMessages, List<Host> hosts, MultiplexingReceive receiver) throws InterruptedException {
        broadcast = new FifoBroadcast(id, nbMessages, hosts, this);
        this.receiver = receiver;

        data = new byte[MULTIPLEX_MESSAGE_NUMBER * DATA_SIZE];
        wrapper = ByteBuffer.wrap(data, 0, data.length);
    }

    void broadcast(int originId, int messageId) {
        wrapper.put((byte)originId);
        wrapper.putInt(messageId);
        cachedMessages++;

        if (wrapper.position() == MULTIPLEX_MESSAGE_NUMBER) {
            flush();
        }
    }

    void flush() {
        broadcast.broadcast(Arrays.copyOfRange(wrapper.array(), 0, cachedMessages * DATA_SIZE));
        cachedMessages = 0;
        wrapper.clear();
    }

    @Override
    public void receive(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, data.length);
        int size = data.length / DATA_SIZE;
        for (int i = 0; i < size; ++i) {
            int originId = buffer.get();
            int messageId = buffer.getInt();
            receiver.receive(originId, messageId);
        }
    }

    public void stop() {
        broadcast.stop();
    }
}
