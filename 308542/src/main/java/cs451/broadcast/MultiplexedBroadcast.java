package cs451.broadcast;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implementation of the fifo broadcast but with an optimisation which consist of grouping the data we receive
 * to minimise the number of messages to send.
 * This technique has been allowed in the moodle thread here: https://moodle.epfl.ch/mod/forum/discuss.php?d=47833
 * > Are we allowed to combine several pending messages into one? For example, in one of the link layers of our implementation.
 * > If you mean to put several messages in one packet, then this is OK (as long as you put all the message(s) contents in this packet).
 * > Just to be clear on what you mean with "all the messages' contents", would that be all the pairs (originId, messageId)? Thanks!
 * > Yes
 * So we group multiple message which consist of some data and the originId. In our case the data is simply an int but
 * this technique is generic and could also be implemented with some data of the type byte[] of an arbitrary length.
 *
 * @author Wermeille Bastien
 */
public class MultiplexedBroadcast implements BroadcastReceive {

    // UDP Header = 8
    // MAX Causal header = 127*4 = 508
    // Max udp packet size = 65 507
    private static final int MAX_DATA_SIZE = 64090;
    private static final int DATA_SIZE = 5;
    private final MultiplexedBroadcastReceive receiver;
    byte[] data;
    ByteBuffer wrapper;
    int cachedMessages = 0;
    private Broadcast broadcast;

    public MultiplexedBroadcast(MultiplexedBroadcastReceive receiver) {
        this.receiver = receiver;

        data = new byte[MAX_DATA_SIZE];
        wrapper = ByteBuffer.wrap(data, 0, data.length);
    }

    public static int nbMessageReduction(int nbMessage) {
        return (int) Math.ceil(((float) nbMessage / (float) MAX_DATA_SIZE) * DATA_SIZE);
    }

    public void init(Broadcast broadcast) {
        this.broadcast = broadcast;
    }

    public void broadcast(int originId, int data) {
        wrapper.put((byte) originId);
        wrapper.putInt(data);
        cachedMessages++;

        if (wrapper.position() == MAX_DATA_SIZE) {
            flush();
        }
    }

    public void flush() {
        broadcast.broadcast(Arrays.copyOfRange(wrapper.array(), 0, cachedMessages * DATA_SIZE));
        cachedMessages = 0;
        wrapper.clear();
    }

    @Override
    public void deliver(int originId, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, data.length);
        int size = data.length / DATA_SIZE;

        for (int i = 0; i < size; ++i) {
            originId = buffer.get();
            int messageId = buffer.getInt();
            receiver.deliver(originId, messageId);
        }
    }

    public void stop() {
        broadcast.stop();
    }
}
