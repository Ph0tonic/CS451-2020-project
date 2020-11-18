package cs451.link;

import java.io.Serializable;
import java.util.Objects;

/**
 * Datatype which is use to exchange data between perfectlink and udpsocket
 *
 * @author Wermeille Bastien
 */
public class Message implements Serializable, Comparable<Message> {
    public int sourceId;
    public int destinationId;
    public int originId;
    public int messageId;
    public boolean ack;
    public long time;
    public double count;
    public byte[] data;

    public Message(int originId, int messageId, int sourceId, int destinationId, boolean ack, long time, byte[] data) {
        this.originId = originId;
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.destinationId = destinationId;
        this.ack = ack;
        this.time = time;
        this.count = 1;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return sourceId == message.sourceId &&
                destinationId == message.destinationId &&
                originId == message.originId &&
                messageId == message.messageId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, destinationId, originId, messageId);
    }

    @Override
    public int compareTo(Message m) {
        int res1 = Integer.compare(sourceId, m.sourceId);
        int res2 = Integer.compare(destinationId, m.destinationId);
        int res3 = Integer.compare(originId, m.originId);
        int res4 = Integer.compare(messageId, m.messageId);
        return res1 != 0 ? res1 : res2 != 0 ? res2 : res3 != 0 ? res3 : res4;
    }
}
