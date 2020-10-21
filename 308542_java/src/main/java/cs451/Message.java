package cs451;

import java.io.Serializable;
import java.util.Objects;

public class Message implements Serializable {
    int sourceId;
    int destinationId;
    int originId;
    int messageId;
    boolean ack;

    public Message(int sourceId, int destinationId, int originId, int messageId, boolean ack) {
        this.sourceId = sourceId;
        this.destinationId = destinationId;
        this.originId = originId;
        this.messageId = messageId;
        this.ack = ack;
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
}
