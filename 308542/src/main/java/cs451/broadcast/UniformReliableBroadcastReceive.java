package cs451.broadcast;

public interface UniformReliableBroadcastReceive {
    void deliver(int originId, int messageId, byte[] data);
}
