package cs451;

public interface ReliableBroadcastReceive {
    void receive(int originId, int messageId, byte[] data);
}
