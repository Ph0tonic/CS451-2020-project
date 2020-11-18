package cs451.broadcast;

public interface MultiplexedBroadcastReceive {
    void deliver(int originId, int messageId);
}
