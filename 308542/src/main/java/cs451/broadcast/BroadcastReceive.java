package cs451.broadcast;

public interface BroadcastReceive {
    void deliver(int originId, byte[] data);
}
