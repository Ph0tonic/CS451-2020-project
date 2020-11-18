package cs451.broadcast;

public interface CausalOrderBroadcastReceive {
    void deliver(int originId, byte[] data);
}
