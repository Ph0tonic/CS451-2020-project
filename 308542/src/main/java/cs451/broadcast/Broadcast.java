package cs451.broadcast;

public interface Broadcast {
    void broadcast(byte[] data);

    void stop();
}
