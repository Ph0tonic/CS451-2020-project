package cs451.link;

public interface PerfectLinksReceive {
    void deliver(int originId, int messageId, int sourceId, byte[] data);
}