package cs451;

interface PerfectLinksReceive {
    void receive(int originId, int messageId, int sourceId, byte[] data);
}