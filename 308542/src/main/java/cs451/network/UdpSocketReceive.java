package cs451.network;

import cs451.link.Message;

public interface UdpSocketReceive {
    void deliver(Message message);
}