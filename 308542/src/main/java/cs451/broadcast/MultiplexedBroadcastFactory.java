package cs451.broadcast;

import cs451.config.Parser;

public class MultiplexedBroadcastFactory {

    public static MultiplexedBroadcast createFifoBroadcast(MultiplexedBroadcastReceive receiver, Parser parser, int nbMessage) throws InterruptedException {
        var multiplexedBroadcast = new MultiplexedBroadcast(receiver);
        Broadcast fifoBroadcast = new FifoBroadcast(parser.myId(), MultiplexedBroadcast.nbMessageReduction(nbMessage), parser.hosts(), multiplexedBroadcast);
        multiplexedBroadcast.init(fifoBroadcast);
        return multiplexedBroadcast;
    }

    public static MultiplexedBroadcast createCausalBroadcast(MultiplexedBroadcastReceive receiver, Parser parser, int nbMessage, int[][] causals) throws Exception {
        var multiplexedBroadcast = new MultiplexedBroadcast(receiver);
        Broadcast causalBroadcast = new CausalOrderBroadcast(parser.myId(), MultiplexedBroadcast.nbMessageReduction(nbMessage), parser.hosts(), multiplexedBroadcast, causals);
        multiplexedBroadcast.init(causalBroadcast);
        return multiplexedBroadcast;
    }
}
