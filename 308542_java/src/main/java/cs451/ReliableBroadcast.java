package cs451;


import java.util.HashSet;
import java.util.List;

public class ReliableBroadcast implements PerfectLinksReceive {

    private int id;
    private int nbHosts;
    private PerfectLinks links;

    private HashSet[][] linkDelivered; // originId -> messageId -> sourceId
    private ReliableBroadcastReceive broadcast;

    public ReliableBroadcast(int id, int nbMessage, List<Host> hosts, ReliableBroadcastReceive broadcast) throws Exception {
        this.id = id;
        this.nbHosts = hosts.size();
        this.broadcast = broadcast;

        // Init socket and links
        this.links = new PerfectLinks(id, hosts, this);

        linkDelivered = new HashSet[nbHosts][nbMessage];
        for (int i = 0; i < linkDelivered.length; i++) {
            linkDelivered[i] = new HashSet[nbMessage];
            for (int j = 0; j < linkDelivered[i].length; j++) {
                linkDelivered[i][j] = new HashSet();
            }
        }
    }

    private boolean isReadyToDeliver(int nb) {
        return nb + 1 > nbHosts / 2;
    }

    @Override
    public void receive(int originId, int messageId, int sourceId, byte[] data) {
        var set = linkDelivered[originId - 1][messageId - 1];
        int size = set.size();
        set.add(sourceId);
        if (set.size() == size) {
            return;
        }

        if (set.size() == 1 && originId != id) {
            // Only send once to all other hosts
            for (int i = 0; i < nbHosts; i++) {
                if (i + 1 != id && !linkDelivered[originId - 1][messageId - 1].contains(i + 1)) {
                    links.send(originId, messageId, id, i + 1, data);
                }
            }
        }

        // Deliver if possible
        if (isReadyToDeliver(set.size()) && !isReadyToDeliver(set.size() - 1)) {
            broadcast.receive(originId, messageId, data);
        }
    }

    public void broadcast(int messageId, byte[] data) {
        // Expect messages to arrive in order
        for (int i = 0; i < nbHosts; i++) {
            if (i + 1 != id) {
                links.send(id, messageId, id, i + 1, data);
            }
        }
    }

    public void stop() {
        links.stop();
    }
}