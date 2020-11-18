package cs451.broadcast;

import cs451.config.Host;
import cs451.link.PerfectLinks;
import cs451.link.PerfectLinksReceive;

import java.util.Arrays;
import java.util.List;

/**
 * UniformReliableBroadcast implementation of URB which allow to send some data with a messageId
 *
 * @author Wermeille Bastien
 */
public class UniformReliableBroadcast implements PerfectLinksReceive {

    private final int id;
    private final int nbHosts;
    private final PerfectLinks links;

    // originId -> messageId -> sourceId
    private final int[][] linkDelivered;
    private final UniformReliableBroadcastReceive broadcast;

    public UniformReliableBroadcast(int id, int nbMessage, List<Host> hosts, UniformReliableBroadcastReceive broadcast) throws Exception {
        this.id = id;
        this.nbHosts = hosts.size();
        this.broadcast = broadcast;

        // Init socket and links
        this.links = new PerfectLinks(id, nbMessage, hosts, this);

        linkDelivered = new int[nbHosts][nbMessage];
        for (int[] ints : linkDelivered) {
            Arrays.fill(ints, 0);
        }
    }

    private boolean isReadyToDeliver(int nb) {
        return nb + 1 > nbHosts / 2 && nb <= nbHosts / 2;
    }

    @Override
    public void deliver(int originId, int messageId, int sourceId, byte[] data) {
        linkDelivered[originId - 1][messageId - 1]++;

        if (linkDelivered[originId - 1][messageId - 1] == 1 && originId != id) {
            // Only send once to all other hosts
            for (int i = 1; i <= nbHosts; ++i) {
                if (i != id && i != originId && i != sourceId) {
                    links.send(originId, messageId, id, i, data);
                }
            }
        }

        // Deliver if possible
        if (isReadyToDeliver(linkDelivered[originId - 1][messageId - 1])) {
            broadcast.deliver(originId, messageId, data);
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