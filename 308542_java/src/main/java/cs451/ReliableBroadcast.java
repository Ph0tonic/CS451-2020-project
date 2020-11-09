package cs451;


import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class ReliableBroadcast implements PerfectLinksReceive {
    // TODO: Adapt to improve performances

    private final UdpSocket socket;
    private int id;

    private int nbHosts;

    private PerfectLinks links;

    // TODO: Once working change into non concurrent has single threaded ! TODO: check really non-multithreaded
    private HashSet[][] linkDelivered; // originId -> messageId -> sourceId
    private FifoBroadcast broadcast;

    public ReliableBroadcast(int id, int nbMessage, List<Host> hosts, FifoBroadcast broadcast) throws Exception {
        this.id = id;
        this.nbHosts = hosts.size();
        this.broadcast = broadcast;

        Optional<Host> host = hosts.stream().filter(x -> x.getId() == id).findFirst();
        if (host.isEmpty()) {
            throw new Exception("Not able to find host info");
        }

        // Init socket and links
        this.links = new PerfectLinks(hosts, 6, id);
        this.socket = new UdpSocket(host.get().getIp(), host.get().getPort(), this.links, hosts);
        this.links.init(socket, this);

        linkDelivered = new HashSet[nbHosts][nbMessage];
        for (int i = 0; i < linkDelivered.length; i++) {
            linkDelivered[i] = new HashSet[nbMessage];
            for (int j = 0; j < linkDelivered[i].length; j++) {
                linkDelivered[i][j] = new HashSet();
            }
        }
    }

    private boolean isReadyToDeliver(int nb) {
        // TODO: Check if the +1 to take into account this process is correct
        return nb + 1 > nbHosts / 2;
    }

    public void receive(int originId, int messageId, int sourceId) {
        var set = linkDelivered[originId - 1][messageId - 1];
        int size = set.size();
        set.add(sourceId);
        if (set.size() == size) {
            return;
        }

        if (set.size() == 1 && originId != id) {
            // System.out.println("--- "+originId+" "+messageId);
            // Only send once to all other hosts
            for (int i = 0; i < nbHosts; i++) {
                if (i + 1 != id && !linkDelivered[originId - 1][messageId - 1].contains(i + 1)) {
                    links.send(originId, messageId, id, i + 1);
                }
            }
        } // else {
            // System.out.println("### "+originId+" "+messageId);
        // }

        // Deliver if possible
        if (isReadyToDeliver(set.size()) && !isReadyToDeliver(set.size() - 1)) {
            // Deliver this message
            // System.out.println("+++ " + originId + " " + messageId);
            broadcast.receive(originId, messageId);
        }
    }

    public void broadcast(int messageId) {
        // Expect messages to arrive in order
        for (int i = 0; i < nbHosts; i++) {
            if (i + 1 != id) {
                links.send(id, messageId, id, i + 1);
            }
        }
    }

    public void stop() {
        socket.stop();
        links.stop();
    }
}