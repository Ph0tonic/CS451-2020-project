package cs451;


import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class ReliableBroadcast {
    private final UdpSocket socket;
    private int pid;

    private int nbHosts;

    private PerfectLink[] links;
    //TODO: Once working change into non concurrent has single threaded ! TODO: check really non-multithreaded
    private HashSet[][] linkDelivered; // originId -> messageId -> sourceId
    private FifoBroadcast broadcast;

    public ReliableBroadcast(int pid, int nbMessage, List<Host> hosts, FifoBroadcast broadcast) throws Exception {
        this.pid = pid;
        this.nbHosts = hosts.size();
        this.broadcast = broadcast;

        Optional<Host> host = hosts.stream().filter(x -> x.getId() == pid).findFirst();
        if (host.isEmpty()) {
            throw new Exception("Not able to find host info");
        }

        // Init socket and links
        this.links = new PerfectLink[nbHosts];
        this.socket = new UdpSocket(host.get().getIp(), host.get().getPort(), this.links, hosts);

        for (var h : hosts) {
            if (h.getId() != pid) {
                // Create and start perfect links
                links[h.getId() - 1] = new PerfectLink(h);
                links[h.getId() - 1].init(socket, this);
                // TODO: Change this to be a pool of thread !!!!!!!!!!
                new Thread(links[h.getId() - 1]).start();
            } else {
                links[h.getId() - 1] = null;
            }
        }

        // Initialise data structure
        linkDelivered = new HashSet[nbHosts][nbMessage];
        for (int i = 0; i < linkDelivered.length; i++) {
            linkDelivered[i] = new HashSet[nbMessage];
            for (int j = 0; j < linkDelivered[i].length; j++) {
                linkDelivered[i][j] = new HashSet();
            }
        }
    }

    private boolean isReadyToDeliver(int nb) {
        //TODO: Check if the +1 to take into account this process is correct
        return nb + 1 > nbHosts / 2;
    }

    public void receive(int originId, int messageId, int sourceId) {
        System.out.println("URB RECEIVE " + originId + " " + messageId + " " + sourceId);
        var set = linkDelivered[originId - 1][messageId - 1];
        set.add(sourceId);

        //TODO: Think about response
        if (isReadyToDeliver(set.size()) && !isReadyToDeliver(set.size() - 1)) {
            // Deliver this message
            broadcast.receive(originId, messageId);
        } else {
            for (int i = 0; i < links.length; i++) {
                if (i + 1 != pid && !linkDelivered[originId - 1][messageId - 1].contains(i + 1)) {
                    links[i + 1].send(originId, messageId, pid);
                }
            }
        }
    }

    public void broadcast(int messageId) {
        for (var l : links) {
            if (l != null) {
                l.send(pid, messageId, pid);
            }
        }
    }

    public void stop() {
        socket.stop();
        for (var l : links) {
            if (l != null) {
                l.stop();
            }
        }
    }
}