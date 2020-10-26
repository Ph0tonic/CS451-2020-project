package cs451;


import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReliableBroadcast {
    private final UdpSocket socket;
    private int pid;

    private int nbHosts;

    private Map<Integer, PerfectLink> links;

    private ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>>> linkDelivered; // originId -> messageId -> sourceId
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
        this.links = hosts.stream().filter(e -> e.getId() != pid).collect(Collectors.toMap(Host::getId, PerfectLink::new));
        this.socket = new UdpSocket(host.get().getIp(), host.get().getPort(), links, hosts);

        // Start perfectLinks
        this.links.forEach((Integer id, PerfectLink l) -> l.init(socket, this));
        this.links.forEach((i, p) -> new Thread(p).start());

        linkDelivered = new ConcurrentHashMap<>();
        hosts.forEach(h -> linkDelivered.put(h.getId(), new ConcurrentHashMap<>()));
        linkDelivered.forEach((o, m) -> {
            IntStream.range(1, nbMessage + 1).forEach(i -> m.put(i, new ConcurrentSkipListSet<>()));
        });
    }

    private boolean isReadyToDeliver(int nb) {
        //TODO: Check if the +1 to take into account this process is correct
        return nb + 1 > nbHosts / 2;
    }

    public void receive(int originId, int messageId, int sourceId) {
        ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>> map = linkDelivered.get(originId);
        Set<Integer> set = map.get(messageId);
        set.add(sourceId);

        //TODO: Think about response
        if (isReadyToDeliver(set.size())) {
            // Deliver this message
            broadcast.receive(originId, messageId);
        } else {
            links.entrySet().parallelStream()
                    .filter(e -> !map.containsKey(e.getKey()))
                    .forEach(e -> e.getValue().send(originId, messageId, sourceId));
        }
    }

    public void broadcast(int messageId) {
        links.entrySet().parallelStream().forEach(e -> {
            e.getValue().send(pid, messageId, pid);
        });
    }

    public void stop() {
        socket.stop();
        links.forEach((i, l) -> l.stop());
    }
}