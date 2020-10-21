package cs451;


import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReliableBroadcast {
    private final UdpSocket socket;
    private int pid;

    private List<Host> hosts;

    private String ip;
    private int port;
    private int nbHosts;

    private Map<Integer, PerfectLink> links;

    private ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>>> linkDelivered; // originId -> messageId -> sourceId
    private FifoBroadcast broadcast;

    public ReliableBroadcast(int pid, int nbMessage, List<Host> hosts, FifoBroadcast broadcast) throws Exception {
        this.pid = pid;
        this.hosts = hosts;
        this.nbHosts = hosts.size();
        this.broadcast = broadcast;

        Optional<Host> host = hosts.stream().filter(x -> x.getId() == pid).findFirst();
        if (host.isEmpty()) {
            throw new Exception("Not able to find host info");
        }
        this.ip = host.get().getIp();
        this.port = host.get().getPort();

        // Init socket and links
        this.links = hosts.stream().collect(Collectors.toMap(Host::getId, PerfectLink::new));
        this.socket = new UdpSocket(this.ip, this.port, links, hosts);

        // Start perfectLinks
        this.links.forEach((Integer id, PerfectLink l) -> l.init(socket, this));
        this.links.forEach((i, p) -> new Thread(p).start());

        hosts.forEach(h -> linkDelivered.put(h.getId(), new ConcurrentHashMap<>()));
        linkDelivered.forEach((o, m) -> {
            IntStream.range(1, nbMessage + 1).forEach(i -> m.put(i, new ConcurrentSkipListSet<>()));
        });
    }

    private boolean isReadyToDeliver(int nb) {
        return nb == nbHosts - 1;
    }

    public void receive(int originId, int messageId, int sourceId) {
        linkDelivered.get(originId).get(messageId).add(sourceId);

        ConcurrentMap<Integer, ConcurrentSkipListSet<Integer>> map = linkDelivered.get(originId);
        Set<Integer> set = map.get(messageId);
        set.add(sourceId);

        if (isReadyToDeliver(set.size())) {
            // Deliver this message
            broadcast.receive(originId, messageId);
        } else {
            links.entrySet().parallelStream().filter(e -> {
                int key = e.getKey();
                return key != pid && !map.containsKey(key);
            }).forEach(e -> {
                e.getValue().send(originId, messageId, sourceId);
            });
        }
    }

    public void broadcast(int messageId) {
        links.entrySet().parallelStream().filter(e -> e.getKey() != pid).forEach(e -> {
            e.getValue().send(pid, messageId, pid);
        });
    }
}