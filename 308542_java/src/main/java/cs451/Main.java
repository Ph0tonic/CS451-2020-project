package cs451;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    // private static FifoBroadcast broadcast;
    private static PerfectLinks links;

    private static String SPACE = " ";
    private static String DELIVER = "d ";
    private static String BROADCAST = "b ";

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
//        if (broadcast != null) {
//            broadcast.stop();
//        }
        if (links != null) {
            links.stop();
        }

        //write/flush output file if necessary
        System.out.println("Writing output.");
        //Configured inside the logger
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID is " + pid + ".");
        System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");

        System.out.println("My id is " + parser.myId() + ".");
        System.out.println("List of hosts is:");
        for (Host host : parser.hosts()) {
            System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
        }

        System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
        System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
        System.out.println("Output: " + parser.output());
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
        }

        int nbMessages = 0;

        try {
            //the file to be opened for reading
            FileInputStream fis = new FileInputStream(parser.config());
            Scanner sc = new Scanner(fis);    //file to be scanned

            nbMessages = Integer.parseInt(sc.nextLine());

            sc.close();     //closes the scanner
        } catch (IOException e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }

        System.out.println("Nb messages : " + nbMessages);

        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

        // Init
        //TODO: Make a final choice
        CountDownLatch latch = new CountDownLatch(nbMessages); // * parser.hosts().size());
        Logger logger = Logger.getInstance(parser.output());

        final AtomicInteger j = new AtomicInteger(0);
        int id = parser.myId();

        // broadcast = new FifoBroadcast(parser.myId(), nbMessages, parser.hosts(), (originId, messageId) -> {
        //     logger.log(DELIVER + originId + SPACE + messageId);
        //     // System.out.println("Fifo deliver " + originId + " " + messageId);
        //     if (originId == id) {
        //         j.set(j.get() + 1);
        //         // System.out.println("Release " + j.get());
        //         latch.countDown();
        //     } else {
        //         // System.out.println("Fifo NOT EQUAL " + originId + " " + id);
        //     }
        // });

        Optional<Host> host = parser.hosts().stream().filter(x -> x.getId() == id).findFirst();

        links = new PerfectLinks(parser.hosts(), 1, id);
        UdpSocket socket = new UdpSocket(host.get().getIp(), host.get().getPort(), links, parser.hosts());
//        UdpSocket socket = new UdpSocket(host.get().getIp(), host.get().getPort(), message -> {
////            System.out.println("Random release ?");
//            queue.add(message);
//            if (message.messageId == 500) {
//                System.out.println("Release " + message.originId + " " + queue.size());
//            }
//            latch.countDown();
////            System.out.println("End Release");
//        }, parser.hosts());

        links.init(socket, (originId, messageId, sourceId) -> {
            if (messageId % 100 == 1) {
//                System.out.println("Release ");
            }
//            System.out.println("Message "+originId+" "+messageId+" "+sourceId);
            if (originId == id) {
                j.incrementAndGet();
                if (messageId % 1000 == 1) {
                    System.out.println("I " + j.get());
                }
                latch.countDown();
            }
        });

        System.out.println("Waiting for all links to finish initialization");
        coordinator.waitOnBarrier();
        long startTime = System.nanoTime();

        System.out.println("Broadcasting messages...");
        for (int i = 1; i <= nbMessages; ++i) {
//            System.out.println("My id is " + id);
//            if (i % 100 == 1) {
//                logger.log(BROADCAST + i);
//            }
            // broadcast.broadcast(i);
            int dest = id == 1 ? 2 : 1;
            links.send(id, i, id, dest);
//            System.out.println(id + " " + i + " " + id + " " + dest + " " + false);
//            socket.send(new Message(id, i, id, dest, false));
//            System.out.println(id + " " + i + " " + id + " " + dest + " " + false);
        }
        latch.await();

        System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();
        long endTime = System.nanoTime();
        System.out.println("Escaped time : " + (endTime - startTime) / 1000000 + "[ms]");

        logger.dump();

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
