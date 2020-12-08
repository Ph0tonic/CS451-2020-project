package cs451;

import cs451.broadcast.MultiplexedBroadcast;
import cs451.broadcast.MultiplexedBroadcastFactory;
import cs451.broadcast.MultiplexedBroadcastReceive;
import cs451.config.Coordinator;
import cs451.config.Host;
import cs451.config.Parser;
import cs451.utils.InMemoryLogger;
import cs451.utils.Logger;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.Random;

public class Main {

    private static final String SPACE = " ";
    private static final String DELIVER = "d ";
    private static final String BROADCAST = "b ";
    private static MultiplexedBroadcast broadcast;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        if (broadcast != null) {
            broadcast.stop();
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

        int nbMessage = 0;
        int[][] causals = null;
        int nbHost = parser.hosts().size();
        boolean isCausalBroadcast = false;

        // Config parsing
        try {
            FileInputStream fis = new FileInputStream(parser.config());
            Scanner sc = new Scanner(fis);

            nbMessage = Integer.parseInt(sc.nextLine());
            causals = new int[nbHost][];

            int i = 1;
            while (sc.hasNextLine() && i <= nbHost) {
                int[] causal = Arrays.stream(sc.nextLine().split(" ")).mapToInt(Integer::parseInt).toArray();
                if (i == 1 && causal.length == 0) {
                    // Fifo broadcast
                    break;
                }
                isCausalBroadcast = true;

                if (causal.length < 1 || causal[0] != i) {
                    if (i == nbHost + 1 && causal.length < 1) {
                        // Specific case when the file ends with an empty line
                        break;
                    }
                    throw new Exception("Error - Invalid config file");
                }
                causals[i - 1] = Arrays.copyOfRange(causal, 1, causal.length);
                i++;
            }
            if (isCausalBroadcast && i != nbHost + 1) {
                throw new Exception("Invalid config file");
            }
            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }

        System.out.println("Nb messages : " + nbMessage);
        System.out.println("Operation mode: " + (isCausalBroadcast ? "Causal" : "Fifo"));
        if (isCausalBroadcast) {
            for (int i = 0; i < nbHost; ++i) {
                System.out.println((i + 1) + " " + Arrays.toString(causals[i]));
            }
        }

        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

        // We assume a majority of correct processes
        CountDownLatch latch = new CountDownLatch(nbMessage * (nbHost / 2));
        CountDownLatch ownLatch = new CountDownLatch(nbMessage);
        System.out.println("Waiting on " + (nbMessage * (nbHost / 2 + 1)) + " messages");

        Logger logger = InMemoryLogger.getInstance(parser.output());

        int id = parser.myId();

        MultiplexedBroadcastReceive receiver = (originId, messageId) -> {
            logger.log(DELIVER + originId + SPACE + messageId);
            if (originId == id) {
                ownLatch.countDown();
            } else {
                latch.countDown();
            }
        };

        try {
            if (isCausalBroadcast) {
                broadcast = MultiplexedBroadcastFactory.createCausalBroadcast(receiver, parser, nbMessage, causals);
            } else {
                broadcast = MultiplexedBroadcastFactory.createFifoBroadcast(receiver, parser, nbMessage);
            }

        } catch (Exception e) {
            return;
        }

        System.out.println("Waiting for all links to finish initialization");
        coordinator.waitOnBarrier();
        long startTime = System.nanoTime();

        System.out.println("Broadcasting messages...");
        for (int i = 1; i <= nbMessage; ++i) {
            logger.log(BROADCAST + i);
            broadcast.broadcast(id, i);
        }
        broadcast.flush();

        ownLatch.await();
        latch.await();

        System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();
        long endTime = System.nanoTime();
        System.out.println("Escaped time : " + (endTime - startTime) / 1000000 + "[ms]");

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
