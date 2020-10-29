package cs451;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

public class Main {

    private static FifoBroadcast broadcast;

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

        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

        // Init
        CountDownLatch latch = new CountDownLatch(nbMessages * parser.hosts().size());
        Logger logger = Logger.getInstance(parser.output());

        broadcast = new FifoBroadcast(parser.myId(), nbMessages, parser.hosts(), (originId, messageId) -> {
            latch.countDown();
        });

        System.out.println("Waiting for all links for finish initialization");
        coordinator.waitOnBarrier();
        long startTime = System.nanoTime();

        System.out.println("Broadcasting messages...");
        IntStream.range(1, nbMessages + 1).forEach(i -> {
            broadcast.broadcast(i);
        });
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
