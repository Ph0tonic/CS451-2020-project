package cs451.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Simple in memory logger
 *
 * @author Wermeille Bastien
 */
public class InMemoryLogger implements Logger {
    private static InMemoryLogger INSTANCE;
    private final ConcurrentLinkedQueue<String> logs;
    private final String output;

    private InMemoryLogger(String output) {
        this.output = output;
        this.logs = new ConcurrentLinkedQueue<>();

        Runtime.getRuntime().addShutdownHook(new Thread(this::dump));
    }

    public static synchronized InMemoryLogger getInstance(String output) {
        if (INSTANCE == null) {
            INSTANCE = new InMemoryLogger(output);
        }
        return INSTANCE;
    }

    public void log(String log) {
        logs.add(log);
    }

    public void dump() {
        // Print logs
        try {
            FileWriter fw = new FileWriter(output, false);
            BufferedWriter bw = new BufferedWriter(fw);
            logs.forEach(l -> {
                try {
                    bw.write(l);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println("Error while writing logs");
            e.printStackTrace();
        }
    }
}
