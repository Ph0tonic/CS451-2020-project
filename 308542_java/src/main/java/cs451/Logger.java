package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Logger {
    private static Logger INSTANCE;

    public static synchronized Logger getInstance(String output){
        if (INSTANCE == null) {
            INSTANCE = new Logger(output);
        }
        return INSTANCE;
    }

    private ConcurrentLinkedQueue<String> logs;
    private String output;

    private Logger(String output){
        this.output = output;
        this.logs = new ConcurrentLinkedQueue<>();

        Runtime.getRuntime().addShutdownHook(new Thread(this::dump));
    }

    public void log(String log){
        System.out.println(log);
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
