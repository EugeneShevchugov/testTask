package reader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

public class CSVReader {
    private static final String CSV_SPLIT_BY = ";";
    private final Map<String, Set<String>> fileLines = new ConcurrentHashMap<>();
    private final Queue<FutureTask<Boolean>> futures;
    private final String[] csvs;

    public CSVReader(final String... csvs) {
        futures = new ConcurrentLinkedQueue<>();
        this.csvs = csvs;
    }

    public void execute() throws ExecutionException, InterruptedException {
        for (final String csv : csvs) {
            FutureTask<Boolean> task = new FutureTask<>(() -> {
                Thread.sleep(10000);
                readCSV(csv);
                return true;
            });
            Thread thread = new Thread(task);
            futures.add(task);
            thread.start();
        }
        waitForThreadToFinish();
        writeToFiles();
        waitForThreadToFinish();
    }

    private void waitForThreadToFinish() throws ExecutionException, InterruptedException {
        while (!futures.isEmpty()) {
            futures.poll().get();
        }
    }

    private void readCSV(final String csv) {
        try {
            List<String> lines = Files.readAllLines(new File(csv).toPath(), StandardCharsets.UTF_8);
            final String[] text = getText(lines, 0);
            for (int j = 1; j < lines.size(); j++) {
                String[] line = lines.get(j).split(CSV_SPLIT_BY);
                for (int i = 0; i < text.length; i++) {
                    fileLines.putIfAbsent(text[i], new ConcurrentSkipListSet<>());
                    fileLines.get(text[i]).add(line[i]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToFiles() {
        for (final String keys : fileLines.keySet()) {
            FutureTask<Boolean> futureTask = createFutureTaskForWriting(keys, fileLines.get(keys));
            futures.add(futureTask);
            new Thread(futureTask).start();
        }
    }

    private FutureTask<Boolean> createFutureTaskForWriting(final String fileName, final Set<String> strings) {
        return new FutureTask<>(() -> {
            File file = new File(fileName + ".txt");
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write(fileName + ":\n");
                strings.forEach(n -> {
                    try {
                        writer.append(n).append(";");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        });
    }

    private String[] getText(final List<String> lines, final int index) {
        return lines.get(index).split(CSV_SPLIT_BY);
    }
}
