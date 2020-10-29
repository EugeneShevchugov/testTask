package reader;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

class CSVReaderTest {
    @Test
    void csvReaderTest() {
        CSVReader reader = new CSVReader("testing.csv", "testing2.csv");
        try {
            reader.execute();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}