package gaiaframework.gaiaagent;

// A thread that reads the file into its blocking queue, until EOF

import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;

public class FileReader implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    String filename;

    LinkedBlockingQueue<DataChunk> dataQueue;

    long totalSize;

    public FileReader(String filename, LinkedBlockingQueue<DataChunk> dataQueue, long totalFileSize) {
        this.filename = filename;
        this.dataQueue = dataQueue;
        this.totalSize = totalFileSize;
    }

    @Override
    public void run() {

        // test if the file exsists
        File datafile = new File(filename);
        File indexfile = new File(filename + ".index");

        if (!datafile.exists()) {
            logger.error("File {} does not exist", filename);
            return;
        }

        if (!indexfile.exists()) {
            logger.error("Index file of {} does not exist", filename);
            return;
        }

        try {
            // first put the indexfile into the blockingqueue
            Path indexPath = Paths.get(filename + ".index");
            byte[] bytes = Files.readAllBytes(indexPath);

            DataChunk firstChunk = new DataChunk(filename, -1, totalSize, bytes);

            dataQueue.put(firstChunk);
            logger.info("Finished reading index file of {}", filename);

            // then read through the large data file

            RandomAccessFile dataRAF = new RandomAccessFile(filename, "r");

            int bufferSize = Constants.CHUNK_SIZE_KB * 1024;
            byte[] dataBytes = new byte[bufferSize];
            int i;
            for (i = 0 ; i <= dataRAF.length() - bufferSize; i += bufferSize){

                logger.info("Reading from {} of {}", i , filename);

                dataRAF.read(dataBytes, i, bufferSize);
                DataChunk midChunk = new DataChunk(filename, i, bufferSize, dataBytes);
                dataQueue.put(midChunk);

            }

            logger.info("Reading the last chunk of {}", filename);
            // the last chunk

            dataRAF.read(dataBytes, i, (int) (dataRAF.length() - i + 1));
            DataChunk lastChunk = new DataChunk(filename, i, (dataRAF.length() - i + 1), dataBytes);
            dataQueue.put(lastChunk);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
