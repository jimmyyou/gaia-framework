package gaiaframework.gaiaagent;

// A thread that reads the file into its blocking queue, until EOF

import gaiaframework.transmission.DataChunk;
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

public class LocalFileReader implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    String filename;

    LinkedBlockingQueue<DataChunk> dataQueue;

    long totalSize;

    public LocalFileReader(String filename, LinkedBlockingQueue<DataChunk> dataQueue, long totalFileSize) {
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

            // read size info from large data file
            RandomAccessFile dataRAF = new RandomAccessFile(filename, "r");

            // totalSize is not correct here
//            DataChunk firstChunk = new DataChunk(srcFilename, -1, totalSize, bytes);
            DataChunk firstChunk = new DataChunk(filename, -1, dataRAF.length(), bytes);

            dataQueue.put(firstChunk);
            logger.info("Finished reading index file of {}", filename);

            // then read through the large data file

            int bufferSize = Constants.MAX_CHUNK_SIZE_Bytes;
            byte[] dataBytes;
            int i;
            for (i = 0; i < dataRAF.length() - bufferSize; i += bufferSize) {

                dataBytes = new byte[bufferSize];
                logger.info("Reading from {} of {}", i, filename);
                dataRAF.seek(i);
                dataRAF.read(dataBytes, 0, bufferSize);
                DataChunk midChunk = new DataChunk(filename, i, bufferSize, dataBytes);
                dataQueue.put(midChunk);

            }

            logger.info("Reading the last chunk of {}, from {}", filename, i);
            // the last chunk

            dataRAF.seek(i);
            dataBytes = new byte[bufferSize];
            dataRAF.read(dataBytes, 0, (int) (dataRAF.length() - i));
            DataChunk lastChunk = new DataChunk(filename, i, (dataRAF.length() - i), dataBytes);
            dataQueue.put(lastChunk);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
