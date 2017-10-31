package gaiaframework.receiver;

// A thread that writes the received data into files
// maintains a pool of RandomAccessFile to write into

import gaiaframework.gaiaagent.DataChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FileWriter implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    RandomAccessFile randomAccessFile;

    LinkedBlockingQueue<DataChunk> dataQueue;

    HashMap <String, FileInfo> activeFiles = new HashMap<String, FileInfo>();

    public FileWriter(LinkedBlockingQueue<DataChunk> dataQueue) {
        this.dataQueue = dataQueue;
    }

    @Override
    public void run() {
        logger.info("Filewriter Thread started");

        DataChunk dataChunk;
        while (true) {
            try {
                dataChunk = dataQueue.take();

                processData(dataChunk);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * process the dataChunk, if startOffset == -1, the start index is the total size of the file
     * @param dataChunk
     */
    private void processData(DataChunk dataChunk) {

        if (dataChunk.getStartIndex() == -1){
            createFileandIndex(dataChunk);
        }
        else if (dataChunk.getChunkLength() == 0){
            closeFile();

        }
        else {
            writeToFile();
        }

    }

    private void createFileandIndex(DataChunk dataChunk) {
        // first check if file exists
        String filename = dataChunk.getFilename();
        File datafile = new File(filename);
        File indexfile = new File(filename + ".index");

        if (datafile.exists()) {
            logger.error("File {} exists", filename);
            return;
        }

        if (indexfile.exists()) {
            logger.error("Index file of {} exists", filename);
            return;
        }

        // continue to create the File, and put into the map
        logger.info("Creating file and index for {}", filename);

        FileInfo fileInfo = new FileInfo(filename, dataChunk.getChunkLength());

        activeFiles.put(filename, fileInfo);



    }

    private void writeToFile() {

    }

    private void closeFile() {

    }
}
