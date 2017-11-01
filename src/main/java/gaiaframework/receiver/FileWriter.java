package gaiaframework.receiver;

// A thread that writes the received data into files
// maintains a pool of RandomAccessFile to write into

// TODO use multiple threads to parallelly handle I/O

import gaiaframework.gaiaagent.DataChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FileWriter implements Runnable{
    private static final Logger logger = LogManager.getLogger();

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
        else {
            writeToFile(dataChunk);
        }
/*        else if (dataChunk.getChunkLength() == 0){
            closeFile(dataChunk);
        }
        else {
            writeToFile(dataChunk);
        }*/

    }

    // upon receiving the first chunk, create and write to index file, also create data file.
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

        // create and write to the index file

        try {
            FileOutputStream fos = new FileOutputStream(filename + ".index");

            fos.write(dataChunk.getData());
            fos.flush();
            fos.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void writeToFile(DataChunk dataChunk) {
        String filename = dataChunk.getFilename();
        FileInfo fileInfo = activeFiles.get(filename);

        if (fileInfo != null) {
            boolean isFinished = fileInfo.writeDataAndCheck(dataChunk);

            if (isFinished) {
                logger.info("File {} finished", filename);

                activeFiles.remove(filename);
            }

        }
        else {
            logger.error("Received dataChunk for inactive file {}", dataChunk.getFilename());
        }

    }

}
