package gaiaframework.receiver;

// A thread that writes the received data into files
// maintains a pool of RandomAccessFile to write into

// TODO use multiple threads to parallelly handle I/O

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.transmission.DataChunkMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FileWriter implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    LinkedBlockingQueue<DataChunkMessage> dataQueue;

    HashMap<String, FileInfo> activeFiles = new HashMap<String, FileInfo>();

    boolean isOutputEnabled = true;

    public FileWriter(LinkedBlockingQueue<DataChunkMessage> dataQueue, boolean isOutputEnabled) {
        this.dataQueue = dataQueue;
        this.isOutputEnabled = isOutputEnabled;
    }

    @Override
    public void run() {
        logger.info("Filewriter Thread started");

        DataChunkMessage dataChunk;
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
     *
     * @param dataChunk
     */
    private void processData(DataChunkMessage dataChunk) {

//        logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getStartIndex(),
//                dataChunk.getChunkLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

        // DEBUG purpose, No Op when debugging on single host
        if (!isOutputEnabled) {

            // append .dbg to file name
            dataChunk.appendToFilename(".dbg");
//            return;
        }

        // TODO change the logic of writing to file

        if (activeFiles.containsKey(dataChunk.getFilename())){
            writeToFile(dataChunk);
        }
        else {
            CreateFile_Spec(dataChunk);
        }

/*        if (dataChunk.getStartIndex() == -1) {
            NaiveCreateFile(dataChunk);
//            createFileandIndex(dataChunk);
        } else {
            writeToFile(dataChunk);
        }*/
/*        else if (dataChunk.getChunkLength() == 0){
            closeFile(dataChunk);
        }
        else {
            writeToFile(dataChunk);
        }*/

    }

    private void CreateFile_Spec(DataChunkMessage dataChunk) {
        String filename = dataChunk.getFilename();
        File datafile = new File(filename);

        if (datafile.exists()) {
            logger.error("File {} exists", filename);
            return;
        }

        // continue to create the File, and put into the map
        logger.info("Creating file and dir for {}", filename);

        File dir = datafile.getParentFile();

        if(!dir.exists()){

            dir.mkdir();

        }

        FileInfo fileInfo = new FileInfo(dataChunk);

        boolean done = fileInfo.writeDataAndCheck(dataChunk);

        if (!done) {
            activeFiles.put(filename, fileInfo);
        }

    }

    private void NaiveCreateFile(DataChunkMessage dataChunk) {
        String filename = dataChunk.getFilename();
        File datafile = new File(filename);

        if (datafile.exists()) {
            logger.error("File {} exists", filename);
            return;
        }

        // continue to create the File, and put into the map
        logger.info("Creating file and index for {}", filename);

        FileInfo fileInfo = new FileInfo(filename, dataChunk.getChunkLength());

        activeFiles.put(filename, fileInfo);

    }

    // upon receiving the first chunk, create and write to index file, also create data file.
    private void createFileandIndex(DataChunkMessage dataChunk) {
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


        // TODO WRONG ChunkLength
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

    private void writeToFile(DataChunkMessage dataChunk) {
        String filename = dataChunk.getFilename();
        FileInfo fileInfo = activeFiles.get(filename);

        if (fileInfo != null) {
            boolean isFinished = fileInfo.writeDataAndCheck(dataChunk);

            if (isFinished) {
                logger.info("File {} finished", filename);

                activeFiles.remove(filename);
            }

        } else {
            logger.error("Received dataChunk for inactive file {}", dataChunk.getFilename());
        }

    }

}
