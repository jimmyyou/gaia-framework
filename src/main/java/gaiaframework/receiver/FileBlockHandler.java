package gaiaframework.receiver;

// This class is used for writing into MapOutputFile with index file, at the receiver side
// it needs to handle the re-ordering, etc.

// VER 1.0 we copy the whole map output file over the net, but only copy once for co-located Reducers (i.e. combine the flowgroups)

import gaiaframework.transmission.DataChunkMessage;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileBlockHandler {
    private static final Logger logger = LogManager.getLogger();

    boolean isTotalBytesKnown = false;

    FileState fileState = FileState.NULL;
    RandomAccessFile dataFile;
    String filename;

    long totalChunks; // does not include the first and the last DataChunk
    long totalSize_bytes;

    // TODO(future) use HashTable etc. to track the progress? in case of retransmission. And report the progress back to GAIA controller
    long receivedChunks = 0;
    long receivedBytes = 0;
    long currentLength = 0;

    // Change to fitting not necessarily the first chunk of the block
    public FileBlockHandler(DataChunkMessage dataChunk) {
        this.filename = dataChunk.getFilename();
        this.totalSize_bytes = dataChunk.getTotalBlockLength();

        logger.info("Created FileBlockHandler for {}, temporary size: {}, chunks: {}", filename, totalSize_bytes, totalChunks);

        try {

            // first try to detect and create the folder, taken care of in FileWriter.java
            dataFile = new RandomAccessFile(filename, "rw");

            fileState = FileState.WRITING;
            logger.info("Created RAF {}", filename);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public enum FileState {
        NULL,
        //        NEW,
        WRITING,
        //        PAUSED,
        FIN
    }
/*
    public FileBlockHandler(String filename, long knownSize_bytes) {
        this.filename = filename;
        this.totalSize_bytes = knownSize_bytes;

        totalChunks = knownSize_bytes / (Constants.MAX_CHUNK_SIZE_Bytes) + ((knownSize_bytes % (Constants.MAX_CHUNK_SIZE_Bytes)) > 0 ? 1 : 0);

        logger.info("Created FileBlockHandler for {}, size: {}, chunks: {}", filename, knownSize_bytes, totalChunks);

        // create the RAF
        try {
            dataFile = new RandomAccessFile(filename, "rw");

            dataFile.setLength(knownSize_bytes);

            fileState = FileState.WRITING;
            logger.info("Created RAF {}", filename);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }*/


    /**
     * Upon receiving the dataChunk, write it, and check if we have finished.
     *
     * @param dataChunk
     * @return true if this is the last chunk
     */
    public boolean writeDataAndCheck(DataChunkMessage dataChunk) {

        // first write the data, then check if it is finished
        long startTime = System.currentTimeMillis();

        long startIndex = dataChunk.getStartIndex();

        try {
            checkAndChangeFileSize(dataChunk);
        } catch (IOException e) {
            e.printStackTrace();
        }

/*
        if(startIndex == -1){

            logger.info("Received the first Chunk");

            isTotalBytesKnown = true;
            totalSize_bytes = chunkLength;


            startIndex = 0;
            chunkLength = dataChunk.getData().length;
        }
        else {

            if (startIndex + chunkLength > totalSize_bytes) {

                logger.info("Enlarging file size from {} to {}", totalSize_bytes, startIndex + chunkLength);
                // Need to enlarge the file!!!
                totalSize_bytes = startIndex + chunkLength;

                try {
                    dataFile.setLength(totalSize_bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
*/

//        logger.info("Writing data to {}, [{}:{}]", filename, startIndex, chunkLength);

        try {
            dataFile.seek(startIndex);
            dataFile.write(dataChunk.getData());
//            dataFile.write(dataChunk.getData(), (int) startIndex, (int) chunkLength);

            // check the overall progress for this file.
//            receivedChunks++;
            receivedBytes += dataChunk.getData().length;

            long deltaTime = System.currentTimeMillis() - startTime;
            logger.info("File {}, progress {} / {} off: {} chunksize: {} took {} ms", filename, receivedBytes,
                    totalSize_bytes, startIndex, dataChunk.getData().length, deltaTime);

            // how to support multipath?
            if (receivedBytes >= totalSize_bytes) {
                finishAndClose();
                return true;
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;

    }

    private void finishAndClose() throws IOException {
        dataFile.close();
        logger.info("Finishing FileBlock {}", this.filename);
    }

    private void checkAndChangeFileSize(DataChunkMessage dataChunk) throws IOException {
        // current file length
        long reqLength = dataChunk.getStartIndex() + dataChunk.getData().length;

        // guarantee that the output file size is the same
        if (reqLength < dataChunk.getTotalFileLength()) {
            reqLength = dataChunk.getTotalFileLength();
        }

        if (currentLength < reqLength) {
            currentLength = reqLength;
            dataFile.setLength(currentLength);
        }
    }


/*    class BlockInfo {
        long startOffSet;
        long endOffSet;

        public BlockInfo(long startOffSet, long endOffSet) {
            this.startOffSet = startOffSet;
            this.endOffSet = endOffSet;
        }

        public long getStartOffSet() {
            return startOffSet;
        }

        public long getEndOffSet() {
            return endOffSet;
        }
    }

    BlockInfo blockInfo;*/


}
