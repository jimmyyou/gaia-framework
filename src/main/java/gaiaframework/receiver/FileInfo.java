package gaiaframework.receiver;

// This class is used for writing into MapOutputFile with index file, at the receiver side
// it needs to handle the re-ordering, etc.

// VER 1.0 we copy the whole map output file over the net, but only copy once for co-located Reducers (i.e. combine the flowgroups)

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.transmission.DataChunkMessage;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileInfo {
    private static final Logger logger = LogManager.getLogger();

    boolean isTotalBytesKnown = false;

    public FileInfo(DataChunkMessage fisrtDataChunk) {
        this.filename = fisrtDataChunk.getFilename();
        this.totalSize_bytes = fisrtDataChunk.getChunkLength() + fisrtDataChunk.getStartIndex();

        logger.info("Created FileInfo for {}, temporary size: {}, chunks: {}", filename, totalSize_bytes, totalChunks);

        try {
            dataFile = new RandomAccessFile(filename, "w");

            dataFile.setLength(totalSize_bytes);

            fileState = FileState.WRITING;
            logger.info("Created RAF {}", filename);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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

    FileState fileState = FileState.NULL;
    RandomAccessFile dataFile;
    String filename;

    long totalChunks; // does not include the first and the last DataChunk
    long totalSize_bytes;

    // TODO use HashTable etc. to track the progress? in case of retransmission. And report the progress back to GAIA controller
    long receivedChunks = 0;
    long receivedBytes = 0;

    public FileInfo(String filename, long knownSize_bytes) {
        this.filename = filename;
        this.totalSize_bytes = knownSize_bytes;

        totalChunks = knownSize_bytes / (Constants.MAX_CHUNK_SIZE_Bytes) + ((knownSize_bytes % (Constants.MAX_CHUNK_SIZE_Bytes)) > 0 ? 1 : 0);

        logger.info("Created FileInfo for {}, size: {}, chunks: {}", filename, knownSize_bytes, totalChunks);

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

    }


    public boolean writeDataAndCheck(DataChunkMessage dataChunk) {

        // first write the data, then check if it is finished

        long startIndex = dataChunk.getStartIndex();
        long chunkLength = dataChunk.getChunkLength();

        if(startIndex == -1){

            logger.info("Received the first Chunk");

            isTotalBytesKnown = true;
            totalSize_bytes = chunkLength;

            try {
                dataFile.setLength(totalSize_bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }

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

//        logger.info("Writing data to {}, [{}:{}]", filename, startIndex, chunkLength);

        try {
            dataFile.seek(startIndex);
            dataFile.write(dataChunk.getData(), 0, (int) chunkLength);
//            dataFile.write(dataChunk.getData(), (int) startIndex, (int) chunkLength);

            // check the overall progress for this file.
//            receivedChunks++;
            receivedBytes += dataChunk.getData().length;

            logger.info("File {}, progress {} / {} ({})", filename, receivedBytes, totalSize_bytes, startIndex);

            // FIXME how to support multipath?
            if (receivedBytes >= totalSize_bytes){
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
