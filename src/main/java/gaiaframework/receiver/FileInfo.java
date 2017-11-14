package gaiaframework.receiver;

// This class is used for writing into MapOutputFile with index file, at the receiver side
// it needs to handle the re-ordering, etc.

// VER 1.0 we copy the whole map output file over the net, but only copy once for co-located Reducers (i.e. combine the flowgroups)

import gaiaframework.gaiaagent.DataChunk;
import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileInfo {
    private static final Logger logger = LogManager.getLogger();

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

    public FileInfo(String filename, long totalSize_bytes) {
        this.filename = filename;
        this.totalSize_bytes = totalSize_bytes;

        totalChunks = totalSize_bytes / (Constants.CHUNK_SIZE_KB * 1024) + ((totalSize_bytes % (Constants.CHUNK_SIZE_KB * 1024)) > 0 ? 1 : 0);

        logger.info("Created FileInfo for {}, size: {}, chunks: {}", filename, totalSize_bytes, totalChunks);

        // create the RAF
        try {
            dataFile = new RandomAccessFile(filename, "rw");

            dataFile.setLength(totalSize_bytes);

            fileState = FileState.WRITING;
            logger.info("Created RAF {}", filename);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public boolean writeDataAndCheck(DataChunk dataChunk) {

        // first write the data, then check if it is finished

        long startIndex = dataChunk.getStartIndex();
        long chunkLength = dataChunk.getChunkLength();

//        logger.info("Writing data to {}, [{}:{}]", filename, startIndex, chunkLength);

        // FIXME how to support files larger than 2GB?
        try {
            dataFile.seek(startIndex);
            dataFile.write(dataChunk.getData(), 0, (int) chunkLength);
//            dataFile.write(dataChunk.getData(), (int) startIndex, (int) chunkLength);

            // check the overall progress for this file.
            receivedChunks++;

            logger.info("File {}, progress {} / {} ({})", filename, receivedChunks, totalChunks, startIndex);

            if (receivedChunks >= totalChunks) {
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
