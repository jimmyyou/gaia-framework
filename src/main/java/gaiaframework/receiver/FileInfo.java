package gaiaframework.receiver;

// This class is used for writing into MapOutputFile with index file, at the receiver side
// it needs to handle the re-ordering, etc.

// VER 1.0 we copy the whole map output file over the net, but only copy once for co-located Reducers (i.e. combine the flowgroups)

import gaiaframework.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileInfo {
    private static final Logger logger = LogManager.getLogger();

    public enum FileState{
        NULL,
//        NEW,
        WRITING,
//        PAUSED,
        FIN
    }

    FileState fileState = FileState.NULL;
    RandomAccessFile randomAccessFile;
    String filename;

    long totalChunks; // does not include the first and the last DataChunk
    long totalSize_bytes;

    public FileInfo(String filename, long totalSize_bytes) {
        this.filename = filename;
        this.totalSize_bytes = totalSize_bytes;

        totalChunks = 1 + totalSize_bytes / (Constants.CHUNK_SIZE_KB * 1024);

        logger.info("Created FileInfo for {}, size: {}, chunks: [}", filename, totalSize_bytes, totalChunks);

        // TODO create the RAF

        try {
            randomAccessFile = new RandomAccessFile(filename, "rw");

            randomAccessFile.setLength(totalSize_bytes);

            fileState = FileState.WRITING;
            logger.info("Created RAF {}", filename);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
