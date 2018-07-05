package gaiaframework.receiver;

// A thread that writes the received data into files
// maintains a pool of RandomAccessFile to write into

// TODO use multiple threads to parallelly handle I/O

import com.google.common.util.concurrent.Uninterruptibles;
import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.MasterServiceGrpc;
import gaiaframework.transmission.DataChunkMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileWriter implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    LinkedBlockingQueue<DataChunkMessage> dataChunkQueue;

    HashMap<String, FileBlockHandler> activeFileBlocks = new HashMap<String, FileBlockHandler>();

    boolean isOutputEnabled = true;
    private ManagedChannel grpcChannel;

    public FileWriter(LinkedBlockingQueue<DataChunkMessage> dataChunkQueue, boolean isOutputEnabled) {
        this.dataChunkQueue = dataChunkQueue;
        this.isOutputEnabled = isOutputEnabled;
    }

    @Override
    public void run() {

        // FIXME not use hardcoded in the future
        grpcChannel = ManagedChannelBuilder.forAddress("dc1master", 8888).usePlaintext().build();

        logger.info("Filewriter Thread started, and grpcChannel established");

        DataChunkMessage dataChunk;
        while (true) {
            try {
                dataChunk = dataChunkQueue.take();

                processData(dataChunk);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * process the dataChunk,
     * In this version we don't care about whether the chunk is the first chunk.
     * Given a chunk, first check the size of the file buffer, then write to the file.
     *
     * @param dataChunk
     */
    private void processData(DataChunkMessage dataChunk) {

//        logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getChunkStartIndex(),
//                dataChunk.getTotalBlockLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

        // DEBUG purpose, No Op when debugging on single host
        if (!isOutputEnabled) {

            // append .dbg to file name
            dataChunk.appendToFilename(".dbg");
//            return;
        }

        String filename = dataChunk.getFilename();
        String handlerId = filename + "-" + dataChunk.getBlockId();

        if (activeFileBlocks.containsKey(handlerId)) {
            FileBlockHandler fileBlockHandler = activeFileBlocks.get(handlerId);

            if (fileBlockHandler != null) {
                boolean isFinished = fileBlockHandler.writeDataAndCheck(dataChunk);
                if (isFinished) {
                    activeFileBlocks.remove(handlerId);
                    sendFileFIN_WithRetry(filename);
                }

            } else {
                logger.error("Received dataChunk for file {}, but FileBlockHandler == null", dataChunk.getFilename());
            }
        } else { // We don't have the handler, so we check whether the file exists
            boolean created = CreateOrOpenFile_Spec(handlerId, dataChunk);
            logger.info("dataChunk file created = {}, start={}, blen={}, flen={}", created, dataChunk.getStartIndex(), dataChunk.getTotalBlockLength(), dataChunk.getTotalFileLength());

            // And then creat the handler
            FileBlockHandler fileBlockHandler = new FileBlockHandler(dataChunk);
            boolean finished = fileBlockHandler.writeDataAndCheck(dataChunk);
            if (!finished) {
                activeFileBlocks.put(handlerId, fileBlockHandler);
            } else {
                // TODO Send out FIN
                sendFileFIN_WithRetry(filename);
            }
        }

    }

    void sendFileFIN_WithRetry(String filename){
        boolean retry = true;
        while (retry) {
            try {
                sendFileFIN(filename);
                logger.info("Sent File_FIN {}", filename);
            } catch (RuntimeException e) {
                e.printStackTrace();
                logger.warn("WARN: retry on File_FIN {}", filename);
            } finally {
                retry = false;
            }
        }
    }

    void sendFileFIN(String filename) throws RuntimeException{
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(grpcChannel);
        GaiaMessageProtos.FileFinishMsg request = GaiaMessageProtos.FileFinishMsg.newBuilder().setFilename(filename).build();

        final CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GaiaMessageProtos.FlowStatus_ACK> responseObserver = new StreamObserver<GaiaMessageProtos.FlowStatus_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FlowStatus_ACK flowStatus_ack) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };

        stub.finishFile(request, responseObserver);

        if (!Uninterruptibles.awaitUninterruptibly(latch, 3, TimeUnit.SECONDS)) {
            throw new RuntimeException("timeout!");
        }
    }


    // Try to create/open the file and write data and put handler into HashMap
    private boolean CreateOrOpenFile_Spec(String handlerId, DataChunkMessage dataChunk) {
        String filename = dataChunk.getFilename();
        File datafile = new File(filename);

        if (datafile.exists()) {
            logger.debug("File {} exists", filename);
            return false;
        } else {

            // create the File, and put into the map
            logger.info("Creating file and dir for {}", filename);
            File dir = datafile.getParentFile();
            if (!dir.exists()) {
                logger.info("Creating dir {}, success = {}", dir, dir.mkdirs());
            } else {
                logger.info("Dir {} exists", dir);
            }

            return true;
        }
    }

/*    private void NaiveCreateFile(DataChunkMessage dataChunk) {
        String filename = dataChunk.getFilename();
        File datafile = new File(filename);

        if (datafile.exists()) {
            logger.error("File {} exists", filename);
            return;
        }

        // continue to create the File, and put into the map
        logger.info("Creating file and index for {}", filename);

        FileBlockHandler fileBlockHandler = new FileBlockHandler(filename, dataChunk.getTotalBlockLength());

        activeFileBlocks.put(filename, fileBlockHandler);

    }*/

    // upon receiving the first chunk, create and write to index file, also create data file.
/*    private void createFileandIndex(DataChunkMessage dataChunk) {
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
        FileBlockHandler fileBlockHandler = new FileBlockHandler(filename, dataChunk.getTotalBlockLength());

        activeFileBlocks.put(filename, fileBlockHandler);

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

    }*/

}
