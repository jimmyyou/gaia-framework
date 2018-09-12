package gaiaframework.receiver;

import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.SendingAgentServiceGrpc;
import gaiaframework.transmission.DataChunkMessage;
import gaiaframework.util.Constants;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Shared states for receiving agents. Contains:
 * 1. HashMap of FileBlockHandlers
 *
 */

public class ReceivingAgentSharedData {

    private static final Logger logger = LogManager.getLogger();

    ConcurrentHashMap<String, FileBlockHandler> activeFileBlocks = new ConcurrentHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(Constants.WRITER_THREADS);

    private final ManagedChannel grpcChannel;

    public ReceivingAgentSharedData(String masterHostname) {
        // TODO(future) load master port from config
        grpcChannel = ManagedChannelBuilder.forAddress(masterHostname, 23330).usePlaintext().build();
        logger.info("Init RASD, created GPRC channel");

    }

    public void processData(DataChunkMessage dataChunk) {

        // TODO
        executor.submit(new processDataChunkTask(dataChunk));


    }

    public class processDataChunkTask implements Runnable{

        private final DataChunkMessage dataChunk;

        public processDataChunkTask(DataChunkMessage dataChunk) {
            this.dataChunk = dataChunk;
        }

        /**
         * process the dataChunk,
         * In this version we don't care about whether the chunk is the first chunk.
         * Given a chunk, first check the size of the file buffer, then write to the file.
         *
         * First check if it is the first chunk of block, by check concurrentHashMap
         *
         */
        @Override
        public void run() {

            // First Check fileBlocks
            //        logger.info("Processing data {} {} {}\n{} {}", dataChunk.getFilename(), dataChunk.getChunkStartIndex(),
//                dataChunk.getTotalBlockLength(), (int) dataChunk.getData()[0], (int) dataChunk.getData()[1]);

            // DEBUG purpose, No Op when debugging on single host
//            if (!isOutputEnabled) {
//
//                // append .dbg to file name
//                dataChunk.appendToFilename(".dbg");
////            return;
//            }

            String filename = dataChunk.getFilename();
            String handlerId = filename + "-" + dataChunk.getBlockId();

            // TODO new logic here
            // First create the handler if not existent
            long totalBlockLength = dataChunk.getTotalBlockLength();
            logger.info("putIfAbsent for handlerID= {} and file = {}", handlerId, filename);
            
            synchronized (activeFileBlocks){
                if(!activeFileBlocks.containsKey(handlerId)){
                    activeFileBlocks.put(handlerId, new FileBlockHandler(filename, totalBlockLength));
                }
            }
//            activeFileBlocks.putIfAbsent(handlerId, new FileBlockHandler(filename, totalBlockLength));

            // Then write data and check state
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


        }

        /** Send FILE_FIN message, retry when failed. We must fork a thread here so that we can unblock
         *
         * @param filename
         */
        void sendFileFIN_WithRetry(String filename) {
//        long startTime = System.nanoTime();
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
//        long deltaTime = System.nanoTime() - startTime;
        }

        /**
         * Async send FileFIN. Don't need to explicitly wait for time out exception here.
         * @param filename
         * @throws RuntimeException
         */
        void sendFileFIN(String filename) throws RuntimeException {

            SendingAgentServiceGrpc.SendingAgentServiceStub stub = SendingAgentServiceGrpc.newStub(grpcChannel);
            GaiaMessageProtos.FileFinishMsg request = GaiaMessageProtos.FileFinishMsg.newBuilder().setFilename(filename).build();

//        final CountDownLatch latch = new CountDownLatch(1);
            StreamObserver<GaiaMessageProtos.ACK> responseObserver = new StreamObserver<GaiaMessageProtos.ACK>() {

                @Override
                public void onNext(GaiaMessageProtos.ACK flowStatus_ack) {
//                latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    throw new RuntimeException("error!");
                }

                @Override
                public void onCompleted() {
                }
            };

            stub.finishFile(request, responseObserver);
//        stub.withDeadlineAfter(100, TimeUnit.MILLISECONDS).finishFile(request, responseObserver);

        }

    }

}
