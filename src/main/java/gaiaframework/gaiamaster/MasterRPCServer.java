package gaiaframework.gaiamaster;

// receive the status update message from the clients and put them into a queue for serialization

import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.MasterServiceGrpc;
import gaiaframework.util.Configuration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class MasterRPCServer {
    private static final Logger logger = LogManager.getLogger();

    private Server server;
    int port;
    MasterSharedData masterSharedData;
    private int srMaxSize = 0;

    public MasterRPCServer(Configuration config, MasterSharedData masterSharedData) {
        this.port = config.getMasterPort();
        this.masterSharedData = masterSharedData;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MasterServiceImpl())
                .build()
                .start();
        logger.info("gRPC Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down master gRPC server since JVM is shutting down");
                MasterRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        }); // end of Shutdown Hook

    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    class MasterServiceImpl extends MasterServiceGrpc.MasterServiceImplBase {

        @Override
        public io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FlowStatusReport> updateFlowStatus(
                io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FlowStatus_ACK> responseObserver) {

            return new StreamObserver<GaiaMessageProtos.FlowStatusReport>() {
                @Override
                public void onNext(GaiaMessageProtos.FlowStatusReport statusReport) {
//                    int srSize = statusReport.getSerializedSize();
//                    srMaxSize = srSize > srMaxSize ? srSize : srMaxSize;
//                    logger.debug("Received Flow Status, size: {} / {}\ncontent: {}" , srSize , srMaxSize , statusReport);
                    handleFlowStatusReport(statusReport);
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("ERROR in handling flow status report: {}", t.toString());
                }

                @Override
                public void onCompleted() {
                    GaiaMessageProtos.FlowStatus_ACK ack = GaiaMessageProtos.FlowStatus_ACK.newBuilder().build();

                    responseObserver.onNext(ack);
                    responseObserver.onCompleted();
                }
            };

        }

        @Override
        public void updatePathStatus(gaiaframework.gaiaprotos.GaiaMessageProtos.PathStatusReport request,
                                     io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.PathStatus_ACK> responseObserver) {
            handlePathUpdate(request);

            responseObserver.onNext(GaiaMessageProtos.PathStatus_ACK.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void finishFGFile(gaiaframework.gaiaprotos.GaiaMessageProtos.FlowStatusReport request,
                                 io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.ACK> responseObserver) {
            handleFGFileFIN(request);

            responseObserver.onNext(GaiaMessageProtos.ACK.getDefaultInstance());
            responseObserver.onCompleted();
        }

//        @Override
//        public void finishFile(gaiaframework.gaiaprotos.GaiaMessageProtos.FileFinishMsg request,
//                               io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FlowStatus_ACK> responseObserver) {
//
//            handleFinishFile(request);
//            responseObserver.onNext(GaiaMessageProtos.FlowStatus_ACK.getDefaultInstance());
//            responseObserver.onCompleted();
//        }

    } // End of MasterServiceImpl

    /**
     * Handle FG_FILE_FIN sent from agents.
     *
     * @param request
     */
    private void handleFGFileFIN(GaiaMessageProtos.FlowStatusReport request) {
        logger.info("Received Status Report for FG_FILE_FIN, content {}", request);
        String fgID = request.getStatus(0).getId();
        FlowGroup fg = masterSharedData.getFlowGroup(fgID);
        Coflow cf = masterSharedData.coflowPool.get(fg.getOwningCoflowID());
        logger.info("Found FlowGroup {} and Coflow {} for FG_FILE_FIN of {}", fg, fgID);

        // Reuse the old onFGFileFIN here. Should be OK.
        masterSharedData.onFGFileFIN(fg, cf);
    }

    private void handlePathUpdate(GaiaMessageProtos.PathStatusReport request) {

        masterSharedData.onLinkChange(request);
//        if (request.getIsBroken()){
//            int pathID = request.getPathID();
//            String saID = request.getSaID();
//            String raID = request.getRaID();
//
////            masterSharedData.onLinkDown(saID, raID, pathID);
//            masterSharedData.onLinkDown(request);
//
//        }
//        else {
//            int pathID = request.getPathID();
//            String saID = request.getSaID();
//            String raID = request.getRaID();
//
////            masterSharedData.onLinkUp(saID, raID, pathID);
//        }
    }

    public void handleFlowStatusReport(GaiaMessageProtos.FlowStatusReport statusReport) {

        for (GaiaMessageProtos.FlowStatusReport.FlowStatus status : statusReport.getStatusList()) {
            // first get the current flowGroup ID
            String fid = status.getId();
            if (status.getFinished()) {
                onFinishFlowGroup(fid, System.currentTimeMillis());
                continue;
            } else {
                // set the transmitted.
                FlowGroup fg = masterSharedData.getFlowGroup(fid);
                if (fg != null) {
                    fg.setTransmitted(status.getTransmitted());
                } else {
                    logger.warn("Received status report but the FlowGroup {} does not exist", fid);
                }
            }
        }

    }

    private void onFinishFlowGroup(String fid, long timestamp) {

        logger.info("Received FLOW_FIN for {}", fid);
        // set the current status

        // FIXME Moved to onFileFinish
        masterSharedData.onFinishFlowGroup(fid, timestamp);

    }

    /**
     * Invoked when File Transfer of a fg is finished, search for the fg and invoke fg finish.
     *
     * @param filename
     */
    @Deprecated
    synchronized void onFileFinish(String filename) {
        int off1 = filename.lastIndexOf('-');
        int off0 = filename.lastIndexOf('-', off1 - 1);
        int off2 = filename.lastIndexOf('.');
        String reducerID = filename.substring(off0 + 1, off1);
        String origFilename = filename.substring(0, off0).concat(".data");

        // Find the FG in the CFPool and then finish the FG
        if (masterSharedData.fileNametoCoflow.containsKey(origFilename)) {

            Coflow cf = masterSharedData.fileNametoCoflow.get(origFilename);

//            logger.info("Found CF {} for file {}", cf.getId(), origFilename);

            // FIXME(deprecated) TODO overhead too high
            boolean foundFG = false;
            for (Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()) {

                if (fge.getValue().getFilename().equals(origFilename)) {
//                    logger.info("Red ID = {} , expected {} ", fge.getValue().redID, reducerID);

                    if (fge.getValue().redID.equals(reducerID)) {

                        foundFG = true;
                        logger.info("Received FILE_FIN for {} {} {}", fge.getKey(), origFilename, reducerID);
                        masterSharedData.onFGFileFIN(fge.getValue(), cf);
//                        masterSharedData.onFinishFlowGroup(fge.getKey(), System.currentTimeMillis());
                    }
                }
            }

            if (!foundFG) {
                logger.error("FATAL: FG not found for {}", origFilename);
            }

        } else {
            logger.error("FATAL: CF not found for {}", origFilename);
        }
    }
}
