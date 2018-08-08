package gaiaframework.gaiaagent;

// RPC client on the agent side, to send status update message to master.

import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.MasterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class AgentRPCClient {
    private static final Logger logger = LogManager.getLogger();
    AgentSharedData agentSharedData;

    private final ManagedChannel channel;

    private final MasterServiceGrpc.MasterServiceStub asyncStub;
    private final MasterServiceGrpc.MasterServiceBlockingStub blockingStub;
    private StreamObserver<GaiaMessageProtos.FlowStatus_ACK> flowStatusAckStreamObserver;
    private StreamObserver<GaiaMessageProtos.PathStatus_ACK> pathStatusAckStreamObserver;
    // should not create a new stream every time!!!
    StreamObserver<GaiaMessageProtos.FlowStatusReport> clientStreamObserver;

    volatile boolean isStreamReady = false;

    public AgentRPCClient(String masterIP, int masterPort, AgentSharedData sharedData) {
        this(ManagedChannelBuilder.forAddress(masterIP, masterPort).usePlaintext(true).build());
        this.agentSharedData = sharedData;
        logger.info("Agent RPC Client connecting to {}:{}", masterIP, masterPort);

        flowStatusAckStreamObserver = new StreamObserver<GaiaMessageProtos.FlowStatus_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FlowStatus_ACK flowStatus_ack) {
                logger.info("Received flowStatus_ack from server");
            }

            @Override
            public void onError(Throwable t) {
                logger.error("ERROR in agent {} when sending flow status update: {}", agentSharedData.saID, t.toString());
                t.printStackTrace();
                isStreamReady = false;
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
            }
        };

        pathStatusAckStreamObserver = new StreamObserver<GaiaMessageProtos.PathStatus_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.PathStatus_ACK flowStatus_ack) {
                logger.info("Received pathStatus_ack from server");
            }

            @Override
            public void onError(Throwable t) {
                logger.error("ERROR in agent {} when calling path status update RPC: {}", agentSharedData.saID, t.toString());
                t.printStackTrace();
                // TODO retry??
            }

            @Override
            public void onCompleted() {
                logger.info("Received pathStatus_FIN from server");
            }
        };

    }

    private AgentRPCClient(ManagedChannel channel) {
        this.channel = channel;
        this.asyncStub = MasterServiceGrpc.newStub(channel);
        blockingStub = MasterServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    void initStream() {
        logger.warn("(Re)starting the Stream for SA {}", agentSharedData.saID);
        clientStreamObserver = asyncStub.updateFlowStatus(flowStatusAckStreamObserver);
        isStreamReady = true;
    }


    public void testStatusUpdate() {
        GaiaMessageProtos.FlowStatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.FlowStatusReport.FlowStatus.newBuilder()
                .setFinished(false).setId("test").setTransmitted(10);
        GaiaMessageProtos.FlowStatusReport.Builder statusReportBuilder = GaiaMessageProtos.FlowStatusReport.newBuilder();
        statusReportBuilder.addStatus(fsBuilder);

        GaiaMessageProtos.FlowStatusReport statusReport = statusReportBuilder.build();

        if (!isStreamReady) {
            initStream();
        }

        synchronized (this) {
            clientStreamObserver.onNext(statusReport);
        }

        logger.info("finished testing status report");
    }

//    public void sendFG_FIN(String fgID){
//
//
//    }

    // send the LinkStatus
    public void sendPathStatus(GaiaMessageProtos.PathStatusReport pathStatusReport) {

        synchronized (blockingStub) {
            blockingStub.updatePathStatus(pathStatusReport);
        }
    }

    void asyncSendPathStatus(GaiaMessageProtos.PathStatusReport pathStatusReport) {
        synchronized (asyncStub) {
            asyncStub.updatePathStatus(pathStatusReport, pathStatusAckStreamObserver);
        }
    }

    // should only be called by the sender thread
    void sendFlowStatus(GaiaMessageProtos.FlowStatusReport statusReport) {
        if (!isStreamReady) {
            initStream();
        }
        synchronized (this) {
            clientStreamObserver.onNext(statusReport);
//            logger.info("FSR: {}", statusReport);
        }
    }

}
