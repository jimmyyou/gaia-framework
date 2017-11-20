package gaiaframework.gaiaagent;

/* states:
 1. idle
 2. connecting to RAs
 3. ready
*/

import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.gaiaprotos.SendingAgentServiceGrpc;
import gaiaframework.network.NetGraph;
import gaiaframework.util.Configuration;
import gaiaframework.util.Constants;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class AgentRPCServer {
    private static final Logger logger = LogManager.getLogger();

    private Server server;
    int port = 23000; // default port number
    NetGraph netGraph;
    String saID;
    String trace_id_;
    Configuration config;

    // data structures from the SAAPI
    AgentSharedData sharedData;
    private int fumaxsize = 0;

    public AgentRPCServer(String id, NetGraph net_graph, Configuration config, AgentSharedData sharedData) {
        this.config = config;
        this.saID = id;
        this.netGraph = net_graph;
        this.port = config.getSAPort(Integer.parseInt(id));
        this.trace_id_ = Constants.node_id_to_trace_id.get(id);
        this.sharedData = sharedData;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new SAServiceImpl())
                .build()
                .start();
        logger.info("gRPC Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                AgentRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        }); // end of Shutdown Hook


        // TODO forward the FUM message

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

    class SAServiceImpl extends SendingAgentServiceGrpc.SendingAgentServiceImplBase {

        // handler of prepareConns message, setup the Workers and PConns, reply with the PA message.
        @Override
        public void prepareConnections(gaiaframework.gaiaprotos.GaiaMessageProtos.PAM_REQ request,
                                       io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.PAMessage> responseObserver) {

            if (sharedData.saState != AgentSharedData.SAState.IDLE) {
                logger.error("Received Prepare Connection message when not IDLE");
                // TODO error handling
//                    responseObserver.onError();
            }

            sharedData.saState = AgentSharedData.SAState.CONNECTING;

            int workerCnt = 0;
            // set up Persistent Connections and send PA Messages.
            for (String ra_id : netGraph.nodes_) {

                if (!saID.equals(ra_id)) { // don't consider path to SA itself.

                    // because apap is consistent among different programs.
                    LinkedBlockingQueue[] queues = new LinkedBlockingQueue[netGraph.apap_.get(saID).get(ra_id).size()];
                    int pathSize = netGraph.apap_.get(saID).get(ra_id).size();
                    for (int i = 0; i < pathSize; i++) {
                        // ID of connection is SA_id-RA_id.path_id
                        String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);
                        int raID = Integer.parseInt(ra_id);


//                            // Create the socket that the PersistentConnection object will use
//                            Socket socketToRA = new Socket( config.getFAIP(raID) , config.getFAPort(raID));
//                            socketToRA.setSoTimeout(0);
//                            socketToRA.setKeepAlive(true);

                        workerCnt++;
                        int port = 40000 + Integer.parseInt(saID) * 100 + workerCnt;

                        queues[i] = new LinkedBlockingQueue<CTRL_to_WorkerMsg>();

                        // send PA message
                        GaiaMessageProtos.PAMessage reply = GaiaMessageProtos.PAMessage.newBuilder().setSaId(saID).setRaId(ra_id).setPathId(i).setPortNo(port).build();
                        responseObserver.onNext(reply);

                        // Start the worker Thread TODO: handle thread failure/PConn failure
                        Thread wt = new Thread( new WorkerThread_New(conn_id, ra_id , i , queues[i] , sharedData,
                                config.getFAIP(raID) , config.getFAPort(raID), port ) );
                        wt.start();

                    }

                    sharedData.workerQueues.put(ra_id , queues);

                } // if id != ra_id

            } // for ra_id in nodes

            responseObserver.onCompleted();
            sharedData.saState = AgentSharedData.SAState.READY;
            sharedData.readySignal.countDown();
//            agentSharedData.saState.notify();
        }

        @Override
        public io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FlowUpdate> changeFlow(
                io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FUM_ACK> responseObserver) {

            return new StreamObserver<GaiaMessageProtos.FlowUpdate>() {
                @Override
                public void onNext(GaiaMessageProtos.FlowUpdate flowUpdate) {
                    int fusize = flowUpdate.getSerializedSize();
                    fumaxsize = fusize > fumaxsize ? fusize : fumaxsize;
                    logger.debug("Received FUM, size: {} / {}\ncontent: {}", fusize, fumaxsize, flowUpdate);
                    try {
                        sharedData.fumQueue.put(flowUpdate);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("ERROR in agent {} when handling FUM: {}", sharedData.saID, t.toString());
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    logger.info("Received RPC completion from master RPC client");
                }
            };
        }

        @Override
        public void controlExperiment(gaiaframework.gaiaprotos.GaiaMessageProtos.Exp_CTRL request,
                                   io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.Exp_CTRL_ACK> responseObserver) {

            switch (request.getOp() ){
                case START:
                    startExperiment(request.getExpName());
                    break;

                case STOP:

                    break;
            }

            responseObserver.onNext(GaiaMessageProtos.Exp_CTRL_ACK.getDefaultInstance());
            responseObserver.onCompleted();
        }

        private void startExperiment(String expName) {
            // tell all workers to connect the socket and start heartbeat.
            int replyCnt = 0;
            for(Map.Entry<String, LinkedBlockingQueue<CTRL_to_WorkerMsg>[] > qe :sharedData.workerQueues.entrySet()){
                LinkedBlockingQueue<CTRL_to_WorkerMsg>[] ql = qe.getValue();
                for (int i = 0 ; i < ql.length ; i++){
                    try {
                        ql[i].put( new CTRL_to_WorkerMsg(0));
                        replyCnt++;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // TODO: add bwm-ng command
            if (expName != null) {
                logger.info("Agent start working for experiment {}" , expName);
            }

            // block until all workers are done with reconnection
            if (sharedData.cnt_StartedConnections == null){
                sharedData.cnt_StartedConnections = new CountDownLatch(replyCnt);
                sharedData.MAX_ACTIVE_CONNECTION = replyCnt;
                logger.info("MAX_ACTIVE_CONNECTION set to {}", replyCnt);
            }
            else {
                logger.error("CountDownLatch already initialized!");
            }

            try {
                sharedData.cnt_StartedConnections.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // start heartbeat and return
            sharedData.isSendingHeartBeat.set(true);
            logger.info("starting heartbeat");
        }

        // non-stream version
/*        @Override
        public void changeFlow(gaiaframework.gaiaprotos.GaiaMessageProtos.FlowUpdate request,
                               io.grpc.stub.StreamObserver<gaiaframework.gaiaprotos.GaiaMessageProtos.FUM_ACK> responseObserver) {
            if (saState != SAState.READY) {
                logger.error("Received changeFLow when not READY");
            }

            // forward the FUM to the CTRLMessageListener.
            try {
                fumQueue.put(request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            gaiaframework.gaiaprotos.GaiaMessageProtos.FUM_ACK fumAck = gaiaframework.gaiaprotos.GaiaMessageProtos.FUM_ACK.newBuilder().build();

            responseObserver.onNext(fumAck);
            responseObserver.onCompleted();

        }*/


    }
}
