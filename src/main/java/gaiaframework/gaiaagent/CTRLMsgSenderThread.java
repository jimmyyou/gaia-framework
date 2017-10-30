package gaiaframework.gaiaagent;

// TODO use singleton to ensure there is only one instance of this thread

public class CTRLMsgSenderThread implements Runnable {

    private final AgentSharedData sharedData;

    public CTRLMsgSenderThread(AgentSharedData sharedData){
        this.sharedData = sharedData;
    }

    @Override
    public void run() {

        while (true) {
            try {
                Worker_to_CTRLMsg m = sharedData.worker_to_ctrlMsgQueue.take();

                // process the message
                switch (m.type) {
                    case FLOWSTATUS:
                        sendFlowStatus(m);

                        break;

                    case PATHSTATUS:
                        sendPathStatus(m);

                        break;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void sendPathStatus(Worker_to_CTRLMsg m) {
        sharedData.rpcClient.sendPathStatus(m.pathStatusReport);
//        sharedData.rpcClient.asyncSendPathStatus(m.pathStatusReport);
    }

    private void sendFlowStatus(Worker_to_CTRLMsg m) {
        sharedData.rpcClient.sendFlowStatus( m.flowStatusReport );
    }

}
