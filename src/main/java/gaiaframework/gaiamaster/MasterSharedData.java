package gaiaframework.gaiamaster;

import gaiaframework.gaiaprotos.GaiaMessageProtos;
import gaiaframework.spark.YARNMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterSharedData {
    private static final Logger logger = LogManager.getLogger();

    volatile ConcurrentHashMap<String, Coflow> coflowPool;

    // index for searching flowGroup in this data structure.
    // only need to add entry, no need to delete entry. TODO verify this.
    private volatile ConcurrentHashMap<String, Coflow> flowIDtoCoflow;
    volatile HashMap<String, Coflow> fileNametoCoflow;

    volatile boolean flag_CF_ADD = false;
    volatile boolean flag_CF_FIN = false;
    volatile boolean flag_FG_FIN = false;

    // move this event queue here because the RPC server module need to access it
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();
    LinkedBlockingQueue<GaiaMessageProtos.PathStatusReport> linkStatusQueue = new LinkedBlockingQueue<>();

/*        public AtomicBoolean flag_CF_ADD = new AtomicBoolean(false);
    public AtomicBoolean flag_CF_FIN = new AtomicBoolean(false);
    public AtomicBoolean flag_FG_FIN = new AtomicBoolean(false);*/

    // stats
    int flowStartCnt = 0;
    int flowFINCnt = 0;

    // handles coflow finish.
    public synchronized boolean onFinishCoflow(String coflowID) {
        logger.info("Master: trying to finish Coflow {}", coflowID);


        // use the get and set method, to make sure that:
        // 1. the value is false before we send COFLOW_FIN
        // 2. the value must be set to true, after whatever we do.
        if (coflowPool.containsKey(coflowID) && !coflowPool.get(coflowID).finish(true)) {

            this.flag_CF_FIN = true;

            coflowPool.remove(coflowID);

            return true;
        }

        return false;
    }

    public synchronized void addCoflow(String id, Coflow cf) { // trim the co-located flowgroup before adding!
        // first add index
        for (FlowGroup fg : cf.getFlowGroups().values()) {
            flowIDtoCoflow.put(fg.getId(), cf);

            if (!fileNametoCoflow.containsKey(fg.getFilename())) {
                fileNametoCoflow.put(fg.getFilename(), cf);
            }
        }
        //  then add coflow
        coflowPool.put(id, cf);
    }


    public FlowGroup getFlowGroup(String id) {
        if (flowIDtoCoflow.containsKey(id)) {
            return flowIDtoCoflow.get(id).getFlowGroup(id);
        } else {
            return null;
        }
    }

    // TODO: set the concurrency level.
    public MasterSharedData() {
        this.coflowPool = new ConcurrentHashMap<>();
        this.flowIDtoCoflow = new ConcurrentHashMap<>();
        this.fileNametoCoflow = new HashMap<>();
    }

    public void onFinishFlowGroup(String fid, long timestamp) {

        flowFINCnt++;

        FlowGroup fg = getFlowGroup(fid);
        if (fg == null) {
            logger.warn("fg == null for fid = {}", fid);
            return;
        }
        if (fg.getAndSetFinish(timestamp)) {
            logger.warn("Finishing a flow that should have been finished {}", fg.getId());
            return; // if already finished, do nothing.
        }

        flag_FG_FIN = true;

        // check if the owning coflow is finished
        Coflow cf = coflowPool.get(fg.getOwningCoflowID());

        if (cf == null) { // cf may already be finished.
            return;
        }

        boolean flag = true;

        for (FlowGroup ffg : cf.getFlowGroups().values()) {
            flag = flag && ffg.isSendingFinished();
        }

        // if so set coflow status, send COFLOW_FIN
        if (flag) {
            String coflowID = fg.getOwningCoflowID();
            if (onFinishCoflow(coflowID)) {
                try {
                    yarnEventQueue.put(new YARNMessages(coflowID));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void onLinkDown(String saID, String raID, int pathID) {

    }

    public void onLinkUp(String saID, String raID, int pathID) {

    }

    public void onLinkChange(GaiaMessageProtos.PathStatusReport request) {
        try {
            linkStatusQueue.add(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onFileFIN(FlowGroup fg, Coflow cf) {
        if (fg == null) {
            logger.error("FATAL: fg == null when file fin");
            return;
        }

        if (fg.getAndSetFileFIN()) {
            logger.warn("FILE_FIN for a flow that should have been FINned {}", fg.getId());
            return; // if already finished, do nothing.
        }

        if (cf == null) {
            logger.error("FATAL: FILE_FIN for null");
            return;
        }

        cf.isCoflowFileFinishedLatch.countDown();
        logger.info("Counting down for cf {} : {}", cf.getId(), cf.isCoflowFileFinishedLatch.getCount());

    }
}
