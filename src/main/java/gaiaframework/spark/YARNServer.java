package gaiaframework.spark;

import edu.umich.gaialib.FlowInfo;
import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import gaiaframework.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class YARNServer extends GaiaAbstractServer {

    private static final Logger logger = LogManager.getLogger();
    LinkedBlockingQueue<Coflow> cfQueue;
    Configuration configuration;

    public YARNServer(Configuration config, int port, LinkedBlockingQueue<Coflow> coflowQueue) {
        super(port);
        this.cfQueue = coflowQueue;
        this.configuration = config;
    }

    @Override
    public void processReq(ShuffleInfo req) {
        logger.info("received shuffle info: {}", req);

        // Create the CF and submit it.
        String cfID = req.getUsername() + ":" + req.getJobID();

        // Aggregate all the flows by their Data Center location
        // Gaia only sees Data Centers
        // How to deal with co-located flows?

        HashMap<String, FlowGroup> coLocatedFGs = new HashMap<>();
        HashMap<String, FlowGroup> flowGroups = new HashMap<>();

        //removeRedundantFlowGroups(generateFlowGroups(cfID, req));

        generateFlowGroups(cfID, req, coLocatedFGs, flowGroups);
        Coflow cf = new Coflow(cfID, flowGroups);

        if (coLocatedFGs.size() >= 0){
            logger.error("{} co-located FG received by Gaia", coLocatedFGs.size());
        }

        try {
            cfQueue.put(cf);

            logger.info("Coflow submitted, Trapping into waiting for coflow to finish");
            cf.blockTillFinish();
//            ShuffleTask st = new ShuffleTask(cf);
//            st.run(); // wait for it to finish
        } catch (InterruptedException e) {
            logger.error("ERROR occurred while submitting coflow");
            e.printStackTrace();
        }

    }

    // generate flowGroups from req using an IP to ID mapping
    // Location encoding starts from 0
    // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
    // src - srcLoc
    // dst - dstLoc
    // owningCoflowID - dstStage
    // Volume - divided_data_size
    private HashMap<String, FlowGroup> generateFlowGroups(String cfID, ShuffleInfo req, HashMap<String, FlowGroup> coLocatedFGs, HashMap<String, FlowGroup> flowGroups) {

        // first store all flows into flowGroups, then move the co-located ones to coLocatedFGs

        // for each FlowInfo, first find the fgID etc.
        for (ShuffleInfo.FlowInfo flowInfo : req.getFlowsList()) {

            String mapID = flowInfo.getMapAttemptID();
            String redID = flowInfo.getReduceAttemptID();

            String srcLoc = getTaskLocationID(mapID, req);
            String dstLoc = getTaskLocationID(redID, req);

            String fgID = cfID + ":" + mapID + ":" + redID + ":" + srcLoc + '-' + dstLoc;

            // check if we already have this fg.
            if (flowGroups.containsKey(fgID)) {
                FlowGroup fg = flowGroups.get(fgID);

                fg.flowInfos.add(flowInfo);
                fg.addTotalVolume(flowInfo.getFlowSize());

            } else {

                // Gaia now uses bytes as the volume
                long flowVolume = flowInfo.getFlowSize();
                if (flowVolume == 0) flowVolume = 1;
                FlowGroup fg = new FlowGroup(fgID, srcLoc, dstLoc, cfID, flowVolume,
                        flowInfo.getDataFilename(), mapID, redID);
                fg.flowInfos.add(flowInfo);

                flowGroups.put(fgID, fg);

                // when co-located
                if (srcLoc.equals(dstLoc)) {
                    coLocatedFGs.put(fgID, fg);
                }
            }

        }

        for(String key : coLocatedFGs.keySet()){
            flowGroups.remove(key);
        }

        return flowGroups;
    }

    // TODO need to change this mechanism in the future // if same mapID and same dstLoc -> redundant
    private HashMap<String, FlowGroup> removeRedundantFlowGroups(HashMap<String, FlowGroup> inFlowGroups) {
        HashMap<String, FlowGroup> ret = new HashMap<>();

        for (Map.Entry<String, FlowGroup> fe : inFlowGroups.entrySet()) {

            boolean reduandant = false;
            for (Map.Entry<String, FlowGroup> rete : ret.entrySet()) {
                if (rete.getValue().getDstLocation().equals(fe.getValue().getDstLocation()) &&
                        rete.getValue().getMapID().equals(fe.getValue().getMapID())) {
                    reduandant = true;
                }
            }
            // check if this fe needs to be put in ret

            if (!reduandant) {
                ret.put(fe.getKey(), fe.getValue());
            }
        }

        return ret;
    }

    // 1. find the IP for this task using ShuffleInfo (first look in MapIP, then in ReduceIP)
    // 2. find the DCID for this IP?
    private String getTaskLocationID(String taskID, ShuffleInfo req) {
        // check the hostIP
        for (ShuffleInfo.MapperInfo mapperInfo : req.getMappersList()) {
            if (taskID.equals(mapperInfo.getMapperID())) {

                String ip_raw = mapperInfo.getMapperIP();

                String id = configuration.findDCIDbyAddr(ip_raw);
//                String id = configuration.findSrcIDbyAddr(ip_raw.split(":")[0]);
                if (id != null) return id;
            }
        }

        // then check the reducerID
        for (ShuffleInfo.ReducerInfo reducerInfo : req.getReducersList()) {
            if (taskID.equals(reducerInfo.getReducerID())) {

                String id = configuration.findDCIDbyAddr(reducerInfo.getReducerIP());
//                String id = configuration.findDstIDbyAddr(reducerInfo.getReducerIP().split(":")[0]);
                if (id != null) return id;
            }
        }

        logger.error("Task Location not found for {}", taskID);
        return null;
    }

}
