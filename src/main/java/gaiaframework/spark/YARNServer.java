package gaiaframework.spark;

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

        if (coLocatedFGs.size() >= 0) {
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

    // generate aggFlowGroups from req using an IP to ID mapping
    // Location encoding starts from 0
    // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
    // src - srcLoc
    // dst - dstLoc
    // owningCoflowID - dstStage
    // Volume - divided_data_size
    private HashMap<String, FlowGroup> generateFlowGroups(String cfID, ShuffleInfo req, HashMap<String, FlowGroup> coLocatedFGs, HashMap<String, FlowGroup> aggFlowGroups) {

        // first store all flows into aggFlowGroups, then move the co-located ones to coLocatedFGs

        // for each FlowInfo, first find the fgID etc.
        for (ShuffleInfo.FlowInfo flowInfo : req.getFlowsList()) {

            String mapID = flowInfo.getMapAttemptID();
            String redID = flowInfo.getReduceAttemptID();

            String srcIP = flowInfo.getMapperIP();
            String dstIP = flowInfo.getReducerIP();

            String srcLoc = getTaskLocationIDfromIP(srcIP);
            String dstLoc = getTaskLocationIDfromIP(dstIP);

/*            String srcLoc = getTaskLocationID(mapID, req);
            String dstLoc = getTaskLocationID(redID, req);

            String srcIP = getRawAddrfromTaskID(mapID, req).split(":")[0];
            String dstIP = getRawAddrfromTaskID(redID, req).split(":")[0];*/


            String afgID = cfID + ":" + srcLoc + '-' + dstLoc;
            String fgID = cfID + ":" + mapID + ":" + redID + ":" + srcLoc + '-' + dstLoc;

            if(srcLoc.equals(dstLoc)) continue;

            // check if we already have this fg.
            if (aggFlowGroups.containsKey(afgID)) {
                FlowGroup fg = aggFlowGroups.get(afgID);

                fg.flowInfos.add(flowInfo);
                fg.srcIPs.add(srcIP);
                fg.dstIPs.add(dstIP);
                fg.addTotalVolume(flowInfo.getFlowSize());

            } else {

                // Gaia now uses bytes as the volume
                long flowVolume = flowInfo.getFlowSize();
                if (flowVolume == 0) flowVolume = 1;
                FlowGroup fg = new FlowGroup(fgID, srcLoc, dstLoc, cfID, flowVolume,
                        flowInfo.getDataFilename(), mapID, redID);
                fg.flowInfos.add(flowInfo);
                fg.srcIPs.add(srcIP);
                fg.dstIPs.add(dstIP);
                aggFlowGroups.put(afgID, fg);

                // when co-located
                // FIXME, now ignoring all co-located
                if (srcLoc.equals(dstLoc)) {
                    coLocatedFGs.put(fgID, fg);
                }
            }

        }

        for (String key : coLocatedFGs.keySet()) {
            aggFlowGroups.remove(key);
        }

        return aggFlowGroups;
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
/*    private String getTaskLocationID(String taskID, ShuffleInfo req) {
        String addr = getRawAddrfromTaskID(taskID, req);

        if (addr != null) {
            return configuration.findDCIDbyHostAddr(addr);
        }

        logger.error("Task IP not found for {}", taskID);
        return null;
    }*/

    private String getTaskLocationIDfromIP(String IP) {

        if (IP != null) {
            return configuration.findDCIDbyHostAddr(IP);
        }

        logger.error("Task IP is null");
        return null;
    }

/*    private String getRawAddrfromTaskID(String taskID, ShuffleInfo req) {
        // check the hostIP
        for (ShuffleInfo.MapperInfo mapperInfo : req.getMappersList()) {
            if (taskID.equals(mapperInfo.getMapperID())) {

                String addr_raw = mapperInfo.getMapperIP();


//                return addr_raw;
                return hardCodedURLResolver(addr_raw);
            }
        }

        // then check the reducerID
        for (ShuffleInfo.ReducerInfo reducerInfo : req.getReducersList()) {
            if (taskID.equals(reducerInfo.getReducerID())) {

                String addr_raw = reducerInfo.getReducerIP();

//                return addr_raw;
                return hardCodedURLResolver(addr_raw);
            }
        }

        return null;
    }*/

    private String hardCodedURLResolver(String url) {
        if(url.equals("clnode045.clemson.cloudlab.us:8042")){
            return "10.0.1.1";
        }

        if(url.equals("clnode072.clemson.cloudlab.us:8042")){
            return "10.0.1.2";
        }

        if(url.equals("clnode062.clemson.cloudlab.us:8042")){
            return "10.0.1.3";
        }

        if(url.equals("clnode093.clemson.cloudlab.us:8042")){
            return "10.0.2.3";
        }

        if(url.equals("clnode056.clemson.cloudlab.us:8042")){
            return "10.0.2.1";
        }

        if(url.equals("clnode088.clemson.cloudlab.us:8042")){
            return "10.0.2.2";
        }

        return null;
    }
}
