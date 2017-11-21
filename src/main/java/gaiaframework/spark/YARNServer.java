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



        HashMap<String, FlowGroup> flowGroups = removeRedundantFlowGroups(generateFlowGroups(cfID, req));

        Coflow cf = new Coflow(cfID, flowGroups);

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

    // FIXME combine the flowgroups with same mapID and same reduceLoc
    private HashMap<String, FlowGroup> generateFlowGroups(String cfID, ShuffleInfo req) {
        HashMap<String, FlowGroup> flowGroups = new HashMap<>();


        for (ShuffleInfo.FlowInfo flowInfo : req.getFlowsList()) {

            String mapID = flowInfo.getMapAttemptID();
            String redID = flowInfo.getReduceAttemptID();

            String srcLoc = getLocation(mapID, req);
            String dstLoc = getLocation(redID, req);

            String fgID = cfID + ":" + mapID + ":" + redID + ":" + srcLoc + '-' + dstLoc;
            // We need MB as the unit for Gaia, so we divide by 1024 and 1024
            long flowVolume = flowInfo.getFlowSize() / 1024 / 1024;
            if (flowVolume == 0) flowVolume = 1;
            FlowGroup fg = new FlowGroup(fgID, srcLoc, dstLoc, cfID, flowVolume,
                    flowInfo.getDataFilename(), mapID, redID);

            flowGroups.put(fgID, fg);
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

    private String getLocation(String taskID, ShuffleInfo req) {
        // first check the IP
        for (ShuffleInfo.MapperInfo mapperInfo : req.getMappersList()) {
            if (taskID.equals(mapperInfo.getMapperID())) {

                String ip_raw = mapperInfo.getMapperIP();

                String id = configuration.findSAIDbyIP(ip_raw.split(":")[0]);
                if (id != null) return id;
            }
        }

        for (ShuffleInfo.ReducerInfo reducerInfo : req.getReducersList()) {
            if (taskID.equals(reducerInfo.getReducerID())) {

                String id = configuration.findFAIDbyIP(reducerInfo.getReducerIP().split(":")[0]);
                if (id != null) return id;
            }
        }

        logger.error("Task Location not found for {}", taskID);
        return null;
    }


}
