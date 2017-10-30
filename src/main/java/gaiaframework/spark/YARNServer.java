package gaiaframework.spark;

import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class YARNServer extends GaiaAbstractServer{

    private static final Logger logger = LogManager.getLogger();
    LinkedBlockingQueue<Coflow> cfQueue;

    public YARNServer(int port, LinkedBlockingQueue<Coflow> coflowQueue) {
        super(port);
        this.cfQueue = coflowQueue;
    }

    @Override
    public void processReq(ShuffleInfo req) {
        logger.info("received shuffle info: {}", req);

        // Create the CF and submit it.
        String cfID = req.getUsername() + ":" + req.getJobID();

        HashMap<String, FlowGroup> flowGroups = generateFlowGroups(req);

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

    // TODO generate flowGroups from req
    private HashMap<String, FlowGroup> generateFlowGroups(ShuffleInfo req) {
        HashMap<String , FlowGroup> flowGroups = new HashMap<>();

        return null;
    }
}
