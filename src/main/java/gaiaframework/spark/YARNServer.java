package gaiaframework.spark;

import edu.umich.gaialib.GaiaAbstractServer;
import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.FlowGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class YARNServer extends GaiaAbstractServer{

    private static final Logger logger = LogManager.getLogger();

    public YARNServer(int port) {
        super(port);
    }

    @Override
    public void processReq(ShuffleInfo req) {
        logger.info("received shuffle info: {}", req);

        // Create the CF and submit it.
        String cfID = req.getUsername() + ":" + req.getJobID();

        HashMap<String, FlowGroup> flowGroups = generateFlowGroups(req);

        Coflow cf = new Coflow(cfID, flowGroups);

        // TODO submit coflow

    }

    // TODO generate flowGroups from req
    private HashMap<String, FlowGroup> generateFlowGroups(ShuffleInfo req) {
        HashMap<String , FlowGroup> flowGroups = new HashMap<>();

        return null;
    }
}
