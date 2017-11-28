package gaiaframework;

// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

import edu.umich.gaialib.FlowInfo;
import edu.umich.gaialib.GaiaClient;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class TestClient {

    public static void main(String[] args) throws Exception {
        GaiaClient gaiaClient = new GaiaClient("localhost", 50051);
        try {
//            gaiaClient.greet("x");

            Map<String, String> mappersIP = new HashMap<String, String>();
            Map<String, String> reducersIP = new HashMap<String, String>();
//            TaskInfo taskInfo = new TaskInfo("taskID", "attemptID");
            mappersIP.put("M1", "127.0.0.1");
//            TaskInfo taskInfor = new TaskInfo("taskIDr", "attemptIDr");
            reducersIP.put("R1", "localhost");

            FlowInfo flowInfo = new FlowInfo("M1", "R1", "/tmp/rand.out",
                    0, 5000000, "10.0.1.1", "10.0.2.2");

            Map<String, FlowInfo> fmap = new HashMap<String, FlowInfo>();
            fmap.put("user:job:map:reduce", flowInfo);

            gaiaClient.submitShuffleInfo("tester", "tester", mappersIP, reducersIP, fmap);
        } finally {
            gaiaClient.shutdown();
        }
    }
}

