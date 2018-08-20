package gaiaframework;

// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo
// TestClientNew <srcIP> <dstIP> <file> <start> <length>

import edu.umich.gaialib.FlowInfo;
import edu.umich.gaialib.GaiaClient;

import java.util.HashMap;
import java.util.Map;

public class TestClientNew {

    public static void main(String[] args) throws Exception {
        GaiaClient gaiaClient = new GaiaClient("localhost", 50051);

        long shuffleSize = 5000000;

        try {
//            gaiaClient.greet("x");

//            if (args.length > 0) {
//                shuffleSize = Long.parseLong(args[0]);
//            }
            if(args.length == 0){
                System.out.println("Usage: TestClientNew <srcIP> <dstIP> <file> <start> <length>");
            }

            Map<String, String> mappersIP = new HashMap<String, String>();
            Map<String, String> reducersIP = new HashMap<String, String>();
//            TaskInfo taskInfo = new TaskInfo("taskID", "attemptID");
            mappersIP.put("M1", args[0]);
//            TaskInfo taskInfor = new TaskInfo("taskIDr", "attemptIDr");
            reducersIP.put("R1", args[1]);

            reducersIP.put("R2", args[1]);

            long startOffset = Long.parseLong(args[3]);
            shuffleSize = Long.parseLong(args[4]);

            FlowInfo flowInfo = new FlowInfo("M1", "R1", args[2], startOffset, shuffleSize, args[0], args[1]);

//            FlowInfo flowInfo2 = new FlowInfo("M1", "R2", args[2], startOffset + shuffleSize, shuffleSize, args[0], args[1]);


            Map<String, FlowInfo> fmap = new HashMap<String, FlowInfo>();
            fmap.put("user:job:map:reduce", flowInfo);

//            fmap.put("user:job:map:reduce2", flowInfo2);

            gaiaClient.submitShuffleInfo("apple", "shuffle_stage", mappersIP, reducersIP, fmap);
        } finally {
            gaiaClient.shutdown();
        }
    }
}

