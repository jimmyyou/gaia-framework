package gaiaframework.util;

import java.util.HashMap;

public class Constants {

    // Min interval between schedule and Min STATUS interval
    public static final int SCHEDULE_INTERVAL_MS = 500;
    public static final int STATUS_MESSAGE_INTERVAL_MS = 100;

    public static final int COLOCATED_FG_COMPLETION_TIME = 0;

    // Port and Addresses
    public static final int SENDING_AGENT_PORT = 23330;
    public static final int RECEIVING_AGENT_PORT = 33330;
    public static final String AGENT_ADDR_PREFIX = "10.0.0.";
    public static final int DEFAULT_MASTER_PORT = 8888;
    public static final int DEFAULT_YARN_PORT = 9999;

    // Comparison between double
    public static final double DOUBLE_EPSILON = 0.001; // to 1 kb level
    public static final double LINK_AVAILABLE_THR = 0.1;

    // Number of milliseconds in a second
    public static final int MILLI_IN_SECOND = 1000;
    public static final double MILLI_IN_SECOND_D = 1000.0;

    // Timestep between advancements of flow transmission
    // in milliseconds.
    public static final int SIMULATION_TIMESTEP_MILLI = 10;

    public static final double SIMULATION_TIMESTEP_SEC = (double)SIMULATION_TIMESTEP_MILLI / (double)MILLI_IN_SECOND;

    // The number of milliseconds in an epoch. An epoch is
    // a period during which jobs may be scheduled. By
    // default we place this at 1 second because we want
    // to be able to schedule jobs at the granularity of
    // one second.
    public static final int EPOCH_MILLI = MILLI_IN_SECOND / 100;

//    public static final int DEFAULT_OUTPUTSTREAM_RATE = 100000;

    // Block is the maximum size of transmission. (64MB for GB/s level trasmission)
    public static final int BLOCK_SIZE_Bytes = 64 * 1024 * 1024;

    public static final int BUFFER_SIZE = 64 * 1024 * 1024;

    public static final int DEFAULT_TOKEN_RATE = 400;
    public static final long EXPERIMENT_INTERVAL = 60000; // 60s between experiments
    public static final long SOCKET_RETRY_MILLIS = 5000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 10000;
    public static final long HEARTBEAT_INTERVAL_MILLIS = 2000; // 2s heartbeat
    public static final int MAX_CHUNK_SIZE_Bytes = 64 * 1024;
    public static final int SENDER_QUEUE_LENGTH = 1000;
    public static final String SCHEDULER_NAME_GAIA = "gaia";
    public static final int HTTP_CHUNKSIZE = 8192;
    public static final int DEFAULT_HTTP_SERVER_PORT = 20020;

    public static HashMap<String, String> node_id_to_trace_id;

    // Return the id of the job owning the Stage, Coflow_Old, or FlowGroup
    // identified by id.
    public static String get_job_id(String id) {
        // Stage, Coflow_Old, and FlowGroup ids begin in the form <job_id>:
        return id.split(":")[0];
    }

    // Return the id of the coflow owining this flow
    public static String get_coflow_id(String id) {
        // FlowGroup ids are of the form <job_id>:<coflow_id>:<flow_id>
        String[] splits = id.split(":");
        return splits[0] + ":" + splits[1];
    }
}
