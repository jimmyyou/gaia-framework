package gaiaframework;

import gaiaframework.gaiamaster.Master;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class TerraMaster {

    public static boolean is_emulation_ = false;
    private static boolean isRunningOnList = false;
    private static boolean isSettingFlowRules = false;
    private static boolean isDebugMode = false;

    public static double SCALE_FACTOR = 1.0; // default value, used by DAGReader.java
    private static final Logger logger = LogManager.getLogger();
    public static double MASTER_SCALE_FACTOR = 1.0;

    public static HashMap<String, String> parse_cli(String[] args)
            throws org.apache.commons.cli.ParseException {

        HashMap<String, String> args_map = new HashMap<String, String>();
        Options options = new Options();
        options.addOption("g", true, "path to gml file");
        options.addOption("j", true, "path to trace file");
        options.addOption("s", true, "scheduler to use. One of {baseline, recursive-remain-flow}");
        options.addOption("o", true, "path to directory to save output files");
        options.addOption("e", false, "run under emulation");
        options.addOption("c", true, "path to config file");
//        options.addOption("u", true, "scale up the data size by factor of X");

        options.addOption("b", true, "scaling factor for bandwidth");
        options.addOption("w", true, "scaling factor for workload");
        options.addOption("l", false, "run on a list of job traces");
        options.addOption("p", false, "set up flow rules");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("e")) {
            is_emulation_ = true;
        }

        if (cmd.hasOption("l")) {
            isRunningOnList = true;
        }

        if (cmd.hasOption('d')) {
            isDebugMode = true;
        }

        if (cmd.hasOption("g")) {
            args_map.put("gml", cmd.getOptionValue("g"));
        } else {
            System.out.println("ERROR: Must specify a path to a gml file using the -g flag");
            System.exit(1);
        }

        if (cmd.hasOption("p")) {
            System.out.println("Flow rules not set, we are setting flow rules this time");
            isSettingFlowRules = true;
        } else {
            logger.warn("Assuming flow rules has been set");
        }

        if (cmd.hasOption("j")) {
            args_map.put("trace", cmd.getOptionValue("j"));
        } else if (!cmd.hasOption('e')) { // if provided -e, no need for the trace.
            System.out.println("ERROR: Must specify a path to a trace file using the -j flag");
            System.exit(1);
        }

        if (cmd.hasOption("s")) {
            args_map.put("scheduler", cmd.getOptionValue("s"));
        } else {
            System.out.println("ERROR: Must specify a scheduler {baseline, recursive-remain-flow} using the -s flag");
            System.exit(1);
        }

        if (cmd.hasOption("o")) {
            args_map.put("outdir", cmd.getOptionValue("o"));
        } else {
            args_map.put("outdir", "/tmp");
        }

        if (cmd.hasOption("c")) {
            args_map.put("config", cmd.getOptionValue("c"));
        } else {
            args_map.put("config", null);
        }

        if (cmd.hasOption('w')) {
            MASTER_SCALE_FACTOR = Double.parseDouble(cmd.getOptionValue('w'));
            System.out.println("Using master scaling factor = " + MASTER_SCALE_FACTOR);
        }

        if (cmd.hasOption("b")) {
            args_map.put("bw_factor", cmd.getOptionValue("b"));
            System.out.println("Given bw_factor: " + Double.parseDouble(args_map.get("bw_factor")) + " , the sending agents don't need this factor");
        } else {
            args_map.put("bw_factor", "1.0");
        }

        return args_map;
    }

    public static void main(String[] args) {
        HashMap<String, String> args_map = null;
        try {
            args_map = parse_cli(args);
        } catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
            return;
        }

        try {
            Process p = Runtime.getRuntime().exec("cp models/MinCCT.mod /tmp/MinCCT.mod");
            p.waitFor();

            logger.info("GAIA: finished copying the model..");

            Master m = new Master(args_map.get("gml"), args_map.get("trace"),
                    args_map.get("scheduler"), args_map.get("outdir"), args_map.get("config"),
                    Double.parseDouble(args_map.get("bw_factor")), isRunningOnList, isSettingFlowRules, isDebugMode);

            if (is_emulation_) {
                m.emulate();
            } else {
                m.simulate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return;
    }
}
