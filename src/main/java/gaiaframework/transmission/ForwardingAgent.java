package gaiaframework.transmission;

// Forwards the data to the specific destination host inside the DataCenter
// Each Data Center only has one ForwardingAgent

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ForwardingAgent {

    boolean isForwarding = true;
    int port = 33330;

    private static final Logger logger = LogManager.getLogger();

    public ForwardingAgent(boolean isForwarding, int port) {
        this.isForwarding = isForwarding;
        this.port = port;
    }

    public static void main(String[] args) {

        boolean isForwarding = true;
        int port = 33330;

        Options options = new Options();
        options.addOption("c", "config", true, "path to config file");
        options.addOption("n", "no-forward", false, "only receives, do not forward");

        HelpFormatter formatter = new HelpFormatter();

        if (args.length == 0) {
            formatter.printHelp("SendingAgent -c [config] -n", options);
            return;
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("n")) {
                logger.info("Disabling forwarding");
                isForwarding = false;
            }

            if (cmd.hasOption("p")) {
                port = Integer.parseInt(cmd.getOptionValue('p'));
                logger.info("Using port {}", port);
            }

            ForwardingAgent fa = new ForwardingAgent(isForwarding, port);
            fa.start();

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    // main body
    private void start() {

        startClients();
        startServerAndBlock();

    }

    private void startServerAndBlock() {

    }

    private void startClients() {

    }
}
