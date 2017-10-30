package gaiaframework.spark;

// This is the class used to transfer the shuffle

import gaiaframework.gaiamaster.Coflow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShuffleTask implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    Coflow cf;


    @Override
    public void run() {



        // TODO at some point, wait for the coflow to finish
        logger.info("Trapping into waiting for coflow to finish");
        try {
            cf.blockTillFinish();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
