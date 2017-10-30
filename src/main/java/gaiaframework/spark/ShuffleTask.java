package gaiaframework.spark;

// This is the class used to transfer the shuffle
// No need for this class

import gaiaframework.gaiamaster.Coflow;
import gaiaframework.gaiamaster.Master;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShuffleTask implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    Coflow cf;

    public ShuffleTask(Coflow cf) {
        this.cf = cf;
    }

    @Override
    public void run() {

        // submit coflow before starting the shuffleTask
//        logger.info(" submitting coflow to Gaia");

        // TODO at some point, wait for the coflow to finish
        logger.info("ShuffleTask started, Trapping into waiting for coflow to finish");
        try {
            cf.blockTillFinish();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
