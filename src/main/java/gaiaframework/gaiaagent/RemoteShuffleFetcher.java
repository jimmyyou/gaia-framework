package gaiaframework.gaiaagent;

// Similar to LocalFileReader, but use HTTP to fetch data instead

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;

public class RemoteShuffleFetcher implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    String filename;

    LinkedBlockingQueue<DataChunk> dataQueue;

    long totalSize;

    public RemoteShuffleFetcher(String filename, LinkedBlockingQueue<DataChunk> dataQueue, long totalFileSize) {
        this.filename = filename;
        this.dataQueue = dataQueue;
        this.totalSize = totalFileSize;
    }


    @Override
    public void run() {
        // first get the URL

        getURL();

        // then set up the connection, and convert the stream into queue

        // TODO


    }

    private URL getURL(){
        return null;
    }
}
