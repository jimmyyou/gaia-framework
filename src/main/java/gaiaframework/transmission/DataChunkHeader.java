package gaiaframework.transmission;


import java.io.Serializable;

public class DataChunkHeader implements Serializable{
    String filename;
    String destURL;
    long startIndex;
    long chunkLength;

    public DataChunkHeader(String filename, String destURL, long startIndex, long chunkLength) {
        this.filename = filename;
        this.destURL = destURL;
        this.startIndex = startIndex;
        this.chunkLength = chunkLength;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getDestURL() {
        return destURL;
    }

    public void setDestURL(String destURL) {
        this.destURL = destURL;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getChunkLength() {
        return chunkLength;
    }

    public void setChunkLength(long chunkLength) {
        this.chunkLength = chunkLength;
    }

    @Override
    public String toString() {
        return "DataChunkHeader{" +
                "filename='" + filename + '\'' +
                ", destURL='" + destURL + '\'' +
                ", startIndex=" + startIndex +
                ", chunkLength=" + chunkLength +
                '}';
    }
}
