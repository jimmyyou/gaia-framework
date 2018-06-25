package gaiaframework.transmission;


import java.io.Serializable;

public class DataChunkHeader implements Serializable{
    String filename;
    String destURL;
    String blockID;
    long chunkStartIndex;
    long totalBlockLength;

    public DataChunkHeader(String filename, String destURL, String blockID, long chunkStartIndex, long totalBlockLength) {
        this.filename = filename;
        this.destURL = destURL;
        this.chunkStartIndex = chunkStartIndex;
        this.totalBlockLength = totalBlockLength;
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

    public long getChunkStartIndex() {
        return chunkStartIndex;
    }

    public void setChunkStartIndex(long chunkStartIndex) {
        this.chunkStartIndex = chunkStartIndex;
    }

    public long getTotalBlockLength() {
        return totalBlockLength;
    }

    public void setTotalBlockLength(long totalBlockLength) {
        this.totalBlockLength = totalBlockLength;
    }

    @Override
    public String toString() {
        return "DataChunkHeader{" +
                "filename='" + filename + '\'' +
                ", destURL='" + destURL + '\'' +
                ", chunkStartIndex=" + chunkStartIndex +
                ", totalBlockLength=" + totalBlockLength +
                '}';
    }
}
