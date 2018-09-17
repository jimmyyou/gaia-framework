package gaiaframework.transmission;

// The new DataChunk to replace the old one

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DataChunkMessage implements Serializable {

//    private static final long serialVersionUID = 759099461767050471L;

//    DataChunkHeader header;

    // Field previously from header
    private String filename;
    private String destURL;
    private String blockID;
    private long chunkStartIndex;
    private long totalBlockLength;

    byte[] data;

    public DataChunkMessage(String filename, String destIP, String blockID, long startIndex, long totalBlockLength, byte[] data) {
//        header = new DataChunkHeader(filename, destIP, blockID, startIndex, totalBlockLength, totalFileLength);
        this.filename = filename;
        this.destURL = destIP;
        this.blockID = blockID;
        this.chunkStartIndex = startIndex;
        this.totalBlockLength = totalBlockLength;

        this.data = data;
    }

    public String getDestURL() { return this.destURL; }

    public String getFilename() { return this.filename; }

    public String getBlockId() { return this.blockID; }

    public long getStartIndex() { return this.chunkStartIndex; }

    public long getTotalBlockLength() { return this.totalBlockLength; }

    public byte[] getData() {
        return data;
    }

    // for debug purpose only
    public void appendToFilename(String appendix) {
//        filename.concat(appendix);
        this.filename = this.filename + appendix;
    }

    @Override
    public String toString() {
        return "DataChunkMessage{" +
                "header=" + "filename='" + filename + '\'' +
                ", destURL='" + destURL + '\'' +
                ", chunkStartIndex=" + chunkStartIndex +
                ", totalBlockLength=" + totalBlockLength +
                ", data size=" + data.length +
                '}';
    }

    public int getSerializedSize() throws IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

        objectOutputStream.writeObject(this);
        objectOutputStream.flush();
        objectOutputStream.close();

        return byteOutputStream.toByteArray().length;
    }
}
