package org.apache.hawq.pxf.api;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * AnalyzerStats holds size statistics for a given path.
 */
public class AnalyzerStats {

    private static final long DEFAULT_BLOCK_SIZE = 67108864L; // 64MB (in bytes)
    private static final long DEFAULT_NUMBER_OF_BLOCKS = 1L;
    private static final long DEFAULT_NUMBER_OF_TUPLES = 1000000L;

    private long blockSize; // block size (in bytes)
    private long numberOfBlocks; // number of blocks
    private long numberOfTuples; // number of tuples

    /**
     * Constructs an AnalyzerStats.
     *
     * @param blockSize block size (in bytes)
     * @param numberOfBlocks number of blocks
     * @param numberOfTuples number of tuples
     */
    public AnalyzerStats(long blockSize, long numberOfBlocks,
                         long numberOfTuples) {
        this.setBlockSize(blockSize);
        this.setNumberOfBlocks(numberOfBlocks);
        this.setNumberOfTuples(numberOfTuples);
    }

    /** Constructs an AnalyzerStats with the default values */
    public AnalyzerStats() {
        this(DEFAULT_BLOCK_SIZE, DEFAULT_NUMBER_OF_BLOCKS,
                DEFAULT_NUMBER_OF_TUPLES);
    }

    /**
     * Given an AnalyzerStats, serialize it in JSON to be used as the result
     * string for HAWQ. An example result is as follows:
     * {"PXFDataSourceStats":{"blockSize"
     * :67108864,"numberOfBlocks":1,"numberOfTuples":5}}
     *
     * @param stats the data to be serialized
     * @return the result in json format
     * @throws IOException if converting to JSON format failed
     */
    public static String dataToJSON(AnalyzerStats stats) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // mapper serializes all members of the class by default
        return "{\"PXFDataSourceStats\":" + mapper.writeValueAsString(stats)
                + "}";
    }

    /**
     * Given a stats structure, convert it to be readable. Intended for
     * debugging purposes only.
     *
     * @param stats the data to be stringify
     * @param datapath the data path part of the original URI (e.g., table name,
     *            *.csv, etc.)
     * @return the stringified data
     */
    public static String dataToString(AnalyzerStats stats, String datapath) {
        return "Statistics information for \"" + datapath + "\" "
                + " Block Size: " + stats.blockSize + ", Number of blocks: "
                + stats.numberOfBlocks + ", Number of tuples: "
                + stats.numberOfTuples;
    }

    public long getBlockSize() {
        return blockSize;
    }

    private void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getNumberOfBlocks() {
        return numberOfBlocks;
    }

    private void setNumberOfBlocks(long numberOfBlocks) {
        this.numberOfBlocks = numberOfBlocks;
    }

    public long getNumberOfTuples() {
        return numberOfTuples;
    }

    private void setNumberOfTuples(long numberOfTuples) {
        this.numberOfTuples = numberOfTuples;
    }

}
