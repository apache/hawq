package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.Analyzer;
import org.apache.hawq.pxf.api.AnalyzerStats;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.ReadBridge;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hawq.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Analyzer class for HDFS data resources
 *
 * Given an HDFS data source (a file, directory, or wild card pattern) return
 * statistics about it (number of blocks, number of tuples, etc.)
 */
public class HdfsAnalyzer extends Analyzer {
    private JobConf jobConf;
    private FileSystem fs;
    private Log Log;

    /**
     * Constructs an HdfsAnalyzer object.
     *
     * @param inputData all input parameters coming from the client
     * @throws IOException if HDFS file system cannot be retrieved
     */
    public HdfsAnalyzer(InputData inputData) throws IOException {
        super(inputData);
        Log = LogFactory.getLog(HdfsAnalyzer.class);

        jobConf = new JobConf(new Configuration(), HdfsAnalyzer.class);
        fs = FileSystem.get(jobConf);
    }

    /**
     * Collects a number of basic statistics based on an estimate. Statistics
     * are: number of records, number of hdfs blocks and hdfs block size.
     *
     * @param datapath path is a data source URI that can appear as a file name,
     *            a directory name or a wildcard pattern
     * @return statistics in JSON format
     * @throws Exception if path is wrong, its metadata cannot be retrieved from
     *             file system, or if scanning the first block using the
     *             accessor failed
     */
    @Override
    public AnalyzerStats getEstimatedStats(String datapath) throws Exception {
        long blockSize = 0;
        long numberOfBlocks;
        long dataSize = 0;
        Path path = new Path(HdfsUtilities.absoluteDataPath(datapath));

        ArrayList<InputSplit> splits = getSplits(path);

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            dataSize += fsp.getLength();
            if (blockSize == 0) {
                Path filePath = fsp.getPath();
                FileStatus fileStatus = fs.getFileStatus(filePath);
                if (fileStatus.isFile()) {
                    blockSize = fileStatus.getBlockSize();
                }
            }
        }

        // if no file is in path (only dirs), get default block size
        if (blockSize == 0) {
            blockSize = fs.getDefaultBlockSize(path);
        }
        numberOfBlocks = splits.size();

        /*
         * The estimate of the number of tuples in table is based on the
         * actual number of tuples in the first block, multiplied by its
         * size compared to the size of the whole data to be read.
         * The calculation:
         * Ratio of tuples to size = number of tuples in first block / first block size.
         * Total of tuples = ratio * number of blocks * total block size.
         */
        long numberOfTuplesInBlock = getNumberOfTuplesInBlock(splits);
        long numberOfTuples = 0;
        if (!splits.isEmpty()) {
            long blockLength = splits.get(0).getLength();
            numberOfTuples = (long) Math.floor((((double) numberOfTuplesInBlock / blockLength) * (dataSize)));
        }
        // AnalyzerStats stats = new AnalyzerStats(blockSize, numberOfBlocks,
        AnalyzerStats stats = new AnalyzerStats(blockSize, numberOfBlocks,
                numberOfTuples);

        // print files size to log when in debug level
        Log.debug(AnalyzerStats.dataToString(stats, path.toString()));

        return stats;
    }

    /**
     * Calculates the number of tuples in a split (block). Reads one block from
     * HDFS. Exception during reading will filter upwards and handled in
     * AnalyzerResource
     */
    private long getNumberOfTuplesInBlock(ArrayList<InputSplit> splits)
            throws Exception {
        long tuples = -1; /* default - if we are not able to read data */
        ReadAccessor accessor;

        if (splits.isEmpty()) {
            return 0;
        }

        /*
         * metadata information includes: file split's start, length and hosts
         * (locations).
         */
        FileSplit firstSplit = (FileSplit) splits.get(0);
        byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(firstSplit);
        inputData.setFragmentMetadata(fragmentMetadata);
        inputData.setDataSource(firstSplit.getPath().toUri().getPath());
        accessor = ReadBridge.getFileAccessor(inputData);

        if (accessor.openForRead()) {
            tuples = 0;
            while (accessor.readNextObject() != null) {
                tuples++;
            }

            accessor.closeForRead();
        }
        Log.debug("number of tuples in first block: " + tuples);

        return tuples;
    }

    private ArrayList<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat fformat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        InputSplit[] splits = fformat.getSplits(jobConf, 1);
        ArrayList<InputSplit> result = new ArrayList<InputSplit>();

        // remove empty splits
        if (splits != null) {
            for (InputSplit split : splits) {
                if (split.getLength() > 0) {
                    result.add(split);
                }
            }
        }

        return result;
    }
}
