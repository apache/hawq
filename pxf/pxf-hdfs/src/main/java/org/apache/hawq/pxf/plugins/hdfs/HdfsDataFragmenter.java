package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hawq.pxf.plugins.hdfs.utilities.PxfInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fragmenter class for HDFS data resources.
 *
 * Given an HDFS data source (a file, directory, or wild card pattern) divide
 * the data into fragments and return a list of them along with a list of
 * host:port locations for each.
 */
public class HdfsDataFragmenter extends Fragmenter {
    private JobConf jobConf;

    /**
     * Constructs an HdfsDataFragmenter object.
     *
     * @param md all input parameters coming from the client
     */
    public HdfsDataFragmenter(InputData md) {
        super(md);

        jobConf = new JobConf(new Configuration(), HdfsDataFragmenter.class);
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String absoluteDataPath = HdfsUtilities.absoluteDataPath(inputData.getDataSource());
        ArrayList<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;

            String filepath = fsp.getPath().toUri().getPath();
            String[] hosts = fsp.getLocations();

            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */
            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata);
            fragments.add(fragment);
        }

        return fragments;
    }

    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        String absoluteDataPath = HdfsUtilities.absoluteDataPath(inputData.getDataSource());
        ArrayList<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        if (splits.isEmpty()) {
            return new FragmentsStats(0, 0, 0);
        }
        long totalSize = 0;
        for (InputSplit split: splits) {
            totalSize += split.getLength();
        }
        InputSplit firstSplit = splits.get(0);
        return new FragmentsStats(splits.size(), firstSplit.getLength(), totalSize);
    }

    private ArrayList<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat fformat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        InputSplit[] splits = fformat.getSplits(jobConf, 1);
        ArrayList<InputSplit> result = new ArrayList<InputSplit>();

        /*
         * HD-2547: If the file is empty, an empty split is returned: no
         * locations and no length.
         */
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
