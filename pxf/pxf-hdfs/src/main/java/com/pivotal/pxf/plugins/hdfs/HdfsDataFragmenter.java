package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;
import com.pivotal.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
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
        InputSplit[] splits = getSplits(new Path(absoluteDataPath));

        for (InputSplit split : splits != null ? splits : new InputSplit[] {}) {
            FileSplit fsp = (FileSplit) split;

            /*
             * HD-2547: If the file is empty, an empty split is returned: no
             * locations and no length.
             */
            if (fsp.getLength() <= 0) {
                continue;
            }

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

    private InputSplit[] getSplits(Path path) throws IOException {
        PxfInputFormat format = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        return format.getSplits(jobConf, 1);
    }
}
