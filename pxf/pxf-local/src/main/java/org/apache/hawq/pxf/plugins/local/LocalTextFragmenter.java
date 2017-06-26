package org.apache.hawq.pxf.plugins.local;

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.utilities.InputData;
import java.util.List;

/**
 * Fragmenter class for text data on local filesystem.
 *
 * Given an text file on local filesystem(a file, directory, or wild card pattern),divide
 * the data into fragments and return a list of them along with a list of
 * host:port locations for each.
 */
public class LocalTextFragmenter extends Fragmenter{
    public LocalTextFragmenter(InputData metaData) {
        super(metaData);
    }
    /*
     * path is a data source URI that can appear as a file name, a directory name or a wildcard
     * returns the data fragments - identifiers of data and a list of available hosts
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        String[] localHosts = new String[]{localhostname, localhostname};
        fragments.add(new Fragment(inputData.getDataSource() + ".1" /* source name */,
                localHosts /* available hosts list */,
                "fragment1".getBytes()));
        fragments.add(new Fragment(inputData.getDataSource() + ".2" /* source name */,
                localHosts /* available hosts list */,
                "fragment2".getBytes()));
        fragments.add(new Fragment(inputData.getDataSource() + ".3" /* source name */,
                localHosts /* available hosts list */,
                "fragment3".getBytes()));
        return fragments;
    }

}
