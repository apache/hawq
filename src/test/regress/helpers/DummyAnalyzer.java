import com.pivotal.pxf.api.Analyzer;
import com.pivotal.pxf.api.AnalyzerStats;
import com.pivotal.pxf.api.utilities.InputData;

/**
 * Class that defines getting statistics for ANALYZE.
 * {@link #getEstimatedStats} returns statistics for a given path
 * (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a PXF external table,
 * to get table's statistics that are used by the optimizer to plan queries. 
 * Dummy implementation, for documentation.
 */
public class DummyAnalyzer extends Analyzer {
    public DummyAnalyzer(InputData metaData) {
        super(metaData);
    }

    /**
     * Gets the estimated statistics for a given path in json format.
     *
     * @param data the URI that can appear as a file name, a directory name or a wildcard
     * @return the estimated resource statistics in json format
     * @throws Exception
     */
    @Override
    public AnalyzerStats getEstimatedStats(String data) throws Exception {
        return new AnalyzerStats(160000 /* disk block size in bytes */,
                3 /* number of disk blocks */,
                6 /* total number of rows */);
    }
}
