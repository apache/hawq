package org.apache.hawq.pxf.plugins.hdfs;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A (atomic) PXF Accessor for reading \n delimited files with quoted
 * field delimiter, line delimiter, and quotes. This accessor supports
 * multi-line records, that are read from a single source (non-parallel).
 */
public class QuotedLineBreakAccessor extends HdfsAtomicDataAccessor {
    private BufferedReader reader;

    /**
     * Constructs a QuotedLineBreakAccessor.
     *
     * @param input all input parameters coming from the client request
     */
    public QuotedLineBreakAccessor(InputData input) {
        super(input);
    }

    @Override
    public boolean openForRead() throws Exception {
        if (!super.openForRead()) {
            return false;
        }
        reader = new BufferedReader(new InputStreamReader(inp));
        return true;
    }

    /**
     * Fetches one record (maybe partial) from the  file. The record is returned as a Java object.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (super.readNextObject() == null) /* check if working segment */ {
            return null;
        }

        String next_line = reader.readLine();
        if (next_line == null) /* EOF */ {
            return null;
        }

        return new OneRow(null, next_line);
    }
}
