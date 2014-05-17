package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;

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
     * Constructs a QuotedLineBreakAccessor
     * 
     * @param input all input parameters coming from the client request
     * @throws Exception
     */
    public QuotedLineBreakAccessor(InputData input) throws Exception {
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

    /*
     * readNextObject
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
