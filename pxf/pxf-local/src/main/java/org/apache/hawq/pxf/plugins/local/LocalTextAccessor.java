package org.apache.hawq.pxf.plugins.local;

import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalTextAccessor extends Plugin implements ReadAccessor {
    private static final Log LOG = LogFactory.getLog(LocalTextAccessor.class);
    private int rowNumber;
    private int fragmentNumber;
    public LocalTextAccessor(InputData metaData) {
        super(metaData);
    }
    @Override
    public boolean openForRead() throws Exception {
        /* fopen or similar */
        return true;
    }
    @Override
    public OneRow readNextObject() throws Exception {
        /* return next row , <key=fragmentNo.rowNo, val=rowNo,text,fragmentNo>*/
        /* check for EOF */
        if (fragmentNumber > 0)
            return null; /* signal EOF, close will be called */
        int fragment = inputData.getDataFragment();
        String fragmentMetadata = new String(inputData.getFragmentMetadata());
        /* generate row */
        OneRow row = new OneRow(fragment + "." + rowNumber, /* key */
                rowNumber + "," + fragmentMetadata + "," + fragment /* value */);
        /* advance */
        rowNumber += 1;
        if (rowNumber == 2) {
            rowNumber = 0;
            fragmentNumber += 1;
        }
        /* return data */
        return row;
    }
    @Override
    public void closeForRead() throws Exception {
        /* fclose or similar */
    }
}
