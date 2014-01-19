import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.WriteAccessor;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
 * Dummy implementation, for documentation
 */
public class DummyAccessor extends Plugin implements ReadAccessor, WriteAccessor {
    private static final Log LOG = LogFactory.getLog(DummyAccessor.class);
    private int rowNumber;
    private int fragmentNumber;

    public DummyAccessor(InputData metaData) {
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

    @Override
    public boolean openForWrite() throws Exception {
        /* fopen or similar */
        return true;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {

        LOG.info(onerow.getData());
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        /* fclose or similar */
    }
}
