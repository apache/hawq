import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;
import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.accessors.IWriteAccessor;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
 * Dummy implementation, for documentation
 */
public class DummyAccessor extends Plugin implements IReadAccessor, IWriteAccessor
{
    private int rowNumber;
    private int fragmentNumber;
    private Log Log;
    
	public DummyAccessor(InputData metaData)
	{
		super(metaData);
        rowNumber = 0;
        fragmentNumber = 0;
		Log = LogFactory.getLog(DummyAccessor.class);

	}
	
	public boolean openForRead() throws Exception
    {
        /* fopen or similar */
        return true;
    }
	
    public OneRow readNextObject() throws Exception
    {
        /* return next row , <key=fragmentNo.rowNo, val=rowNo,text,fragmentNo>*/
        /* check for EOF */
        if (fragmentNumber > 0) 
            return null; /* signal EOF, close will be called */
        
        int fragment = this.inputData.getDataFragment();
        String fragmentMetadata = new String(this.inputData.getFragmentMetadata());
        /* generate row */
        OneRow row = new OneRow(Integer.toString(fragment) + "." + Integer.toString(rowNumber), /* key */
                                Integer.toString(rowNumber) + "," + fragmentMetadata + "," + Integer.toString(fragment) /* value */);
        /* advance */
        rowNumber += 1;
        if (rowNumber == 2) {
            rowNumber = 0;
            fragmentNumber += 1;
        } 
        /* return data */
        return row;
    }
    
	public void closeForRead() throws Exception
    {
        /* fclose or similar */
    }

	@Override
	public boolean openForWrite() throws Exception {
		/* fopen or similar */
        return true;
	}

	@Override
	public boolean writeNextObject(OneRow onerow) throws Exception {
		
		Log.info(onerow.getData());
		return true;
	}

	@Override
	public void closeForWrite() throws Exception {
		/* fclose or similar */
	}
}
