import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.accessors.Accessor;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
 * Dummy implementation, for documentation
 */
public class DummyAccessor extends Accessor
{
    private int rowNumber;
    private int fragmentNumber;
    
	public DummyAccessor(InputData metaData)
	{
		super(metaData);
        rowNumber = 0;
        fragmentNumber = 0;
	}
	
	public boolean Open() throws Exception
    {
        /* fopen or similar */
        return true;
    }
	
    public OneRow LoadNextObject() throws Exception
    {
        /* return next row , <key=fragmentNo.rowNo, val=rowNo,text,fragmentNo>*/
        /* check for EOF */
        if (fragmentNumber > 0) 
            return null; /* signal EOF, close will be called */
        
        int fragment = this.inputData.getDataFragment();
        /* generate row */
        OneRow row = new OneRow(Integer.toString(fragment) + "." + Integer.toString(rowNumber), /* key */
                                Integer.toString(rowNumber) + ",text," + Integer.toString(fragment) /* value */);
        /* advance */
        rowNumber += 1;
        if (rowNumber == 2) {
            rowNumber = 0;
            fragmentNumber += 1;
        } 
        /* return data */
        return row;
    }
    
	public void Close() throws Exception
    {
        /* fclose or similar */
    }
}
