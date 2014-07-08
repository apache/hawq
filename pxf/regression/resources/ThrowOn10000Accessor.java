import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.LineBreakAccessor;

/**
 * Test accessor, based on LineBreakAccessor.
 * This accessor throws a runtime exception after reading 10000 records.
 * Used to test an error that occurs after the first packet 
 * of the response is already sent (GPSQL-2272).
 */
public class ThrowOn10000Accessor extends LineBreakAccessor {

    private Log Log;
    private int rowCount;

    /**
     * Constructs a ThrowOn1000Accessor
     * 
     * @param input all input parameters coming from the client request
     * @throws Exception
     */
    public ThrowOn10000Accessor(InputData input) throws Exception {
        super(input);
        
        rowCount = 0;
        
        Log = LogFactory.getLog(ThrowOn10000Accessor.class);
    }

    /**
     * Reads next record using LineBreakAccessor.readNextObject().
     * Throws a runtime exception upon reading the 10000 record.
     * 
     * @return next record
     * @throws IOException
     */
    @Override 
    public OneRow readNextObject() throws IOException {
    
    	OneRow oneRow = super.readNextObject();
    	
    	if ((oneRow != null) && (rowCount == 10000)) {
    		throw new RuntimeException("10000 rows!");
    	}
    	rowCount++;
    	
    	return oneRow;
    }
  
}
