
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.hbase.HBaseFilterBuilder;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseTupleDescription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test class for regression tests.
 * The only thing this class does is to throw an exception
 * containing the received filter from HAWQ (HAS-FILTER & FILTER).
 */
public class FilterPrinterAccessor extends Plugin implements ReadAccessor
{
	static private Log Log = LogFactory.getLog(FilterPrinterAccessor.class);

	/*
	 * exception for exposing the filter to the world
	 */
	class FilterPrinterException extends Exception {
		FilterPrinterException(String filter) {
			super("Filter string: '" + filter + "'");
		}
	}

	public FilterPrinterAccessor(InputData input) {
		super(input);
	}

	@Override
	public boolean openForRead() throws Exception {
		
		String filter = inputData.hasFilter() ?
				inputData.getFilterString() : "No filter"; 
		throw new FilterPrinterException(filter);
	}
	
	@Override
	public void closeForRead() {
		throw new UnsupportedOperationException("closeForRead is not implemented");
	}

	@Override
	public OneRow readNextObject() {
		throw new UnsupportedOperationException("readNextObject is not implemented");
	}
}