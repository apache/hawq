package com.pivotal.pxf.api.utilities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Common configuration available to all PXF plugins. Represents input data
 * coming from client applications, such as Hawq.
 */
public class InputData {
	
    public static final int INVALID_SPLIT_IDX = -1;
    private static final Log LOG = LogFactory.getLog(InputData.class);

    protected Map<String, String> requestParametersMap;
    protected ArrayList<ColumnDescriptor> tupleDescription;
    protected int segmentId;
    protected int totalSegments;
    protected byte[] fragmentMetadata = null;
    protected byte[] userData = null;
    protected boolean filterStringValid;
    protected String filterString;
    protected String dataSource;
    protected String accessor;
    protected String resolver;
    protected String analyzer;
    protected String fragmenter;
    protected String remoteLogin;
    protected String remoteSecret;
    protected int dataFragment; /* should be deprecated */

    /**
     * When false the bridge has to run in synchronized mode.
     * default value - true.
     */
    protected boolean threadSafe;

	/**
	 * The name of the recordkey column. It can appear in any location in the
	 * columns list. By specifying the recordkey column, the user declares that
	 * he is interested to receive for every record retrieved also the the
	 * recordkey in the database. The recordkey is present in HBase table (it is
	 * called rowkey), and in sequence files. When the HDFS storage element
	 * queried will not have a recordkey and the user will still specify it in
	 * the "create external table" statement, then the values for this field
	 * will be null. This field will always be the first field in the tuple
	 * returned.
	 */
    protected ColumnDescriptor recordkeyColumn;

    /**
     * Constructs an empty InputData
     */
    public InputData() {
    }
    
    /**
     * Constructs an InputData from a copy.
     * Used to create from an extending class.
     *
     * @param copy the input data to copy
     */
    public InputData(InputData copy) {

        this.requestParametersMap = copy.requestParametersMap;
        this.segmentId = copy.segmentId;
        this.totalSegments = copy.totalSegments;
        this.fragmentMetadata = copy.fragmentMetadata;
        this.userData = copy.userData;
        this.tupleDescription = copy.tupleDescription;
        this.recordkeyColumn = copy.recordkeyColumn;
        this.filterStringValid = copy.filterStringValid;
        this.filterString = copy.filterString;
        this.dataSource = copy.dataSource;
        this.accessor = copy.accessor;
        this.resolver = copy.resolver;
        this.fragmenter = copy.fragmenter;
        this.analyzer = copy.analyzer;
        this.remoteLogin = copy.remoteLogin;
        this.remoteSecret = copy.remoteSecret;
        this.threadSafe = copy.threadSafe;
    }

    /**
     * Returns a user defined property.
     *
     * @param userProp the lookup user property
     * @return property value as a String
     */
    public String getUserProperty(String userProp) {        
        return requestParametersMap.get("X-GP-" + userProp.toUpperCase());
    }

    /**
     * set the byte serialization of a fragment meta data
     * @param location start, len, and location of the fragment
     */
    public void setFragmentMetadata(byte[] location) {
        this.fragmentMetadata = location;
    }

    /** the byte serialization of a data fragment */
    public byte[] getFragmentMetadata() {
        return fragmentMetadata;
    }

    /** 
     * Gets any custom user data that may have been passed from the 
     * fragmenter. Will mostly be used by the accessor or resolver. 
     */
    public byte[] getFragmentUserData() {
        return userData;
    }
    
    /** 
     * Sets any custom user data that needs to be shared across plugins. 
     * Will mostly be set by the fragmenter. 
     */
    public void setFragmentUserData(byte[] userData) {
        this.userData = userData;
    }

    /** Returns the number of segments in GP. */
    public int getTotalSegments() {
        return totalSegments;
    }

    /** Returns the current segment ID. */
    public int getSegmentId() {
        return segmentId;
    }

    /** Returns true if there is a filter string to parse. */
    public boolean hasFilter() {
        return filterStringValid;
    }

    /** Returns the filter string, <tt>null</tt> if #hasFilter is <tt>false</tt> */
    public String getFilterString() {
        return filterString;
    }

    /** Returns tuple description. */
    public ArrayList<ColumnDescriptor> getTupleDescription() {
        return tupleDescription;
    }

    /** Returns the number of columns in tuple description. */
    public int getColumns() {
        return tupleDescription.size();
    }

    /** Returns column index from tuple description. */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

	/**
	 * Returns the column descriptor of the recordkey column. If the recordkey
	 * column was not specified by the user in the create table statement will
	 * return null.
	 */
    public ColumnDescriptor getRecordkeyColumn() {
        return recordkeyColumn;
    }

    /** Returns the data source of the required resource (i.e a file path or a table name). */
    public String getDataSource() {
        return dataSource;
    }

    /** Sets the data source for the required resource */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /** Returns the ClassName for the java class that was defined as Accessor */
    public String getAccessor() {
        return accessor;
    }

    /** Returns the ClassName for the java class that was defined as Resolver */
    public String getResolver() {
        return resolver;
    }

	/**
	 * Returns the ClassName for the java class that was defined as Fragmenter
	 * or null if no fragmenter was defined
	 */
    public String getFragmenter() {
    	return fragmenter;
    }

	/**
	 * Returns the ClassName for the java class that was defined as Analyzer or
	 * null if no analyzer was defined
	 */
    public String getAnalyzer() {
    	return analyzer;
    }

    /**
     * Returns the contents of pxf_remote_service_login set in Hawq.
     * Should the user set it to an empty string this function will return null.
     *
     * @return remote login details if set, null otherwise
     */
    public String getLogin() {
        return remoteLogin;
    }

    /**
     * Returns the contents of pxf_remote_service_secret set in Hawq.
     * Should the user set it to an empty string this function will return null.
     *
     * @return remote password if set, null otherwise
     */
    public String getSecret() {
        return remoteSecret;
    }

    public boolean isThreadSafe() {
        return threadSafe;
    }

	/** 
	 * Returns a data fragment index. plan to deprecate it in favor of using
	 * getFragmentMetadata().
	 */
	public int getDataFragment() {
		return dataFragment;
	}

}
