package org.apache.hawq.pxf.api.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
    protected String profile;
    protected String accessor;
    protected String resolver;
    protected String fragmenter;
    protected String metadata;
    protected String remoteLogin;
    protected String remoteSecret;
    protected int dataFragment; /* should be deprecated */

    /**
     * When false the bridge has to run in synchronized mode. default value -
     * true.
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
     * Constructs an InputData from a copy. Used to create from an extending
     * class.
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
        this.profile = copy.profile;
        this.accessor = copy.accessor;
        this.resolver = copy.resolver;
        this.fragmenter = copy.fragmenter;
        this.metadata = copy.metadata;
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
     * Sets the byte serialization of a fragment meta data.
     *
     * @param location start, len, and location of the fragment
     */
    public void setFragmentMetadata(byte[] location) {
        this.fragmentMetadata = location;
    }

    /**
     * The byte serialization of a data fragment.
     *
     * @return serialized fragment metadata
     */
    public byte[] getFragmentMetadata() {
        return fragmentMetadata;
    }

    /**
     * Gets any custom user data that may have been passed from the fragmenter.
     * Will mostly be used by the accessor or resolver.
     *
     * @return fragment user data
     */
    public byte[] getFragmentUserData() {
        return userData;
    }

    /**
     * Sets any custom user data that needs to be shared across plugins. Will
     * mostly be set by the fragmenter.
     *
     * @param userData user data
     */
    public void setFragmentUserData(byte[] userData) {
        this.userData = userData;
    }

    /**
     * Returns the number of segments in HAWQ.
     *
     * @return number of segments
     */
    public int getTotalSegments() {
        return totalSegments;
    }

    /**
     * Returns the current segment ID in HAWQ.
     *
     * @return current segment ID
     */
    public int getSegmentId() {
        return segmentId;
    }

    /**
     * Returns true if there is a filter string to parse.
     *
     * @return whether there is a filter string
     */
    public boolean hasFilter() {
        return filterStringValid;
    }

    /**
     * Returns the filter string, <tt>null</tt> if #hasFilter is <tt>false</tt>.
     *
     * @return the filter string or null
     */
    public String getFilterString() {
        return filterString;
    }

    /**
     * Returns tuple description.
     *
     * @return tuple description
     */
    public ArrayList<ColumnDescriptor> getTupleDescription() {
        return tupleDescription;
    }

    /**
     * Returns the number of columns in tuple description.
     *
     * @return number of columns
     */
    public int getColumns() {
        return tupleDescription.size();
    }

    /**
     * Returns column index from tuple description.
     *
     * @param index index of column
     * @return column by index
     */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    /**
     * Returns the column descriptor of the recordkey column. If the recordkey
     * column was not specified by the user in the create table statement will
     * return null.
     *
     * @return column of record key or null
     */
    public ColumnDescriptor getRecordkeyColumn() {
        return recordkeyColumn;
    }

    /**
     * Returns the data source of the required resource (i.e a file path or a
     * table name).
     *
     * @return data source
     */
    public String getDataSource() {
        return dataSource;
    }

    /**
     * Sets the data source for the required resource.
     *
     * @param dataSource data source to be set
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the profile name.
     *
     * @return name of profile
     */
    public String getProfile() {
        return profile;
    }

    /**
     * Returns the ClassName for the java class that was defined as Accessor.
     *
     * @return class name for Accessor
     */
    public String getAccessor() {
        return accessor;
    }

    /**
     * Returns the ClassName for the java class that was defined as Resolver.
     *
     * @return class name for Resolver
     */
    public String getResolver() {
        return resolver;
    }

    /**
     * Returns the ClassName for the java class that was defined as Fragmenter
     * or null if no fragmenter was defined.
     *
     * @return class name for Fragmenter or null
     */
    public String getFragmenter() {
        return fragmenter;
    }

    /**
     * Returns the ClassName for the java class that was defined as Metadata
     * or null if no metadata was defined.
     *
     * @return class name for METADATA or null
     */
    public String getMetadata() {
        return metadata;
    }

    /**
     * Returns the contents of pxf_remote_service_login set in Hawq. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote login details if set, null otherwise
     */
    public String getLogin() {
        return remoteLogin;
    }

    /**
     * Returns the contents of pxf_remote_service_secret set in Hawq. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote password if set, null otherwise
     */
    public String getSecret() {
        return remoteSecret;
    }

    /**
     * Returns whether this request is thread safe.
     * If it is not, request will be handled consequentially and not in parallel.
     *
     * @return whether the request is thread safe
     */
    public boolean isThreadSafe() {
        return threadSafe;
    }

    /**
     * Returns a data fragment index. plan to deprecate it in favor of using
     * getFragmentMetadata().
     *
     * @return data fragment index
     */
    public int getDataFragment() {
        return dataFragment;
    }

}
