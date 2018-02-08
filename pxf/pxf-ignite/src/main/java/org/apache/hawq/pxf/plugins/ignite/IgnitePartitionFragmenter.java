package org.apache.hawq.pxf.plugins.ignite;

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

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Fragmenter class for Ignite data resources.
 * @note At the moment this fragmenter works just like the JDBC's one
 *
 * Aside from implementing PXF {@link Fragmenter},
 * this class also transforms input data path (given by user) by adding the
 * information on splitting the request into a few requests for different regions.
 * <br>
 * The parameter Patterns
 * <br>
 * There are three  parameters, the format is as follows:
 * <br>
 * <pre>
 * <code>PARTITION_BY=column_name:column_type&amp;RANGE=start_value[:end_value]&amp;INTERVAL=interval_num[:interval_unit]</code>
 * </pre>
 * The <code>PARTITION_BY</code> parameter can be split by colon(':'),the <code>column_type</code> current supported : <code>date,int,enum</code> .
 * The Date format is 'yyyy-MM-dd'. <br>
 * The <code>RANGE</code> parameter can be split by colon(':') ,used to identify the starting range of each fragment.
 * The range is left-closed, ie:<code> '&gt;= start_value AND &lt; end_value' </code>.If the <code>column_type</code> is <code>int</code>,
 * the <code>end_value</code> can be empty. If the <code>column_type</code>is <code>enum</code>,the parameter <code>RANGE</code> can be empty. <br>
 * The <code>INTERVAL</code> parameter can be split by colon(':'), indicate the interval value of one fragment.
 * When <code>column_type</code> is <code>date</code>,this parameter must be split by colon, and <code>interval_unit</code> can be <code>year,month,day</code>.
 * When <code>column_type</code> is <code>int</code>, the <code>interval_unit</code> can be empty.
 * When <code>column_type</code> is <code>enum</code>,the <code>INTERVAL</code> parameter can be empty.
 * <br>
 * <p>
 * The syntax examples is:
 * <br>
 * <code>PARTITION_BY=createdate:date&amp;RANGE=2008-01-01:2010-01-01&amp;INTERVAL=1:month'</code>
 * <br>
 * <code>PARTITION_BY=year:int&amp;RANGE=2008:2010&amp;INTERVAL=1</code>
 * <br>
 * <code>PARTITION_BY=grade:enum&amp;RANGE=excellent:good:general:bad</code>
 * </p>
 */
public class IgnitePartitionFragmenter extends Fragmenter {
    Log LOG = LogFactory.getLog(IgnitePartitionFragmenter.class);

    String[] partitionBy = null;
    String[] range = null;
    String[] interval = null;
    PartitionType partitionType = null;
    String partitionColumn = null;
    IntervalType intervalType = null;
    int intervalNum = 1;

    //when partitionType is DATE,it is valid
    Calendar rangeStart = null;
    Calendar rangeEnd = null;


    enum PartitionType {
        DATE,
        INT,
        ENUM;

        public static PartitionType getType(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    enum IntervalType {
        DAY,
        MONTH,
        YEAR;

        public static IntervalType type(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    /**
     * @throws UserDataException if the request parameter is malformed
     */
    public IgnitePartitionFragmenter(InputData inConf) throws UserDataException {
        super(inConf);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor started");
        }

        if (inConf.getUserProperty("PARTITION_BY") == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Constructor successful; partition was not used");
            }
            return;
        }

        try {
            partitionBy = inConf.getUserProperty("PARTITION_BY").split(":");
            partitionColumn = partitionBy[0];
            partitionType = PartitionType.getType(partitionBy[1]);
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e1) {
            throw new UserDataException("The parameter 'PARTITION_BY' invalid, the pattern is 'column_name:date|int|enum'");
        }

        //parse and validate parameter-RANGE
        try {
            String rangeStr = inConf.getUserProperty("RANGE");
            if (rangeStr != null) {
                range = rangeStr.split(":");
                if (range.length == 1 && partitionType != PartitionType.ENUM) {
                    throw new UserDataException("The parameter 'RANGE' does not specify '[:end_value]'");
                }
            } 
            else {
                throw new UserDataException("The parameter 'RANGE' must be specified along with 'PARTITION_BY'");
            }
        }
        catch (IllegalArgumentException e) {
            throw new UserDataException("The parameter 'RANGE' invalid, the pattern is 'start_value[:end_value]'");
        }

        //parse and validate parameter-INTERVAL
        try {
            String intervalStr = inConf.getUserProperty("INTERVAL");
            if (intervalStr != null) {
                interval = intervalStr.split(":");
                intervalNum = Integer.parseInt(interval[0]);
                if (interval.length > 1) {
                    intervalType = IntervalType.type(interval[1]);
                }
                if (interval.length == 1 && partitionType == PartitionType.DATE) {
                    throw new UserDataException("The parameter 'INTERVAL' does not specify unit [:year|month|day]");
                }
            } 
            else if (partitionType != PartitionType.ENUM) {
                throw new UserDataException("The parameter 'INTERVAL' must be specified along with 'PARTITION_BY'");
            }
            if (intervalNum < 1) {
                throw new UserDataException("The parameter 'INTERVAL' must > 1, but actual is '" + intervalNum + "'");
            }
        } 
        catch (IllegalArgumentException e) {
            throw new UserDataException("The parameter 'INTERVAL' invalid, the pattern is 'interval_num[:interval_unit]'");
        }

        //parse any date values
        try {
            if (partitionType == PartitionType.DATE) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                rangeStart = Calendar.getInstance();
                rangeStart.setTime(df.parse(range[0]));
                rangeEnd = Calendar.getInstance();
                rangeEnd.setTime(df.parse(range[1]));
            }
        } catch (ParseException e) {
            throw new UserDataException("The parameter 'RANGE' has invalid date format. Expected format is 'YYYY-MM-DD'");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor successful; some partition used");
        }
    }

    /**
     * Returns statistics for  Ignite table. Currently it's not implemented.
     * @throws UnsupportedOperationException
     */
    @Override
    public FragmentsStats getFragmentsStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for Ignite plugin is not supported");
    }

    /**
     * Returns list of fragments for Ignite table queries
     *
     * @return a list of fragments
     * @throws UnknownHostException from java.net.InetAddress.getLocalHost().getHostAddress()
     */
    @Override
    public List<Fragment> getFragments() throws UnknownHostException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getFragments() called; dataSource is '" + inputData.getDataSource() + "'");
        }

        // Always use 'localhost' as a replica holder
        // The replica holder is a *PXF* host which can process the requested query
        // Note that this is not an Ignite host
        String[] replicaHostAddressWrapped = new String[]{InetAddress.getLocalHost().getHostAddress()};
        byte[] fragmentMetadata = null;
        byte[] fragmentUserdata = null;

        if (partitionType == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getFragments() found no partition");
            }
            Fragment fragment = new Fragment(inputData.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
            fragments.add(fragment);
            if (LOG.isDebugEnabled()) {
                LOG.debug("getFragments() successful. Total fragments: " + fragments.size());
            }
            return fragments;
        }

        switch (partitionType) {
            case DATE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found DATE partition");
                }
                int currInterval = intervalNum;

                Calendar fragStart = rangeStart;
                while (fragStart.before(rangeEnd)) {
                    Calendar fragEnd = (Calendar) fragStart.clone();
                    switch (intervalType) {
                        case DAY:
                            fragEnd.add(Calendar.DAY_OF_MONTH, currInterval);
                            break;
                        case MONTH:
                            fragEnd.add(Calendar.MONTH, currInterval);
                            break;
                        case YEAR:
                            fragEnd.add(Calendar.YEAR, currInterval);
                            break;
                    }
                    if (fragEnd.after(rangeEnd))
                        fragEnd = (Calendar) rangeEnd.clone();

                    // Build Fragment.metadata: convert the date to a millisecond, then get bytes.
                    byte[] msStart = ByteUtil.getBytes(fragStart.getTimeInMillis());
                    byte[] msEnd = ByteUtil.getBytes(fragEnd.getTimeInMillis());
                    fragmentMetadata = ByteUtil.mergeBytes(msStart, msEnd);

                    Fragment fragment = new Fragment(inputData.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
                    fragments.add(fragment);

                    // Continue the previous fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case INT: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found INT partition");
                }
                int rangeStart = Integer.parseInt(range[0]);
                int rangeEnd = Integer.parseInt(range[1]);
                int currInterval = intervalNum;

                // Validate : curr_interval > 0
                int fragStart = rangeStart;
                while (fragStart < rangeEnd) {
                    int fragEnd = fragStart + currInterval;
                    if (fragEnd > rangeEnd) fragEnd = rangeEnd;

                    byte[] bStart = ByteUtil.getBytes(fragStart);
                    byte[] bEnd = ByteUtil.getBytes(fragEnd);
                    fragmentMetadata = ByteUtil.mergeBytes(bStart, bEnd);

                    Fragment fragment = new Fragment(inputData.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
                    fragments.add(fragment);

                    // Continue the previous fragment
                    fragStart = fragEnd;// + 1;
                }
                break;
            }
            case ENUM:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found ENUM partition");
                }
                for (String frag : range) {
                    fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(inputData.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
                    fragments.add(fragment);
                }
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getFragments() successful. Total fragments: " + fragments.size());
        }
        return fragments;
    }

    public StringBuilder buildFragmenterSql(StringBuilder sb) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("buildFragmenterSql() called");
        }

        byte[] meta = inputData.getFragmentMetadata();
        if (meta == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fragment metadata is null; No partition constraints added");
            }
            return sb;
        }

        if (!sb.toString().contains("WHERE")) {
            sb.append(" WHERE 1=1 ");
        }
        sb.append(" AND ");

        switch (partitionType) {
            case DATE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DATE partition found");
                }
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                //parse metadata of this fragment
                byte[][] newb = ByteUtil.splitBytes(meta, 8);
                Date fragStart = new Date(ByteUtil.toLong(newb[0]));
                Date fragEnd = new Date(ByteUtil.toLong(newb[1]));

                sb.append(partitionColumn).append(">=").append("date'" + df.format(fragStart) + "'");
                sb.append(" AND ");
                sb.append(partitionColumn).append("<").append("date'" + df.format(fragEnd) + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DATE partition constraints added");
                }
                break;
            }
            case INT: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("INT partition found");
                }
                byte[][] newb = ByteUtil.splitBytes(meta, 4);
                int fragStart = ByteUtil.toInt(newb[0]);
                int fragEnd = ByteUtil.toInt(newb[1]);
                sb.append(partitionColumn).append(">=").append(fragStart);
                sb.append(" AND ");
                sb.append(partitionColumn).append("<").append(fragEnd);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("INT partition constraints added");
                }
                break;
            }
            case ENUM:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ENUM partition found");
                }
                sb.append(partitionColumn).append("='").append(new String(meta)).append("'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ENUM partition constraints added");
                }
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("buildFragmenterSql() successful");
        }
        return sb;
    }
}
