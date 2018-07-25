package org.apache.hawq.pxf.plugins.jdbc;

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
import org.apache.hawq.pxf.plugins.jdbc.utils.ByteUtil;
import org.apache.hawq.pxf.plugins.jdbc.utils.DbProduct;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * JDBC fragmenter
 *
 * Splits the query to allow multiple simultaneous SELECTs
 */
public class JdbcPartitionFragmenter extends Fragmenter {
    /**
     * Insert fragment constraints into the SQL query.
     *
     * @param inputData InputData of the fragment
     * @param dbName Database name (affects the behaviour for DATE partitions)
     * @param query SQL query to insert constraints to. The query may may contain other WHERE statements
     */
    public static void buildFragmenterSql(InputData inputData, String dbName, StringBuilder query) {
        if (inputData.getUserProperty("PARTITION_BY") == null) {
            return;
        }

        byte[] meta = inputData.getFragmentMetadata();
        if (meta == null) {
            return;
        }
        String[] partitionBy = inputData.getUserProperty("PARTITION_BY").split(":");
        String partitionColumn = partitionBy[0];
        PartitionType partitionType = PartitionType.typeOf(partitionBy[1]);
        DbProduct dbProduct = DbProduct.getDbProduct(dbName);

        if (!query.toString().contains("WHERE")) {
            query.append(" WHERE ");
        }
        else {
            query.append(" AND ");
        }

        switch (partitionType) {
            case DATE: {
                byte[][] newb = ByteUtil.splitBytes(meta);
                Date fragStart = new Date(ByteUtil.toLong(newb[0]));
                Date fragEnd = new Date(ByteUtil.toLong(newb[1]));

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                query.append(partitionColumn).append(" >= ").append(dbProduct.wrapDate(df.format(fragStart)));
                query.append(" AND ");
                query.append(partitionColumn).append(" < ").append(dbProduct.wrapDate(df.format(fragEnd)));

                break;
            }
            case INT: {
                byte[][] newb = ByteUtil.splitBytes(meta);
                long fragStart = ByteUtil.toLong(newb[0]);
                long fragEnd = ByteUtil.toLong(newb[1]);

                query.append(partitionColumn).append(" >= ").append(fragStart);
                query.append(" AND ");
                query.append(partitionColumn).append(" < ").append(fragEnd);
                break;
            }
            case ENUM: {
                query.append(partitionColumn).append(" = '").append(new String(meta)).append("'");
                break;
            }
        }
    }

    /**
     * Class constructor.
     *
     * @param inputData PXF InputData
     * @throws UserDataException if the request parameter is malformed
     */
    public JdbcPartitionFragmenter(InputData inputData) throws UserDataException {
        super(inputData);
        if (inputData.getUserProperty("PARTITION_BY") == null) {
            return;
        }

        // PARTITION_BY
        try {
            partitionType = PartitionType.typeOf(
                inputData.getUserProperty("PARTITION_BY").split(":")[1]
            );
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException ex) {
            throw new UserDataException("The parameter 'PARTITION_BY' is invalid. The pattern is '<column_name>:date|int|enum'");
        }

        // RANGE
        try {
            String rangeStr = inputData.getUserProperty("RANGE");
            if (rangeStr != null) {
                range = rangeStr.split(":");
                if (range.length == 1 && partitionType != PartitionType.ENUM) {
                    throw new UserDataException("The parameter 'RANGE' must specify ':<end_value>' for this PARTITION_TYPE");
                }
            }
            else {
                throw new UserDataException("The parameter 'RANGE' must be specified along with 'PARTITION_BY'");
            }

            if (partitionType == PartitionType.DATE) {
                // Parse DATE partition type values
                try {
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                    rangeDateStart = Calendar.getInstance();
                    rangeDateStart.setTime(df.parse(range[0]));
                    rangeDateEnd = Calendar.getInstance();
                    rangeDateEnd.setTime(df.parse(range[1]));
                }
                catch (ParseException e) {
                    throw new UserDataException("The parameter 'RANGE' has invalid date format. The correct format is 'yyyy-MM-dd'");
                }
            }
            else if (partitionType == PartitionType.INT) {
                // Parse INT partition type values
                try {
                    rangeIntStart = Long.parseLong(range[0]);
                    rangeIntEnd = Long.parseLong(range[1]);
                }
                catch (NumberFormatException e) {
                    throw new UserDataException("The parameter 'RANGE' is invalid. Both range boundaries must be integers");
                }
            }
        }
        catch (IllegalArgumentException ex) {
            throw new UserDataException("The parameter 'RANGE' is invalid. The pattern is '<start_value>[:<end_value>]'");
        }

        // INTERVAL
        try {
            String intervalStr = inputData.getUserProperty("INTERVAL");
            if (intervalStr != null) {
                String[] interval = intervalStr.split(":");
                try {
                    intervalNum = Long.parseLong(interval[0]);
                    if (intervalNum < 1) {
                        throw new UserDataException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + intervalNum);
                    }
                }
                catch (NumberFormatException ex) {
                    throw new UserDataException("The '<interval_num>' in parameter 'INTERVAL' must be an integer");
                }

                // Intervals of type DATE
                if (interval.length > 1) {
                    intervalType = IntervalType.typeOf(interval[1]);
                }
                if (interval.length == 1 && partitionType == PartitionType.DATE) {
                    throw new UserDataException("The parameter 'INTERVAL' must specify unit (':year|month|day') for the PARTITION_TYPE = 'DATE'");
                }
            }
            else if (partitionType != PartitionType.ENUM) {
                throw new UserDataException("The parameter 'INTERVAL' must be specified along with 'PARTITION_BY' for this PARTITION_TYPE");
            }
        }
        catch (IllegalArgumentException ex) {
            throw new UserDataException("The parameter 'INTERVAL' is invalid. The pattern is '<interval_num>[:<interval_unit>]'");
        }
    }

    /**
     * @throws UnsupportedOperationException ANALYZE for Jdbc plugin is not supported
     */
    @Override
    public FragmentsStats getFragmentsStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for JDBC plugin is not supported");
    }

    /**
     * getFragments() implementation
     *
     * @return a list of fragments to be passed to PXF segments
     */
    @Override
    public List<Fragment> getFragments() {
        if (partitionType == null) {
            // No partition case
            Fragment fragment = new Fragment(inputData.getDataSource(), pxfHosts, null);
            fragments.add(fragment);
            return fragments;
        }

        switch (partitionType) {
            case DATE: {
                Calendar fragStart = rangeDateStart;

                while (fragStart.before(rangeDateEnd)) {
                    // Calculate a new fragment
                    Calendar fragEnd = (Calendar)fragStart.clone();
                    switch (intervalType) {
                        case DAY:
                            fragEnd.add(Calendar.DAY_OF_MONTH, (int)intervalNum);
                            break;
                        case MONTH:
                            fragEnd.add(Calendar.MONTH, (int)intervalNum);
                            break;
                        case YEAR:
                            fragEnd.add(Calendar.YEAR, (int)intervalNum);
                            break;
                    }
                    if (fragEnd.after(rangeDateEnd))
                        fragEnd = (Calendar)rangeDateEnd.clone();

                    // Convert to byte[]
                    byte[] msStart = ByteUtil.getBytes(fragStart.getTimeInMillis());
                    byte[] msEnd = ByteUtil.getBytes(fragEnd.getTimeInMillis());
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(msStart, msEnd);

                    // Write fragment
                    Fragment fragment = new Fragment(inputData.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case INT: {
                long fragStart = rangeIntStart;

                while (fragStart < rangeIntEnd) {
                    // Calculate a new fragment
                    long fragEnd = fragStart + intervalNum;
                    if (fragEnd > rangeIntEnd) {
                        fragEnd = rangeIntEnd;
                    }

                    // Convert to byte[]
                    byte[] bStart = ByteUtil.getBytes(fragStart);
                    byte[] bEnd = ByteUtil.getBytes(fragEnd);
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(bStart, bEnd);

                    // Write fragment
                    Fragment fragment = new Fragment(inputData.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case ENUM: {
                for (String frag : range) {
                    byte[] fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(inputData.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);
                }
                break;
            }
        }

        return fragments;
    }

    // Partition parameters (filled by class constructor)
    private String[] range = null;
    private PartitionType partitionType = null;
    private long intervalNum;

    // Partition parameters for INT partitions (filled by class constructor)
    private long rangeIntStart;
    private long rangeIntEnd;

    // Partition parameters for DATE partitions (filled by class constructor)
    private IntervalType intervalType;
    private Calendar rangeDateStart;
    private Calendar rangeDateEnd;

    private static enum PartitionType {
        DATE,
        INT,
        ENUM;

        public static PartitionType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    private static enum IntervalType {
        DAY,
        MONTH,
        YEAR;

        public static IntervalType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    // A PXF engine to use as a host for fragments
    private static final String[] pxfHosts;
    static {
        String[] localhost = {"localhost"};
        try {
            localhost[0] = InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException ex) {
            // It is always possible to get 'localhost' address
        }
        pxfHosts = localhost;
    }
}
