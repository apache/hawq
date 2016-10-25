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

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.plugins.jdbc.utils.DbProduct;
import org.apache.hawq.pxf.plugins.jdbc.utils.ByteUtil;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Fragmenter class for JDBC data resources.
 *
 * Extends the {@link Fragmenter} abstract class, with the purpose of transforming
 * an input data path  (an JDBC Database table name  and user request parameters)  into a list of regions
 * that belong to this table.
 * <p>
 * <h4>The parameter Patterns </h4>
 * There are three  parameters,  the format is as follows:<p>
 * <pre>
 * <code>PARTITION_BY=column_name:column_type&RANGE=start_value[:end_value]&INTERVAL=interval_num:interval_unit'</code>
 * </pre>
 * The <code>PARTITION_BY</code> parameter can be split by colon(':'),the <code>column_type</code> current supported : <code>date,int,enum</code> .
 * The Date format is 'yyyy-MM-dd'. <p>
 * The <code>RANGE</code> parameter can be split by colon(':') ,used to identify the starting range of each fragment.
 * The range is left-closed, ie:<code> '>= start_value AND < end_value' </code>.If the <code>column_type</code> is <code>int</code>,
 * the <code>end_value</code> can be empty. If the <code>column_type</code>is <code>enum</code>,the parameter <code>RANGE</code> can be empty. <p>
 * The <code>INTERVAL</code> parameter can be split by colon(':'), indicate the interval value of one fragment.
 * When <code>column_type</code> is <code>date</code>,this parameter must be split by colon, and <code>interval_unit</code> can be <code>year,month,day</code>.
 * When <code>column_type</code> is <code>int</code>, the <code>interval_unit</code> can be empty.
 * When <code>column_type</code> is <code>enum</code>,the <code>INTERVAL</code> parameter can be empty.
 * </p>
 * <p>
 * The syntax examples is :<p>
 * <code>PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month'</code> <p>
 * <code>PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1</code> <p>
 * <code>PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad</code>
 * </p>
 *
 */
public class JdbcPartitionFragmenter extends Fragmenter {
    String[] partition_by = null;
    String[] range = null;
    String[] interval = null;
    PartitionType partitionType = null;
    String partitionColumn = null;
    IntervalType intervalType = null;
    int intervalNum = 1;

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

    //The unit interval, in milliseconds, that is used to estimate the number of slices for the date partition type
    static Map<IntervalType, Long> intervals = new HashMap<IntervalType, Long>();

    static {
        intervals.put(IntervalType.DAY, (long) 24 * 60 * 60 * 1000);
        intervals.put(IntervalType.MONTH, (long) 30 * 24 * 60 * 60 * 1000);//30 day
        intervals.put(IntervalType.YEAR, (long) 365 * 30 * 24 * 60 * 60 * 1000);//365 day
    }

    /**
     * Constructor for JdbcPartitionFragmenter.
     *
     * @param inConf input data such as which Jdbc table to scan
     * @throws JdbcFragmentException
     */
    public JdbcPartitionFragmenter(InputData inConf) throws JdbcFragmentException {
        super(inConf);
        if(inConf.getUserProperty("PARTITION_BY") == null )
            return;
        partition_by = inConf.getUserProperty("PARTITION_BY").split(":");
        partitionColumn = partition_by[0];
        partitionType = PartitionType.getType(partition_by[1]);

        range = inConf.getUserProperty("RANGE").split(":");

        //parse and validate parameter-INTERVAL
        if (inConf.getUserProperty("INTERVAL") != null) {
            interval = inConf.getUserProperty("INTERVAL").split(":");
            intervalNum = Integer.parseInt(interval[0]);
            if (interval.length > 1)
                intervalType = IntervalType.type(interval[1]);
        }
        if (intervalNum < 1)
            throw new JdbcFragmentException("The parameter{INTERVAL} must > 1, but actual is '" + intervalNum+"'");
    }
    /**
     * Returns statistics for Jdbc table. Currently it's not implemented.
     */
    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for Jdbc plugin is not supported");
    }



    /**
     * Returns list of fragments containing all of the
     * Jdbc table data.
     *
     * @return a list of fragments
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        switch (partitionType) {
            case DATE: {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                Date t_start = df.parse(range[0]);
                Date t_end = df.parse(range[1]);
                int curr_interval = intervalNum;

                Calendar frag_start = Calendar.getInstance();
                Calendar c_end = Calendar.getInstance();
                frag_start.setTime(t_start);
                c_end.setTime(t_end);
                while (frag_start.before(c_end)) {//|| frag_start.compareTo(c_end) == 0) {
                    Calendar frag_end = (Calendar) frag_start.clone();
                    switch (intervalType) {
                        case DAY:
                            frag_end.add(Calendar.DAY_OF_MONTH, curr_interval);
                            break;
                        case MONTH:
                            frag_end.add(Calendar.MONTH, curr_interval);
                            break;
                        //case YEAR:
                        default:
                            frag_end.add(Calendar.YEAR, curr_interval);
                            break;
                    }
                    if (frag_end.after(c_end)) frag_end = (Calendar) c_end.clone();

                    //make metadata of this fragment , converts the date to a millisecond,then get bytes.
                    byte[] ms_start = ByteUtil.getBytes(frag_start.getTimeInMillis());
                    byte[] ms_end = ByteUtil.getBytes(frag_end.getTimeInMillis());
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(ms_start, ms_end);

                    byte[] userData = new byte[0];
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, userData);
                    fragments.add(fragment);

                    //continue next fragment.
                    frag_start = frag_end;
                }
                break;
            }
            case INT: {
                int i_start = Integer.parseInt(range[0]);
                int i_end = Integer.parseInt(range[1]);
                int curr_interval = intervalNum;

                //validate : curr_interval > 0
                int frag_start = i_start;
                while (frag_start < i_end) {
                    int frag_end = frag_start + curr_interval;
                    if (frag_end > i_end) frag_end = i_end;

                    byte[] b_start = ByteUtil.getBytes(frag_start);
                    byte[] b_end = ByteUtil.getBytes(frag_end);
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(b_start, b_end);

                    byte[] userData = new byte[0];
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, userData);
                    fragments.add(fragment);

                    //continue next fragment.
                    frag_start = frag_end;// + 1;
                }
                break;
            }
            case ENUM:
                for (String frag : range) {
                    byte[] fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, new byte[0]);
                    fragments.add(fragment);
                }
                break;
        }

        return prepareHosts(fragments);
    }

    /**
     * For each fragment , assigned a host address.
     * In Jdbc Plugin, 'replicas' is the host address of the PXF engine that is running, not the database engine.
     * Since the other PXF host addresses can not be probed, only the host name of the current PXF engine is returned.
     * @param fragments a list of fragments
     * @return a list of fragments that assigned hosts.
     * @throws Exception
     */
    public static List<Fragment> prepareHosts(List<Fragment> fragments) throws Exception {
        for (Fragment fragment : fragments) {
            String pxfhost =  InetAddress.getLocalHost().getHostAddress();
            String[] hosts = new String[]{pxfhost};
            fragment.setReplicas(hosts);
        }

        return fragments;
    }

    public String buildFragmenterSql(String db_product, String origin_sql) {
        byte[] meta = inputData.getFragmentMetadata();
        if (meta == null)
            return origin_sql;

        DbProduct dbProduct = DbProduct.getDbProduct(db_product);

        StringBuilder sb = new StringBuilder(origin_sql);
        if (!origin_sql.contains("WHERE"))
            sb.append(" WHERE 1=1 ");

        sb.append(" AND ");
        switch (partitionType) {
            case DATE: {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                //parse metadata of this fragment
                //validate：the length of metadata == 16 (long)
                byte[][] newb = ByteUtil.splitBytes(meta, 8);
                Date frag_start = new Date(ByteUtil.toLong(newb[0]));
                Date frag_end = new Date(ByteUtil.toLong(newb[1]));

                sb.append(partitionColumn).append(" >= ").append(dbProduct.wrapDate(df.format(frag_start)));
                sb.append(" AND ");
                sb.append(partitionColumn).append(" < ").append(dbProduct.wrapDate(df.format(frag_end)));

                break;
            }
            case INT: {
                //validate：the length of metadata ==8 （int)
                byte[][] newb = ByteUtil.splitBytes(meta, 4);
                int frag_start = ByteUtil.toInt(newb[0]);
                int frag_end = ByteUtil.toInt(newb[1]);
                sb.append(partitionColumn).append(" >= ").append(frag_start);
                sb.append(" AND ");
                sb.append(partitionColumn).append(" < ").append(frag_end);
                break;
            }
            case ENUM:
                sb.append(partitionColumn).append("='").append(new String(meta)).append("'");
                break;
        }
        return sb.toString();
    }
}
