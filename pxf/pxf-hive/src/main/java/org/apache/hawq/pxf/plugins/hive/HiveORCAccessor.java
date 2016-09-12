package org.apache.hawq.pxf.plugins.hive;

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


import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.commons.lang.StringUtils;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hawq.pxf.plugins.hive.HiveInputFormatFragmenter.PXF_HIVE_SERDES;

/**
 * Specialization of HiveAccessor for a Hive table that stores only ORC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as ORC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveORCAccessor extends HiveAccessor {

    private final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
    private final String READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
    private final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
    private final String SARG_PUSHDOWN = "sarg.pushdown";

    /**
     * Constructs a HiveORCFileAccessor.
     *
     * @param input input containing user data
     * @throws Exception if user data was wrong
     */
    public HiveORCAccessor(InputData input) throws Exception {
        super(input, new OrcInputFormat());
        String[] toks = HiveInputFormatFragmenter.parseToks(input, PXF_HIVE_SERDES.ORC_SERDE.name());
        initPartitionFields(toks[HiveInputFormatFragmenter.TOK_KEYS]);
        filterInFragmenter = new Boolean(toks[HiveInputFormatFragmenter.TOK_FILTER_DONE]);
    }

    @Override
    public boolean openForRead() throws Exception {
        addColumns();
        addFilters();
        return super.openForRead();
    }

    /**
     * Adds the table tuple description to JobConf ojbect
     * so only these columns will be returned.
     */
    private void addColumns() throws Exception {

        List<Integer> colIds = new ArrayList<Integer>();
        List<String> colNames = new ArrayList<String>();
        for(ColumnDescriptor col: inputData.getTupleDescription()) {
            if(col.isProjected()) {
                colIds.add(col.columnIndex());
                colNames.add(col.columnName());
            }
        }
        jobConf.set(READ_ALL_COLUMNS, "false");
        jobConf.set(READ_COLUMN_IDS_CONF_STR, StringUtils.join(colIds, ","));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, StringUtils.join(colNames, ","));
    }

    /**
     * Uses {@link HiveFilterBuilder} to translate a filter string into a
     * Hive {@link SearchArgument} object. The result is added as a filter to
     * JobConf object
     */
    private void addFilters() throws Exception {
        if (!inputData.hasFilter()) {
            return;
        }

        /* Predicate pushdown configuration */
        String filterStr = inputData.getFilterString();
        HiveFilterBuilder eval = new HiveFilterBuilder(inputData);
        Object filter = eval.getFilterObject(filterStr);

        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder();
        filterBuilder.startAnd();
        if (filter instanceof List) {
            for (Object f : (List<?>) filter) {
                buildArgument(filterBuilder, f);
            }
        } else {
            buildArgument(filterBuilder, filter);
        }
        filterBuilder.end();
        SearchArgument sarg = filterBuilder.build();
        jobConf.set(SARG_PUSHDOWN, sarg.toKryo());
    }

    private void buildArgument(SearchArgument.Builder builder, Object filterObj) {
        /* The below functions will not be compatible and requires update  with Hive 2.0 APIs */
        FilterParser.BasicFilter filter = (FilterParser.BasicFilter) filterObj;
        int filterColumnIndex = filter.getColumn().index();
        Object filterValue = filter.getConstant().constant();
        ColumnDescriptor filterColumn = inputData.getColumn(filterColumnIndex);
        String filterColumnName = filterColumn.columnName();

        switch(filter.getOperation()) {
            case HDOP_LT:
                builder.lessThan(filterColumnName, filterValue);
                break;
            case HDOP_GT:
                builder.startNot().lessThanEquals(filterColumnName, filterValue).end();
                break;
            case HDOP_LE:
                builder.lessThanEquals(filterColumnName, filterValue);
                break;
            case HDOP_GE:
                builder.startNot().lessThanEquals(filterColumnName, filterValue).end();
                break;
            case HDOP_EQ:
                builder.equals(filterColumnName, filterValue);
                break;
            case HDOP_NE:
                builder.startNot().equals(filterColumnName, filterValue).end();
                break;
        }
        return;
    }

}
