package org.apache.hawq.pxf.plugins.hive;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hawq.pxf.api.BasicFilter;
import org.apache.hawq.pxf.api.LogicalFilter;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HiveORCSearchArgumentTest {

    @Test
    public void buildLogicalOperationTree() throws Exception {

        /* Predicate pushdown configuration */
        String filterStr = "a2c23s1d1o2a3c23s1d3o3l0a4c23s1d5o1l1";
        HiveFilterBuilder eval = new HiveFilterBuilder(null);
        Object filter = eval.getFilterObject(filterStr);

        Object current = filter;
        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder();
        buildExpression(filterBuilder, Arrays.asList(filter));
        SearchArgument sarg = filterBuilder.build();
        Assert.assertEquals("and(or(lt(col1, 5), not(lteq(col1, 1))), or(lt(col1, 5), lteq(col1, 3)))", sarg.toFilterPredicate().toString());
    }

    private void buildExpression(SearchArgument.Builder builder, List<Object> filterList) {
        for (Object f : filterList) {
            if (f instanceof LogicalFilter) {
                switch(((LogicalFilter) f).getOperator()) {
                    case HDOP_OR:
                        builder.startOr();
                        break;
                    case HDOP_AND:
                        builder.startAnd();
                        break;
                    case HDOP_NOT:
                        builder.startNot();
                        break;
                }
                buildExpression(builder, ((LogicalFilter) f).getFilterList());
                builder.end();
            } else {
                buildArgument(builder, f);
            }
        }
    }

    private void buildArgument(SearchArgument.Builder builder, Object filterObj) {
        /* The below functions will not be compatible and requires update  with Hive 2.0 APIs */
        BasicFilter filter = (BasicFilter) filterObj;
        int filterColumnIndex = filter.getColumn().index();
        Object filterValue = filter.getConstant().constant();
        Integer[] arr = {};
        ColumnDescriptor filterColumn = new ColumnDescriptor("col1", 1,1, "Integer", arr);
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
