package org.apache.hawq.pxf.plugins.hbase;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseFilterBuilderTest {

    @Test
    public void parseNOTExpressionIgnored() throws Exception {
        String filter = "a1c2o1a1c2o2l0l2";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));
    }

    @Test
    public void parseISNULLExpression() throws Exception {
        String filter = "a1o9";
        HBaseTupleDescription desc = mock(HBaseTupleDescription.class);
        HBaseColumnDescriptor column = mock(HBaseColumnDescriptor.class);
        when(desc.getColumn(1)).thenReturn(column);

        byte[] family = new byte[]{};
        byte[] qualifier = new byte[]{};

        when(column.columnFamilyBytes()).thenReturn(family);
        when(column.qualifierBytes()).thenReturn(qualifier);

        HBaseFilterBuilder builder = new HBaseFilterBuilder(desc);
        SingleColumnValueFilter result = (SingleColumnValueFilter) builder.getFilterObject(filter);

        assertNotNull(result);
        assertSame(family, result.getFamily());
        assertSame(qualifier, result.getQualifier());
        assertEquals(CompareFilter.CompareOp.EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);

    }

    @Test
    public void parseISNOTNULLExpression() throws Exception {
        String filter = "a1o10";
        HBaseTupleDescription desc = mock(HBaseTupleDescription.class);
        HBaseColumnDescriptor column = mock(HBaseColumnDescriptor.class);
        when(desc.getColumn(1)).thenReturn(column);

        byte[] family = new byte[]{};
        byte[] qualifier = new byte[]{};

        when(column.columnFamilyBytes()).thenReturn(family);
        when(column.qualifierBytes()).thenReturn(qualifier);

        HBaseFilterBuilder builder = new HBaseFilterBuilder(desc);
        SingleColumnValueFilter result = (SingleColumnValueFilter) builder.getFilterObject(filter);

        assertNotNull(result);
        assertSame(family, result.getFamily());
        assertSame(qualifier, result.getQualifier());
        assertEquals(CompareFilter.CompareOp.NOT_EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);

    }

}