package org.apache.hawq.pxf.api;

/**
 * Basic filter provided for cases where the target storage system does not provide it own filter
 * For example: Hbase storage provides its own filter but for a Writable based record in a
 * SequenceFile there is no filter provided and so we need to have a default
 */
public class BasicFilter {
    private FilterParser.Operation oper;
    private FilterParser.ColumnIndex column;
    private FilterParser.Constant constant;

    /**
     * Constructs a BasicFilter.
     *
     * @param oper the parse operation to perform
     * @param column the column index
     * @param constant the constant object
     */
    public BasicFilter(FilterParser.Operation oper, FilterParser.ColumnIndex column, FilterParser.Constant constant) {
        this.oper = oper;
        this.column = column;
        this.constant = constant;
    }

    public FilterParser.Operation getOperation() {
        return oper;
    }

    public FilterParser.ColumnIndex getColumn() {
        return column;
    }

    public FilterParser.Constant getConstant() {
        return constant;
    }
}
