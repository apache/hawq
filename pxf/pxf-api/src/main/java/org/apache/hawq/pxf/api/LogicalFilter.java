package org.apache.hawq.pxf.api;


import java.util.List;

public class LogicalFilter {
    private FilterParser.LogicalOperation operator;
    private List<Object> filterList;

    public LogicalFilter(FilterParser.LogicalOperation operator, List<Object> result) {
        this.operator = operator;
        this.filterList = result;
    }

    public FilterParser.LogicalOperation getOperator() {
        return operator;
    }

    public void setOperator(FilterParser.LogicalOperation operator) {
        this.operator = operator;
    }

    public List<Object> getFilterList() {
        return filterList;
    }

    public void setFilterList(List<Object> filterList) {
        this.filterList = filterList;
    }
}
