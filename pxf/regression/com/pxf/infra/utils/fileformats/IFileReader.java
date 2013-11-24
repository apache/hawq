package com.pxf.infra.utils.fileformats;

import com.pxf.infra.structures.tables.basic.Table;

public interface IFileReader {

	public Object[] prepareData(int rows, Table dataTable) throws Exception;
}
