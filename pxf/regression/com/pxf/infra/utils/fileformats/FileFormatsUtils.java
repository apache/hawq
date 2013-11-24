package com.pxf.infra.utils.fileformats;

import com.pxf.infra.structures.tables.basic.Table;

public class FileFormatsUtils {

	public static Object[] prepareData(IFileReader reader, int rows, Table dataTable)
			throws Exception {

		return reader.prepareData(rows, dataTable);
	}
}