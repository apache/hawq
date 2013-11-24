package com.pxf.infra.structures.tables.pxf;

/**
 * represents HAWQ -> PXF Readable External Table
 */
public class ReadbleExternalTable extends ExternalTable {

	public ReadbleExternalTable(String name, String[] fields, String path, String format) {
		super(name, fields, path, format);
	}

}
