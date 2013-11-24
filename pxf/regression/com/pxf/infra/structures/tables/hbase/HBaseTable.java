package com.pxf.infra.structures.tables.hbase;

import java.util.ArrayList;

import com.pxf.infra.structures.tables.basic.Table;

/**
 * represents HBase Table
 */
public class HBaseTable extends Table {

	private ArrayList<String> cols;

	public HBaseTable(String name, String[] fields) {
		super(name, fields);
	}

	public ArrayList<String> getCols() {
		return cols;
	}

	public void setCols(ArrayList<String> cols) {
		this.cols = cols;
	}

}
