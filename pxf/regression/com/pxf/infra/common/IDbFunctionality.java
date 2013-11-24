package com.pxf.infra.common;

import java.util.ArrayList;

import com.pxf.infra.structures.tables.basic.Table;

public interface IDbFunctionality {

	public void createTable(Table table) throws Exception;

	public void dropTable(Table table) throws Exception;

	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail)
			throws Exception;

	/**
	 * Insert data from source Table data to target Table.
	 * 
	 * @param source
	 *            table to read the data from
	 * @param target
	 *            table to get the required table to insert to
	 * @throws Exception
	 */
	public void insertData(Table source, Table target) throws Exception;

	public void createDataBase(String schemaName, boolean ignoreFail)
			throws Exception;

	/**
	 * get tables list for data base
	 * 
	 * @param dataBaseName
	 * @return List of Table names for data base
	 * @throws Exception
	 */
	public ArrayList<String> getTableList(String dataBaseName) throws Exception;

	public void getTableData(Table table) throws Exception;
}
