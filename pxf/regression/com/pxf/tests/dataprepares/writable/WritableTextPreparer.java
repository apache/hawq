package com.pxf.tests.dataprepares.writable;

import java.util.ArrayList;

import com.pivotal.parot.fileformats.IDataPreparer;
import com.pivotal.parot.structures.tables.basic.Table;

public class WritableTextPreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		String name = dataTable.getName();
		Object[] data = new Object[rows];

		for (int i = 0; i < rows; i++) {

			ArrayList<String> row = new ArrayList<String>();
			row.add(name + i);
			row.add(String.valueOf((i + 1)));
			row.add(String.valueOf((i * 2)));

			dataTable.addRow(row);
			data[i] = row;
		}

		return data;
	}

}
