package com.pxf.tests.dataprepares.text;

import java.util.ArrayList;

import com.pivotal.pxfauto.infra.fileformats.IDataPreparer;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;

public class MultiLinePreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable)
			throws Exception {

		Object[] data = new Object[rows];

		for (int j = 0, num1 = 1; j < rows; j++, num1++) {

			ArrayList<String> row = new ArrayList<String>();

			row.add("t" + num1);
			row.add(String.valueOf(num1));

			dataTable.addRow(row);
			data[j] = row;
		}

		return null;
	}
}
