package com.pxf.tests.dataprepares.text;

import java.util.ArrayList;

import com.pivotal.pxfauto.infra.fileformats.IDataPreparer;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;

public class QuatedLineTextPreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		Object[] data = new Object[rows];

		for (int i = 0, num1 = 1; i < rows; i++, num1++) {

			ArrayList<String> row = new ArrayList<String>();

			if (i % 2 != 0) {

				row.add(String.valueOf(num1));
				row.add("\"aaa_" + num1 + "\"");
				row.add(String.valueOf(num1 + 1));

				dataTable.addRow(row);
				data[i] = row;

			} else {
				row.add(String.valueOf(num1));
				row.add("\"aaa_" + num1 + "\n_c\"");
				row.add(String.valueOf(num1 + 1));

				dataTable.addRow(row);
				data[i] = row;
			}
		}

		return data;
	}
}
