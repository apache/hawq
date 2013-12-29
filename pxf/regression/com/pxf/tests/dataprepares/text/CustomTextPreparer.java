package com.pxf.tests.dataprepares.text;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import com.pivotal.pxfauto.infra.fileformats.IDataPreparer;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;

public class CustomTextPreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable)
			throws Exception {

		Object[] data = new Object[rows];

		for (int i = 0, num1 = 1; i < rows; i++, num1++) {

			ArrayList<String> row = new ArrayList<String>();

			row.add("s_" + num1);
			row.add("s_" + num1 * 10);
			row.add("s_" + num1 * 100);

			Timestamp tms = new Timestamp(Calendar.getInstance()
					.getTime()
					.getTime());

			row.add(tms.toString());

			row.add(String.valueOf(num1));
			row.add(String.valueOf(num1 * 10));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));

			row.add("s_" + num1);
			row.add("s_" + num1 * 10);
			row.add("s_" + num1 * 100);

			row.add(tms.toString());

			row.add(String.valueOf(num1));
			row.add(String.valueOf(num1 * 10));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));
			row.add(String.valueOf(num1 * 100));

			dataTable.addRow(row);
			data[i] = row;
		}

		return data;
	}
}
