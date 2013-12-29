package com.pxf.tests.dataprepares.sequence;

import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import com.pivotal.pxfauto.infra.fileformats.IDataPreparer;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;

public class CustomSequencePreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		Object[] cwArr = new Object[rows];

		for (int i = 0; i < cwArr.length; i++) {

			ArrayList<String> row = new ArrayList<String>();

			int num1 = i;

			Timestamp tms = new Timestamp(Calendar.getInstance()
					.getTime()
					.getTime());

			Class<?> c = Class.forName("CustomWritable");// full package name
			Constructor<?> constructor = c.getConstructor(Timestamp.class, int.class, int.class, int.class);

			constructor.setAccessible(true);

			row.add(tms.toString());

			cwArr[i] = constructor.newInstance(tms, num1, 10 * num1, 20 * num1);

			if (i > 0) {
				/**
				 * Read fields and store in sudo table
				 */

				int[] num = ((int[]) cwArr[i].getClass()
						.getField("num")
						.get(cwArr[i]));

				for (int j = 0; j < num.length; j++) {
					row.add(String.valueOf(num[j]));
				}

				int int1 = ((Integer) cwArr[i].getClass()
						.getField("int1")
						.get(cwArr[i]));

				row.add(String.valueOf(int1));

				int int2 = ((Integer) cwArr[i].getClass()
						.getField("int2")
						.get(cwArr[i]));

				row.add(String.valueOf(int2));

				String[] strings = ((String[]) cwArr[i].getClass()
						.getField("strings")
						.get(cwArr[i]));

				for (int j = 0; j < strings.length; j++) {
					row.add(strings[j]);
				}

				String st1 = ((String) cwArr[i].getClass()
						.getField("st1")
						.get(cwArr[i]));

				row.add(st1);

				double[] dubs = ((double[]) cwArr[i].getClass()
						.getField("dubs")
						.get(cwArr[i]));

				for (int j = 0; j < dubs.length; j++) {
					row.add(String.valueOf(dubs[j]));
				}

				double db = ((Double) cwArr[i].getClass()
						.getField("db")
						.get(cwArr[i]));

				row.add(String.valueOf(db));

				float[] fts = ((float[]) cwArr[i].getClass()
						.getField("fts")
						.get(cwArr[i]));

				for (int j = 0; j < fts.length; j++) {
					row.add(String.valueOf(fts[j]));
				}

				float ft = ((Float) cwArr[i].getClass()
						.getField("ft")
						.get(cwArr[i]));

				row.add(String.valueOf(ft));

				long[] lngs = ((long[]) cwArr[i].getClass()
						.getField("lngs")
						.get(cwArr[i]));

				for (int j = 0; j < lngs.length; j++) {
					row.add(String.valueOf(lngs[j]));
				}

				long lng = ((Long) cwArr[i].getClass()
						.getField("lng")
						.get(cwArr[i]));

				row.add(String.valueOf(lng));

				byte[] bts = ((byte[]) cwArr[i].getClass()
						.getField("bts")
						.get(cwArr[i]));

				row.add(new String(bts));

				boolean[] bools = ((boolean[]) cwArr[i].getClass()
						.getField("bools")
						.get(cwArr[i]));
				for (int j = 0; j < bools.length; j++) {
					row.add(String.valueOf(bools[j]));
				}
				
				boolean bool = ((Boolean) cwArr[i].getClass()
								.getField("bool")
								.get(cwArr[i]));
				row.add(String.valueOf(bool));

				short[] shrts = ((short[]) cwArr[i].getClass()
						.getField("shrts")
						.get(cwArr[i]));
				
				for (int j = 0; j < shrts.length; j++) {
					row.add(String.valueOf(shrts[j]));
				}
				
				long shrt = ((Short) cwArr[i].getClass()
						.getField("shrt")
						.get(cwArr[i]));

				row.add(String.valueOf(shrt));
				
				dataTable.addRow(row);
			}
		}

		return cwArr;
	}

}
