package com.pxf.tests.dataprepares.avro;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import com.pivotal.parot.fileformats.IDataPreparer;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pxf.tests.dataprepares.avro.schema.CustomAvroRecInFile;

public class CustomAvroPreparer implements IDataPreparer {

	private String schemaName;

	public CustomAvroPreparer(String schemaName) {
		this.schemaName = schemaName;
	}

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		CustomAvroRecInFile[] data = new CustomAvroRecInFile[rows];

		for (int i = 0; i < data.length; i++) {

			int num1 = i + 1;

			Timestamp tms = new Timestamp(Calendar.getInstance()
					.getTime()
					.getTime());

			data[i] = new CustomAvroRecInFile(schemaName, tms, num1, 10 * num1, 20 * num1);

			ArrayList<String> row = new ArrayList<String>();

			row.add(data[i].tms);

			for (int j = 0; j < data[i].num.length; j++) {
				row.add(String.valueOf(data[i].num[j]));
			}

			row.add(String.valueOf(data[i].int1));
			row.add(String.valueOf(data[i].int2));

			for (int j = 0; j < data[i].strings.length; j++) {
				row.add(data[i].strings[j]);
			}

			row.add(data[i].st1);

			for (int j = 0; j < data[i].dubs.length; j++) {
				row.add(String.valueOf(data[i].dubs[j]));
			}

			row.add(String.valueOf(data[i].db));

			for (int j = 0; j < data[i].fts.length; j++) {
				row.add(String.valueOf(data[i].fts[j]));
			}

			row.add(String.valueOf(data[i].ft));

			for (int j = 0; j < data[i].lngs.length; j++) {
				row.add(String.valueOf(data[i].lngs[j]));
			}

			row.add(String.valueOf(data[i].lng));

			row.add(new String(data[i].bts));

			row.add(String.valueOf(data[i].bl));

			dataTable.addRow(row);
		}

		return data;
	}
}
