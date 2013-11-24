package com.pxf.infra.utils.csv;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Utilities for working with CSV files
 */
public abstract class CsvUtils {

	private static CSVReader csvReader;

	/**
	 * Get List of data from CSV file
	 * 
	 * @param pathToCsvFile
	 * @return
	 * @throws IOException
	 */
	public static List<List<String>> getList(String pathToCsvFile)
			throws IOException {

		csvReader = new CSVReader(new FileReader(pathToCsvFile));

		List<String[]> list = csvReader.readAll();

		List<List<String>> result = new ArrayList<List<String>>();

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String[] strings = (String[]) iterator.next();

			List<String> rowList = new ArrayList<String>();

			for (int i = 0; i < strings.length; i++) {
				rowList.add(strings[i]);
			}

			result.add(rowList);
		}

		return result;
	}
}
