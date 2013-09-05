package com.pivotal.hawq.mapreduce.ao.tool;

import com.pivotal.hawq.mapreduce.ao.db.Database;
import com.pivotal.hawq.mapreduce.ao.db.Register;

import java.sql.SQLException;

public final class RegisterNewAOTable extends Register {

	public static void main(String[] args) throws Exception {
		RegisterNewAOTable register = new RegisterNewAOTable();
		register.register(args);
	}

	@Override
	protected void printUsage() {
		System.out.println("  Usage: java <className> <sourceInHdfs>"
				+ " <statisticFileName> <dbHost> <dbPort>"
				+ " <dbName> <newTable>\n    Example: java RegisterTableTest"
				+ " /mapreduce/hasqout statistic localhost 5432"
				+ " template1 student(id int, score int)\n");
		normalExit = true;
	}

	@Override
	public void register(String[] args) throws Exception {
		// Start the "Control+c" handler
		ExitHandler exithandler = new ExitHandler();
		Runtime.getRuntime().addShutdownHook(exithandler);
		className = "RegisterNewTable";

		// Analyze the arguments
		if ((args.length == 1 && args[0].equals("--help"))) {
			printUsage();
			return;
		} else if (args.length < 7) {
			String input = "Wrong input: ";
			for (int i = 0; i < args.length; i++) {
				if (!input.equals(""))
					input += " ";
				input += args[i];
			}
			System.out.println(input);
			printUsage();
			return;
		}
		source = args[0];
		statisticFile = source + "/" + args[1];
		String host = args[2], port = args[3], dbName = args[4];
		String newTableName = args[5].substring(0, args[5].indexOf('('));
		String tableFormat = "";
		for (int i = 5; i < args.length; i++)
			tableFormat += args[i] + " ";

		try {
			// Connect to database
			String url = host + ":" + port + "/" + dbName;
			printlog("info", "Try to connect to database " + url + ", user: "
					+ username);
			database = new Database();
			database.connectToDatabase(url, username, "");
			printlog("info", "Succeed to connect to database " + url + "/"
					+ dbName);

			// Check whether new table exits.
			if (database.getTableOid(newTableName) != -1)
				throw new SQLException("Table " + newTableName
						+ " already exits in database " + dbName);

			// Connect to HDFS
			connectToHdfs();

			// Analyze statistic file and amount of files
			checkFileAndStatistic();
			analyzeAndRemoveStatisticFile();

			// Create new table
			database.transaction('b');
			exithandler.needRollback = true;
			database.createTable(tableFormat);
			printlog("info", "Create table " + newTableName);
			// Register files in the source folder to new table
			registerTable(newTableName);
			database.transaction('c');
			normalExit = true;
			printlog("info", "Succeed to register new table " + newTableName);
		} catch (Exception e) {
			if (!aborted)
				lastException = e;
			throw e;
		}

		// Vacuum analyze
		vacuumAnalyze(newTableName);
		printlog("info", "All Done!");
	}
}