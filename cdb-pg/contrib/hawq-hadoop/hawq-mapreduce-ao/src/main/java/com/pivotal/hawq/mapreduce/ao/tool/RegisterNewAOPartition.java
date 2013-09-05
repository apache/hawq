package com.pivotal.hawq.mapreduce.ao.tool;

import com.pivotal.hawq.mapreduce.ao.db.Database;
import com.pivotal.hawq.mapreduce.ao.db.Register;

import java.sql.SQLException;

public final class RegisterNewAOPartition extends Register {

	public static void main(String[] args) throws Exception {
		RegisterNewAOPartition register = new RegisterNewAOPartition();
		register.register(args);
	}

	@Override
	protected void printUsage() {
		System.out.println("  Usage: java <className>"
				+ " <sourceInHdfs> <statisticFileName>"
				+ " <dbHost> <dbPort> <dbName>"
				+ " <existTable> <newPartitionName>"
				+ " (<lowerBound>) [INCLUSIVE/EXCLUSIVE]"
				+ " (<upperBound>) [INCLUSIVE/EXCLUSIVE]\n   "
				+ " Example: java RegisterNewPartitionTest /mapreduce/hasqout"
				+ " statistic localhost 5432 template1 orders p1_129"
				+ " (DATE '1999-01-01') (DATE '1999-01-21')\n");
		normalExit = true;
	}

	@Override
	public void register(String[] args) throws Exception {
		// Start the "Control+c" handler
		ExitHandler exithandler = new ExitHandler();
		Runtime.getRuntime().addShutdownHook(exithandler);
		className = "RegisterNewPartition";

		// Analyze the arguments
		if ((args.length == 1 && args[0].equals("--help"))) {
			printUsage();
			return;
		} else if (args.length < 9) {
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
		String existTableName = args[5];
		String newPartitionName = args[6];
		String lowerBound = args[7], upperBound = "";
		int argv;
		for (argv = 8; argv < args.length; argv++) {
			if (args[argv].charAt(0) == '(')
				break;
			if (!lowerBound.equals(""))
				lowerBound += " ";
			lowerBound += args[argv];
		}
		for (; argv < args.length; argv++) {
			if (!upperBound.equals(""))
				upperBound += " ";
			upperBound += args[argv];
		}
		if (lowerBound.equals("") || upperBound.equals("")) {
			printlog("error", "Wrong format for lowerBound and upperBound");
			printUsage();
			return;
		}
		String newPartitionTableName = existTableName + "_1_prt_"
				+ newPartitionName;

		try {
			// Connect to database
			String url = host + ":" + port + "/" + dbName;
			printlog("info", "Try to connect to database " + url + ", user: "
					+ username);
			database = new Database();
			database.connectToDatabase(url, username, "");
			printlog("info", "Succeed to connect to database " + url + "/"
					+ dbName);

			// Check whether base table exists and get checksum of table.
			int existTableOid = database.getTableOid(existTableName);
			if (existTableOid == -1)
				throw new SQLException("Table " + existTableName
						+ " does not exit in database " + dbName);

			// Check whether new partition relation exists.
			if (database.getTableOid(newPartitionTableName) != -1)
				throw new SQLException("Partition " + newPartitionTableName
						+ " already exits in table " + existTableName);

			// Connect to HDFS
			connectToHdfs();

			// Analyze statistic file and amount of files
			checkFileAndStatistic();
			analyzeAndRemoveStatisticFile();

			// Create new partition
			database.transaction('b');
			exithandler.needRollback = true;
			database.addPartition(existTableName, newPartitionName, lowerBound,
					upperBound);
			database.tableLock(0, existTableOid);
			printlog("info", "Add partition " + newPartitionTableName
					+ " to table " + existTableName);
			// Register files in the source folder to new partition
			registerPartition(newPartitionTableName);
			database.tableLock(1, existTableOid);
			database.transaction('c');
			normalExit = true;
			printlog("info", "Succeed to register new partition "
					+ newPartitionTableName + " for table " + existTableName);
		} catch (Exception e) {
			if (!aborted)
				lastException = e;
			throw e;
		}

		// Vacuum analyze
		vacuumAnalyze(existTableName);
		printlog("info", "All Done!");
	}

}