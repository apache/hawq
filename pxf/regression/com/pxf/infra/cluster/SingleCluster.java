package com.pxf.infra.cluster;

import java.io.IOException;
import java.util.HashMap;

import com.pxf.infra.common.ShellSystemObject;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.jsystem.sut.SutUtils;

/**
 * SingleCluster system object.
 * 
 */
public class SingleCluster extends ShellSystemObject implements Cluster {

	private String clusterFolder = System.getenv("GPHD_ROOT");
	private String hiveServerFolder = "hive/bin";

	private int nodesAmount = 3;

	@Override
	public void init() throws Exception {

		setHost(SutUtils.getValue(sut, "//cluster/host"));
		setPassword(SutUtils.getValue(sut, "//cluster/password"));
		setUserName(SutUtils.getValue(sut, "//cluster/userName"));

		super.init();

		if (clusterFolder == null || clusterFolder.equals("null") || clusterFolder.equals("")) {
			throw new Exception(getClass().getSimpleName() + ": Illegal Cluster Folder: please define GPHD_ROOT" + clusterFolder);
		}

		runCommand("cd " + clusterFolder);
	}

	@Override
	public void startHiveServer() throws Exception {

		ReportUtils.startLevel(report, getClass(), "Start Hive Server");

		runCommand("cd " + hiveServerFolder);
		runCommand("hive --service hiveserver &");

		Thread.sleep(20 * 1000);

		ReportUtils.stopLevel(report);
	}

	@Override
	public void start(EnumClusterComponents component) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Start " + component);

		stop(component);

		runCommand(clusterFolder + "/bin/start-" + component.getCmd());

		Thread.sleep(5000);

		ReportUtils.stopLevel(report);
	}

	@Override
	public void stop(EnumClusterComponents component) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Stop " + component);

		runCommand(clusterFolder + "/bin/stop-" + component.getCmd());

		Thread.sleep(5000);

		ReportUtils.stopLevel(report);
	}

	@Override
	public boolean isUp(EnumClusterComponents component) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Check " + component.toString() + " is Up");

		boolean result = false;

		switch (component) {
		case HDFS:

			result = isComponentUp(new EnumScProcesses[] {
					EnumScProcesses.DataNode,
					EnumScProcesses.NameNode });
			break;

		case HIVE:

			result = isComponentUp(new EnumScProcesses[] {
					EnumScProcesses.DataNode,
					EnumScProcesses.NameNode,
					EnumScProcesses.RunJar });
			break;

		default:
			result = isComponentUp(EnumScProcesses.values());
			break;
		}

		ReportUtils.report(report, getClass(), "Required Processes are " + ((result) ? " Up" : " Not all up"));

		ReportUtils.stopLevel(report);

		return result;

	}

	private boolean isComponentUp(EnumScProcesses[] proccessesToCheck)
			throws IOException {

		HashMap<String, Integer> pMap = getProcessMap();

		for (int i = 0; i < proccessesToCheck.length; i++) {

			ReportUtils.report(report, getClass(), "Check: " + proccessesToCheck[i].toString());

			Integer value = pMap.get(proccessesToCheck[i].toString());

			if (value == null) {
				report.stopLevel();
				return false;
			}

			int amount = value.intValue();

			if (amount != proccessesToCheck[i].getInstances()) {
				report.stopLevel();
				return false;
			}
		}

		return true;
	}

	/**
	 * @return Map of running cluster process <process-name,amount-of-instances>
	 * @throws IOException
	 */
	private HashMap<String, Integer> getProcessMap() throws IOException {

		runCommand("jps");

		String cmdResult = getLastCmdResult();

		String[] splitResults = cmdResult.split(System.getProperty("line.separator"));

		HashMap<String, Integer> map = new HashMap<String, Integer>();

		for (int i = 0; i < splitResults.length; i++) {

			String currentSplitResult = null;
			try {
				currentSplitResult = splitResults[i].split(" ")[1].trim();
			} catch (Exception e) {
				continue;
			}

			int value = 1;
			if (map.get(currentSplitResult) != null) {

				value = map.get(currentSplitResult).intValue();
				value++;

			}

			map.put(currentSplitResult, Integer.valueOf(value));
		}

		return map;

	}

	public String getClusterFolder() {
		return clusterFolder;
	}

	public void setClusterFolder(String clusterFolder) {
		this.clusterFolder = clusterFolder;
	}

	@Override
	public void killHiveServer() throws Exception {
		// TODO: figure out how to kill the process
	}

	public enum EnumScProcesses {

		DataNode(3), HMaster(1), HRegionServer(3), NameNode(1), NodeManager(3), ResourceManager(
				1), RunJar(1);

		private int instances;

		private EnumScProcesses(int instances) {
			this.instances = instances;
		}

		public int getInstances() {
			return instances;
		}

	}
}
