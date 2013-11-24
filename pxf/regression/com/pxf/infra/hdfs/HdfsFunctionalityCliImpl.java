package com.pxf.infra.hdfs;

import java.util.ArrayList;

import systemobject.terminal.Prompt;

import com.aqua.sysobj.conn.CliCommand;
import com.pxf.infra.common.ShellSystemObject;
import com.pxf.infra.utils.jsystem.sut.SutUtils;

/**
 * implementation of HdfsFunctionality using command-line interface,
 * 
 */
public abstract class HdfsFunctionalityCliImpl extends ShellSystemObject
		implements HdfsFunctionality {

	private String clusterFolder = System.getenv("GPHD_ROOT");

	private String hdfsPrefix = "bin/hdfs dfs";

	@Override
	public void init() throws Exception {

		setHost(SutUtils.getValue(sut, "//hdfs/host"));
		setUser(SutUtils.getValue(sut, "//hdfs/func/userName"));
		setPassword(SutUtils.getValue(sut, "//hdfs/func/password"));

		Prompt p = new Prompt();
		p.setCommandEnd(true);
		p.setPrompt("$");

		addPrompts(new Prompt[] { p });

		super.init();

		connect();

		CliCommand cmd = new CliCommand();
		cmd.setCommand("cd " + clusterFolder);

		command(cmd);
	}

	@Override
	public ArrayList<String> list(String path) throws Exception {
		runCommand(getHdfsPrefix() + " -ls " + path);

		ArrayList<String> fileList = new ArrayList<String>();
		// TODO: parse result and store as List

		return fileList;
	}

	@Override
	public int listSize(String hdfsDir) throws Exception {
		return list(hdfsDir).size();

	}

	@Override
	public void copyFromLocal(String srcPath, String destPath) throws Exception {
		runCommand(getHdfsPrefix() + " -copyFromLocal " + srcPath + " /" + destPath);

		if (getLastCmdResult().contains("File exists")) {
			throw new Exception("File " + destPath + " already exists");
		}

	}

	@Override
	public void createDirectory(String path) throws Exception {
		runCommand(getHdfsPrefix() + " -mkdir /" + path);

		if (getLastCmdResult().contains("File exists")) {
			throw new Exception("Directory " + path + " already exists");
		}

	}

	@Override
	public void removeDirectory(String path) throws Exception {
		runCommand(getHdfsPrefix() + " -rm -r /" + path);

	}

	@Override
	public String getFileContent(String path) throws Exception {
		runCommand(getHdfsPrefix() + " -cat /" + path);

		return getLastCmdResult();
	}

	public String getHdfsPrefix() {
		return hdfsPrefix;
	}

	public void setHdfsPrefix(String hdfsprefix) {
		this.hdfsPrefix = hdfsprefix;
	}

	public String getClusterFolder() {
		return clusterFolder;
	}

	public void setClusterFolder(String clusterFolder) {
		this.clusterFolder = clusterFolder;
	}
}
