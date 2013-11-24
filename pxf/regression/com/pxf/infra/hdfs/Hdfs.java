package com.pxf.infra.hdfs;

import jsystem.framework.system.SystemObjectImpl;

/**
 * Represents HDFS, holds HdfsFunctionality interface as a member, the
 * implementation needs to be mentioned in the SUT file.
 * 
 */
public class Hdfs extends SystemObjectImpl {

	private String host;

	private String workingDirectory;

	public HdfsFunctionality func;

	@Override
	public void init() throws Exception {

		super.init();

		getFunc().removeDirectory(getWorkingDirectory());
		getFunc().createDirectory(getWorkingDirectory());

		getFunc().list(getWorkingDirectory());
	}

	public HdfsFunctionality getFunc() {
		return func;
	}

	public void setFunc(HdfsFunctionality func) {
		this.func = func;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getWorkingDirectory() {
		return workingDirectory;
	}

	public void setWorkingDirectory(String workingDirectory) {
		this.workingDirectory = workingDirectory;
	}

}
