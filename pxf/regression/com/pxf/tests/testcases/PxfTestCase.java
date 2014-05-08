package com.pxf.tests.testcases;

import java.io.File;

import jsystem.utils.FileUtils;
import junit.framework.SystemTestCase4;

import org.junit.After;
import org.junit.Before;

import com.pivotal.parot.components.hawq.Hawq;
import com.pivotal.parot.components.hdfs.Hdfs;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;

/**
 * All test cases that related to PXF will extends from this Test Case class. Already includes HAWQ
 * and HDFS system objects loaded and ready to use.
 */
public class PxfTestCase extends SystemTestCase4 {
	protected Hawq hawq;

	protected Hdfs hdfs;

	protected String hdfsWorkingFolder;

	protected String loaclTempFolder = "regressionTempFolder";

	@Before
	public void defaultBefore() throws Throwable {

		ReportUtils.startLevel(report, getClass(), "setup");
		super.defaultBefore();

		hawq = (Hawq) system.getSystemObject("hawq");
		hdfs = (Hdfs) system.getSystemObject("hdfs");

		hdfsWorkingFolder = hdfs.getWorkingDirectory();

		hdfs.removeDirectory(hdfsWorkingFolder);
		new File(loaclTempFolder).mkdirs();

		ReportUtils.stopLevel(report);
	}

	@After
	public void defaultAfter() throws Throwable {

		ReportUtils.startLevel(report, getClass(), "teardown");

		super.defaultAfter();

		FileUtils.deleteDirectory(new File(loaclTempFolder));

		ReportUtils.stopLevel(report);
	}
}