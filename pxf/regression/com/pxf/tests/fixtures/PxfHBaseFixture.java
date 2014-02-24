package com.pxf.tests.fixtures;

import java.io.File;

import jsystem.utils.FileUtils;

import com.pivotal.pxfauto.infra.common.ShellSystemObject;
import com.pivotal.pxfauto.infra.hbase.HBase;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;

public class PxfHBaseFixture extends BasicFixture {

	HBase hbase;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		startFixtureLevel();

		hbase = (HBase) system.getSystemObject("hbase");

		Table[] hbaseTables = new Table[] {
				new Table("hbase_table", null),
				new Table("hbase_table_with_nulls", null),
				new Table("hbase_table_integer_row_key", null),
				new Table("pxflookup", null) };

		for (int i = 0; i < hbaseTables.length; i++) {

			if (hbase.checkTableExists(hbaseTables[i])) {
				hbase.dropTable(hbaseTables[i], false);
			}
		}

		prepareJar(
				new String[] {
						"HBaseAccessorWithFilter", 
						"FilterPrinterAccessor"},
				new String[] {
						"HBaseAccessorWithFilter", 
						"HBaseAccessorWithFilter$SplitBoundary", 
						"FilterPrinterAccessor",
						"FilterPrinterAccessor$FilterPrinterException"},
				"Class-Path: pxf-hbase-2.2.0.jar",
				"hbase-filter-test.jar");
		
		stopFixtureLevel();
	}
	
	private void prepareJar(String[] javaFileNames, String[] classFileNames,
							String manifestText, String jarName) throws Exception {
		
		final String regressionDir = "regression/resources/";
		final String tmpDir = "/tmp/";
		
		File manifestFile = new File(tmpDir + "MANIFEST.MF");
		File jarFile = new File(tmpDir + jarName);
		File[] javaFiles = new File[javaFileNames.length];
		File[] classFiles = new File[classFileNames.length];
		
		ReportUtils.report(report, getClass(), "copy java files to /tmp/");
		int index = 0;
		for (String javaFileName: javaFileNames) {
			javaFiles[index] = new File(tmpDir + javaFileName + ".java");
			FileUtils.copyFile(regressionDir + javaFileName + ".java", 
							   javaFiles[index].getAbsolutePath());
			++index;
		}
		
		ShellSystemObject sso = new ShellSystemObject();
		sso.init();
		
		ReportUtils.report(report, getClass(), "create MANIFEST file");
		sso.runCommand("echo -e \"" + manifestText + "\n\" > " + manifestFile.getAbsolutePath());

		ReportUtils.report(report, getClass(), "compile java files");
		StringBuilder sb = new StringBuilder();
		sb.append("javac -cp `${GPHD_ROOT}/bin/hadoop classpath`:" +
				"`${GPHD_ROOT}/bin/hbase classpath`:`echo ${GPHD_ROOT}/hbase/lib/log4j-*.jar` ");
		for (File javaFile: javaFiles) {
			sb.append(javaFile.getAbsolutePath()).append(" ");
		}
		sb.append("-d ").append(tmpDir);
		sso.runCommand(sb.toString());
		
		ReportUtils.report(report, getClass(), "create " + jarFile.getName());
		// the $ in inner class files must be prepended by a slash in command line
		sb = new StringBuilder();
		sb.append("jar cmf ").append(manifestFile.getAbsolutePath()).
		   append(" ").append(jarFile.getAbsolutePath()).append(" ");
		index = 0;
		for (String className: classFileNames) {
			classFiles[index] = new File(tmpDir + className + ".class");
			sb.append("-C ").append(tmpDir).append(" ").
			   append(getEscapedClassName(classFiles[index])).append(" ");
			++index;
		}
		sso.runCommand(sb.toString());
		
		ReportUtils.report(report, getClass(), "move " + jarFile.getName() + " to pxf directory");
		sso.runCommand("mv " + jarFile.getAbsolutePath() + " $GPHD_ROOT/pxf/.");
		
		sso.close();
		
		ReportUtils.report(report, getClass(), "cleanup temp files from /tmp/");
		manifestFile.delete();
		for (File classFile: classFiles) {
				classFile.delete();
		}
		for (File javaFile: javaFiles) {
				javaFile.delete();
		}
		jarFile.delete();
	}
	
	private String getEscapedClassName(File file) {
		return file.getName().replace("$", "\\$");
	}
}
