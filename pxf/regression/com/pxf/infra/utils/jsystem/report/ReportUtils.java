package com.pxf.infra.utils.jsystem.report;

import java.io.IOException;

import jsystem.framework.report.Reporter;
import jsystem.framework.report.Reporter.ReportAttribute;

/**
 * JSystem Reporter wrapper to custom reports
 */
public abstract class ReportUtils {

	public static void report(Reporter jsystemReport, Class<?> contextClass, String message) {

		report(jsystemReport, contextClass, message, Reporter.PASS);
	}

	public static void report(Reporter jsystemReport, Class<?> contextClass, String message, int status) {

		jsystemReport.report(contextClass.getSimpleName() + " -> " + message, status);
	}

	public static void startLevel(Reporter jsystemReport, Class<?> contextClass, String message)
			throws IOException {

		jsystemReport.startLevel(contextClass.getSimpleName() + " -> " + message);
	}

	public static void stopLevel(Reporter jsystemReport) throws IOException {

		jsystemReport.stopLevel();
	}

	public static void reportHtml(Reporter jsystemReport, Class<?> contextClass, String data) {

		jsystemReport.report(contextClass.getSimpleName() + " -> " + data, ReportAttribute.HTML);
	}

	public static void reportHtmlLink(Reporter jsystemReport, String title, Class<?> contextClass, String data) {

		jsystemReport.report(contextClass.getSimpleName() + " -> " + title, contextClass.getSimpleName() + " -> " + data, Reporter.PASS, ReportAttribute.HTML);
	}

	public static void reportHtmlLink(Reporter jsystemReport, Class<?> contextClass, String title, String data, int status) {

		jsystemReport.report(contextClass.getSimpleName() + " -> " + title, data, status, ReportAttribute.HTML);
	}

	public static void throwUnsupportedFunctionality(Class<?> contextClass, String message)
			throws Exception {
		throw new Exception(contextClass.getSimpleName() + " -> " + message + " is currently not supported");
	}
}
