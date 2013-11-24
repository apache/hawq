package com.pxf.infra.utils.exception;

import java.io.IOException;

import jsystem.framework.report.Reporter;

import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.regex.RegexUtils;

/**
 * Utilities for parsing Exceptions and validate it messages
 */
public abstract class ExceptionUtils {

	public static void validate(Reporter report, Exception thrown, Exception expected, boolean isRegex)
			throws IOException {

		validate(report, thrown, expected, isRegex, false);
	}

	public static void validate(Reporter report, Exception thrown, Exception expected, boolean isRegex, boolean ignoreExceptionType)
			throws IOException {

		int validationStatus = Reporter.PASS;
		String reportMessage = "";

		if (!ignoreExceptionType) {
			reportMessage = "\n\nExpected exception: " + expected.getClass()
					.getName() + "\n\nactual exception thrown: " + thrown.getClass()
					.getName() + "\n\n";
			if (!thrown.getClass()
					.getName()
					.equals(expected.getClass().getName())) {
				validationStatus = Reporter.FAIL;
				reportMessage = "\n\nExpected exception: " + expected.getClass()
						.getName() + "\n\nactual exception thrown: " + thrown.getClass()
						.getName() + "\n\n";
			}
		}
		reportMessage += "\n\nExpected message pattern: " + expected.getMessage() + "\n\nActual message thrown: " + thrown.getMessage();
		if (isRegex) {
			if (!RegexUtils.match(expected.getMessage(), thrown.getMessage())) {
				validationStatus = Reporter.FAIL;
				reportMessage += "\n\nExpected message pattern: " + expected.getMessage() + "\n\nActual message thrown: " + thrown.getMessage();
			}
		} else {
			if (!expected.getMessage().equals(thrown.getMessage())) {
				validationStatus = Reporter.FAIL;
				reportMessage += "\n\nExpected message pattern: " + expected.getMessage() + "\n\nActual message thrown: " + thrown.getMessage();

			}
		}

		ReportUtils.startLevel(report, ExceptionUtils.class, "Thrown Exception Validation Results");
		ReportUtils.report(report, ExceptionUtils.class, reportMessage, validationStatus);
		ReportUtils.stopLevel(report);
	}
}
