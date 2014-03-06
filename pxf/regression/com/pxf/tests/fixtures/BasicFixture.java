package com.pxf.tests.fixtures;

import java.io.IOException;

import jsystem.framework.fixture.Fixture;

import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;

/**
 * Fixture is a system state. Each Test will verify that the system is in the fixture it required,
 * if not - it will perform the fixture to get in that state. This is the Base Fixture that all
 * fixture will extends from.
 * 
 */
public class BasicFixture extends Fixture {

	protected void startFixtureLevel() throws IOException {
		ReportUtils.startLevel(report, getClass(), "Fixture");
	}

	protected void stopFixtureLevel() throws IOException {
		ReportUtils.stopLevel(report);
	}
}
