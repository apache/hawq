package com.pxf.infra.fixtures;

import java.io.IOException;

import com.pxf.infra.utils.jsystem.report.ReportUtils;

import jsystem.framework.fixture.Fixture;

/**
 * Fixture is a system state. Each Test will verify that the system is in the
 * fixture it required, if not - it will perform the fixture to get in that
 * state. This is the Base Fixture that all fixture will extends from.
 * 
 */
public class BasicFixture extends Fixture {

	protected void startFixtureLevel() throws IOException {
		ReportUtils.startLevel(report, getClass(), "Start Fixture");
	}

	protected void stopFixtureLevel() throws IOException {
		ReportUtils.stopLevel(report);
	}
}
