package com.pxf.tests.fixtures;

import com.pivotal.pxfauto.infra.cluster.Cluster;
import com.pivotal.pxfauto.infra.hive.Hive;
import com.pxf.tests.basic.PxfHiveRegression;

/**
 * Preparing the system for hive tests. Each Hive using test case should use this Fixture by using
 * the setFixture method.
 * 
 * @see PxfHiveRegression
 */
public class PxfHiveFixture extends BasicFixture {

	private Cluster sc;
	private Hive hive;

	/**
	 * Will be called when entering to the fixture.
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();

		startFixtureLevel();

		// get cluster object from sut
		sc = (Cluster) system.getSystemObject("cluster");

		// start Hive server for Hive JDBC requests
		sc.startHiveServer();

		// get hive object from sut
		hive = (Hive) system.getSystemObject("hive");

		stopFixtureLevel();
	}

	/**
	 * Clean up for the Fixture. This method will be called when pulling out from this Fixture to
	 * the Parent Fixture.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		startFixtureLevel();

		// close hive connection
		hive.close();

		// stop hive server
		sc.stopHiveServer();

		stopFixtureLevel();
	}
}