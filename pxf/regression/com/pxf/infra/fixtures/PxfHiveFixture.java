package com.pxf.infra.fixtures;

import com.pxf.infra.cluster.Cluster;
import com.pxf.tests.basic.PxfHiveRegression;

/**
 * Preparing the system for hive tests. Each Hive using test case should use
 * this Fixture by using the setFixture method.
 * 
 * @see PxfHiveRegression
 */
public class PxfHiveFixture extends BasicFixture {

	Cluster sc;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		startFixtureLevel();

		sc = (Cluster) system.getSystemObject("cluster");
		sc.startHiveServer();

		stopFixtureLevel();
	}
}
