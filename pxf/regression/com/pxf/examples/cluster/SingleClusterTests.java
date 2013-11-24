package com.pxf.examples.cluster;

import junit.framework.SystemTestCase4;

import org.junit.Test;

import com.pxf.infra.cluster.SingleCluster;

public class SingleClusterTests extends SystemTestCase4 {

	@Test
	public void start() throws Exception {

		SingleCluster sc = (SingleCluster) system.getSystemObject("cluster");

		// sc.stop(EnumScComponents.All);
		// sc.start(EnumScComponents.All);
	}

	@Test
	public void gphdRootFolder() throws Exception {

		SingleCluster sc = (SingleCluster) system.getSystemObject("cluster");

		System.out.println(sc.getClusterFolder());

	}
}
