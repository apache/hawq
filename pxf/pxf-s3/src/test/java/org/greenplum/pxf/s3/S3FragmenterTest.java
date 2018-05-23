package org.greenplum.pxf.s3;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;

public class S3FragmenterTest {

	private static PxfS3 pxfS3;
	private static InputData inputData;

	@Test
	public void testGetFragments() throws Exception {
		S3Fragmenter fragmenter = new S3Fragmenter(inputData, pxfS3);
		List<Fragment> testFragments = fragmenter.getFragments();
		List<Fragment> expectFragments = new ArrayList<>();
		String pxfHost = InetAddress.getLocalHost().getHostAddress();
		String[] hosts = new String[] { pxfHost };
		for (String key : Arrays.asList("test-write/1524148597-0000000911_1", "test-write/1524496731-0000002514_5")) {
			expectFragments.add(new Fragment(inputData.getDataSource(), hosts, key.getBytes()));
		}
		// Verify same number of elements
		System.out.println("Comparing lengths of the Fragment lists ...");
		assertTrue(expectFragments.size() == testFragments.size());
		// Verify equality of each Fragment
		for (int i = 0; i < expectFragments.size(); i++) {
			System.out.println("Comparing Fragment[" + i + "] ...");
			assertTrue(fragmentEquals(expectFragments.get(i), testFragments.get(i)));
		}
	}

	/**
	 * In the absence of an equals() method in Fragment, verify equality only of the
	 * fields in use here
	 * 
	 * @param a
	 *            left hand side
	 * @param b
	 *            right hand side
	 * @return whether they are equal
	 */
	private boolean fragmentEquals(Fragment a, Fragment b) {
		// sourceName
		boolean rv = a.getSourceName().equals(b.getSourceName());
		// replicas
		String[] replicasA = a.getReplicas();
		String[] replicasB = b.getReplicas();
		if (replicasA.length == replicasB.length) {
			List<String> replicasAList = Arrays.asList(replicasA);
			for (int i = 0; i < replicasB.length; i++) {
				if (!replicasAList.contains(replicasB[i])) {
					rv = false;
				}
			}
		}
		// metadata
		if (!Arrays.equals(a.getMetadata(), b.getMetadata())) {
			rv = false;
		}
		return rv;
	}

	@BeforeClass
	public static void setUpMock() {
		inputData = InputDataMock.getInputData();
		inputData.setDataSource("/pxf-s3-devel/test-write");
		pxfS3 = PxfS3Mock.fromInputData(inputData);
		AmazonS3 s3 = pxfS3.getS3();
		s3.createBucket(pxfS3.getBucketName());
		s3.putObject(pxfS3.getBucketName(), "test-write/1524148597-0000000911_1",
				"contents of 1524148597-0000000911_1");
		s3.putObject(pxfS3.getBucketName(), "test-write/1524496731-0000002514_5",
				"contents of 1524496731-0000002514_5");
	}

	@AfterClass
	public static void destroyMock() {
		try {
			pxfS3.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
