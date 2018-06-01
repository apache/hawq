package org.apache.hawq.pxf.plugins.s3;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.s3.PxfS3;
import org.junit.Test;

public class PxfS3Test {
	
	static final String TEST_S3_ACCESS_KEY = "DAZFVISAXWVBOBDBHQZX";
	static final String TEST_S3_SECRET_KEY = "IhipDEysJnknxCBeX/9LGRg35l55owj/wf7KZYh9";
	static final String TEST_S3_REGION = "us-east-1";

	/**
	 * Supports <b>READ</b> operations
	 * 
	 * @throws UnsupportedEncodingException
	 */
	@Test
	public void testGetS3aURIForRead() throws UnsupportedEncodingException {
		InputData inputData = InputDataMock.getInputData();
		inputData.setDataSource("pxf-s3-devel/sample-from-spark/part-0");
		PxfS3 pxfS3 = PxfS3.fromInputData(inputData);
		pxfS3.setObjectName("sample-from-spark/part-00000-f5d896f5-e61e-4991-935e-95707ff4d6eb-c000.snappy.parquet");
		String s3aURI = pxfS3.getS3aURI();
		String expectedS3aURI = "s3a://" + PxfS3Test.TEST_S3_ACCESS_KEY + ":" + URLEncoder.encode(PxfS3Test.TEST_S3_SECRET_KEY, "UTF-8")
			+ "@" + "pxf-s3-devel" + "/" + "sample-from-spark/part-00000-f5d896f5-e61e-4991-935e-95707ff4d6eb-c000.snappy.parquet";
		assertEquals(expectedS3aURI, s3aURI);
	}
	
	/**
	 * Supports <b>WRITE</b> operations
	 * 
	 * @throws UnsupportedEncodingException
	 */
	@Test
	public void testGetS3aURIForWrite() throws UnsupportedEncodingException {
		InputData inputData = InputDataMock.getInputData();
		inputData.setDataSource("/pxf-s3-devel/test-write/1524148597-0000000430_1");
		PxfS3 pxfS3 = PxfS3.fromInputData(inputData);
		String s3aURI = pxfS3.getS3aURI(inputData);
		String expectedS3aURI = "s3a://" + PxfS3Test.TEST_S3_ACCESS_KEY + ":" + URLEncoder.encode(PxfS3Test.TEST_S3_SECRET_KEY, "UTF-8")
			+ "@" + "pxf-s3-devel" + "/" + "test-write/1524148597-0000000430_1";
		assertEquals(expectedS3aURI, s3aURI);
	}

}
