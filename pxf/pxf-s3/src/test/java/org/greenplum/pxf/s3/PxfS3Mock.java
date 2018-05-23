package org.greenplum.pxf.s3;

import java.io.IOException;

import org.apache.hawq.pxf.api.utilities.InputData;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.findify.s3mock.S3Mock;

public class PxfS3Mock extends PxfS3 {

	private static final int PORT = 1999;
	private static S3Mock API = null;

	/*
	 * static { API = new
	 * S3Mock.Builder().withPort(PORT).withInMemoryBackend().build(); API.start(); }
	 */

	public PxfS3Mock(String s3AccessKey, String s3SecretKey, String s3Region) {
		super();
		this.setS3AccessKey(s3AccessKey);
		this.setS3SecretKey(s3SecretKey);
		if (null == API) {
			API = new S3Mock.Builder().withPort(PORT).withInMemoryBackend().build();
			API.start();
		}
		AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(
				"http://localhost:" + PORT, s3Region);
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
				.withEndpointConfiguration(endpoint)
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())).build();
		setS3(s3);
	}

	public static PxfS3 fromInputData(InputData inputData) {
		String s3AccessKey = inputData.getUserProperty(PxfS3.S3_ACCESS_KEY);
		String s3SecretKey = inputData.getUserProperty(PxfS3.S3_SECRET_KEY);
		String s3Region = inputData.getUserProperty(PxfS3.S3_REGION);
		PxfS3 rv = new PxfS3Mock(s3AccessKey, s3SecretKey, s3Region);
		rv.setBucketName(inputData);
		return rv;
	}

	public void close() throws IOException {
		super.close();
		if (API != null) {
			API.shutdown();
		}
	}

	/**
	 * Example of how to use this class
	 * 
	 * NOTE: the try/catch/finally is important since, if an exception is thrown,
	 * this will keep the port open, preventing future runs (within the same JVM;
	 * e.g. the IDE).
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("Starting ...");
		PxfS3 pxfS3 = null;
		try {
			InputData inputData = InputDataMock.getInputData();
			inputData.setDataSource("/pxf-s3-devel/test-write");
			pxfS3 = PxfS3Mock.fromInputData(inputData);
			AmazonS3 s3 = pxfS3.getS3();
			s3.createBucket(pxfS3.getBucketName());
			s3.putObject(pxfS3.getBucketName(), "test-write/1524148597-0000000911_1",
					"contents of 1524148597-0000000911_1");
			s3.putObject(pxfS3.getBucketName(), "test-write/1524496731-0000002514_5",
					"contents of 1524496731-0000002514_5");
			for (String key : pxfS3.getKeysInBucket(inputData)) {
				System.out.println("key: " + key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != pxfS3) {
				pxfS3.close();
			}
		}
	}

}
