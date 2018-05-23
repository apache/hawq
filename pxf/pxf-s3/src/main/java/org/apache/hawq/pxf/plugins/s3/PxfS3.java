package org.apache.hawq.pxf.plugins.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.utilities.InputData;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Test S3 access, ls, wildcarding filenames, open/read
 */
public class PxfS3 {

	private AmazonS3 s3;
	private String bucketName;
	private String objectName;
	private BufferedReader s3In;
	private String s3AccessKey;
	private String s3SecretKey;

	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	public static final String S3_REGION = "S3_REGION";

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(PxfS3.class);

	public String toString() {
		return "PxfS3[bucketName=" + bucketName + ", objectName=" + objectName + "]";
	}
	
	/**
	 * This exists to support tests
	 */
	public PxfS3() {
		super();
	}

	// FIXME: how to bubble these errors up to the SQL prompt?
	public PxfS3(String s3AccessKey, String s3SecretKey, String s3Region) {
		if (null == s3AccessKey || null == s3SecretKey) {
			throw new RuntimeException("Both " + S3_ACCESS_KEY + " and " + S3_SECRET_KEY + " must be set");
		}
		if (null == s3Region) {
			throw new RuntimeException(S3_REGION + " must be set");
		}
		this.s3AccessKey = s3AccessKey;
		this.s3SecretKey = s3SecretKey;
		BasicAWSCredentials creds = new BasicAWSCredentials(s3AccessKey, s3SecretKey);
		s3 = AmazonS3ClientBuilder.standard().withRegion(s3Region)
				.withCredentials(new AWSStaticCredentialsProvider(creds)).build();
	}
	
	// Used within the *write* context
	public String getS3aURI(InputData inputData) {
		// /pxf-s3-devel/test-write/1524148597-0000000430_1
		// Need to get objectName based on this string, which is in inputData.getDataSource()
		String s3Path = getS3Path(inputData);
		this.objectName = s3Path.replaceAll("^[^/]+/", "");
		return getS3aURI();
	}

	// Format: s3a://S3_ACCESS_KEY:S3_SECRET_KEY@BUCKET_NAME/FILE_NAME
	public String getS3aURI() {
		String rv = "s3a://" + this.s3AccessKey + ":";
		try {
			rv += URLEncoder.encode(this.s3SecretKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		rv += "@" + this.bucketName + "/" + this.objectName;
		return rv;
	}

	public void setObjectName(String name) {
		this.objectName = name;
	}

	public void open() {
		S3Object s3Obj = s3.getObject(bucketName, objectName);
		S3ObjectInputStream s3is = s3Obj.getObjectContent();
		s3In = new BufferedReader(new InputStreamReader(s3is));
	}

	public void close() throws IOException {
		if (null != s3In) {
			s3In.close();
			s3In = null;
		}
	}

	public String readLine() throws IOException {
		return s3In.readLine();
	}

	public String getBucketName() {
		return bucketName;
	}

	private static String getS3Path(InputData inputData) {
		return inputData.getDataSource().replaceAll("^/+", "");
	}

	// Set bucketName and prefix, based on the PXF URI
	protected void setBucketName(InputData inputData) {
		// Remove any leading '/' as the value passed in here is in consistent in this
		// regard
		String s3Path = getS3Path(inputData);
		int slashPos = s3Path.indexOf("/");
		if (slashPos > -1) {
			bucketName = s3Path.substring(0, slashPos);
		} else {
			bucketName = s3Path;
		}
	}

	/*
	 * Useful from within Fragmenter and Accessor classes
	 */
	public static PxfS3 fromInputData(InputData inputData) {
		String s3AccessKey = inputData.getUserProperty(S3_ACCESS_KEY);
		String s3SecretKey = inputData.getUserProperty(S3_SECRET_KEY);
		String s3Region = inputData.getUserProperty(S3_REGION);
		PxfS3 rv = new PxfS3(s3AccessKey, s3SecretKey, s3Region);
		rv.setBucketName(inputData);
		return rv;
	}

	/*
	 * If prefix is null, it will have no effect. Note that this doesn't handle
	 * something like "*.csv", since that's a wildcard and not a prefix.
	 */
	public List<String> getKeysInBucket(InputData inputData) {
		List<String> rv = new ArrayList<>();
		String s3Path = getS3Path(inputData);
		String prefix = null;
		int slashPos = s3Path.indexOf("/");
		if (slashPos > -1) {
			bucketName = s3Path.substring(0, slashPos);
			if (s3Path.length() > slashPos + 1) {
				prefix = s3Path.substring(slashPos + 1);
			}
		} else {
			bucketName = s3Path;
		}
		ListObjectsV2Result result = s3.listObjectsV2(bucketName, prefix);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary os : objects) {
			String objKey = os.getKey();
			if (!objKey.endsWith("/")) {
				rv.add(objKey);
			}
		}
		return rv;
	}

	public AmazonS3 getS3() {
		return s3;
	}

	public void setS3(AmazonS3 s3) {
		this.s3 = s3;
	}

	public void setS3AccessKey(String s3AccessKey) {
		this.s3AccessKey = s3AccessKey;
	}

	public void setS3SecretKey(String s3SecretKey) {
		this.s3SecretKey = s3SecretKey;
	}

}
