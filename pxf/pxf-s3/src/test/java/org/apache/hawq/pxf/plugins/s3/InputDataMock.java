package org.apache.hawq.pxf.plugins.s3;

import java.util.HashMap;

import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.s3.PxfS3;

/**
 * This exists so the various userProperty values can be set
 * InputData instance
 */
public class InputDataMock extends InputData {

	public InputDataMock() {
		super();
		super.requestParametersMap = new HashMap<String, String>();
	}

	public void setUserProperty(String key, String value) {
		super.requestParametersMap.put(USER_PROP_PREFIX + key.toUpperCase(), value);
	}

	/**
	 * Helper to build InputData
	 * @return an InputData instance
	 */
	public static InputData getInputData() {
		InputDataMock inputData = new InputDataMock();
		inputData.setUserProperty(PxfS3.S3_ACCESS_KEY, PxfS3Test.TEST_S3_ACCESS_KEY);
		inputData.setUserProperty(PxfS3.S3_SECRET_KEY, PxfS3Test.TEST_S3_SECRET_KEY);
		inputData.setUserProperty(PxfS3.S3_REGION, PxfS3Test.TEST_S3_REGION);
		return inputData;
	}
}
