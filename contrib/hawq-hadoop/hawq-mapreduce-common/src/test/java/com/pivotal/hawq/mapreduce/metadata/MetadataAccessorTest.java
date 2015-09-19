package com.pivotal.hawq.mapreduce.metadata;

import junit.framework.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;

public class MetadataAccessorTest {

	@Test
	public void testInvalidMetadataFile() throws Exception {
		try {
			MetadataAccessor.newInstanceUsingFile("no_such_file");
			Assert.fail();
		} catch (MetadataAccessException e) {
			Assert.assertSame(e.getCause().getClass(), FileNotFoundException.class);
		}

		try {
			MetadataAccessor.newInstanceUsingFile(
					getFilePathUnderClasspath("/invalid_metadata.yaml"));
			Assert.fail();
		} catch (MetadataAccessException e) {}
	}

	@Test
	public void testIncorrectAPICall() throws Exception {
		MetadataAccessor accessor;

		accessor = MetadataAccessor.newInstanceUsingFile(
				getFilePathUnderClasspath("/sample_ao_metadata.yaml"));
		Assert.assertEquals(HAWQTableFormat.AO, accessor.getTableFormat());
		try {
			accessor.getParquetMetadata();
			Assert.fail();
		} catch (IllegalStateException e) {
			Assert.assertEquals("shouldn't call getParquetMetadata on a AO table!", e.getMessage());
		}

		accessor = MetadataAccessor.newInstanceUsingFile(
				getFilePathUnderClasspath("/sample_parquet_metadata.yaml"));
		Assert.assertEquals(HAWQTableFormat.Parquet, accessor.getTableFormat());
		try {
			accessor.getAOMetadata();
			Assert.fail();
		} catch (IllegalStateException e) {
			Assert.assertEquals("shouldn't call getAOMetadata on a Parquet table!", e.getMessage());
		}
	}

	private String getFilePathUnderClasspath(String filename) {
		return getClass().getResource(filename).getPath();
	}
}
