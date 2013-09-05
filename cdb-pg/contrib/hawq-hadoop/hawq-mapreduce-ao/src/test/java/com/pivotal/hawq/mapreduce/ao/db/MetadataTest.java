package com.pivotal.hawq.mapreduce.ao.db;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;

public class MetadataTest
{
	@Ignore
	public void testMetadatCtorFromFile()
	{
		Metadata metadataFromDB = null, metadataFromFile = null;
		Exception exception = null;
		try
		{
			metadataFromDB = new Metadata("localhost:5432/ao_nocomp_1g_partition",
					"wangj77", "", "public.lineitem");
			metadataFromFile = new Metadata(
					"/Users/wangj77/Desktop/ao_nocomp_1g_partition_lineitem");
		}
		catch (Exception e)
		{
			e.printStackTrace();
			exception = e;
		}
		assertNull(exception);
		String db = metadataFromDB.toString();
		String file = metadataFromFile.toString();
		assertTrue(db.equals(file));
	}
}
