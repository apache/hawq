package com.pivotal.pxf.fragmenters;

import com.pivotal.pxf.utilities.InputData;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/*
 * Test HiveDataFragmenter
 */
public class HiveDataFragmenterTest 
{
	InputData input;
	
    @Test
    public void testparseMetastoreUri() throws Exception 
	{
		String uri = "thrift://isenshackamac.corp.emc.com:9084";
		HiveDataFragmenter.parseMetastoreUri(uri);
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_HOST, "isenshackamac.corp.emc.com");
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_PORT, 9084);
		
		/* when the uri is invalid we default to host:port = localhost:9083*/
		uri = "thrift:/isenshackamac.corp.emc.com:9084";
		HiveDataFragmenter.METASTORE_DEFAULT_HOST = "localhost";
		HiveDataFragmenter.METASTORE_DEFAULT_PORT = 9083;
		HiveDataFragmenter.parseMetastoreUri(uri);
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_HOST, "localhost");
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_PORT, 9083);
		
		/* when the uri is invalid we default to host:port = localhost:9083*/
		uri = "thrift://isenshackamac.corp.emc.com9084";
		HiveDataFragmenter.METASTORE_DEFAULT_HOST = "localhost";
		HiveDataFragmenter.METASTORE_DEFAULT_PORT = 9083;		
		HiveDataFragmenter.parseMetastoreUri(uri);
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_HOST, "localhost");
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_PORT, 9083);
		
		/* when the uri is invalid we default to host:port = localhost:9083*/
		uri = null;
		HiveDataFragmenter.METASTORE_DEFAULT_HOST = "localhost";
		HiveDataFragmenter.METASTORE_DEFAULT_PORT = 9083;		
		HiveDataFragmenter.parseMetastoreUri(uri);
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_HOST, "localhost");
		assertEquals(HiveDataFragmenter.METASTORE_DEFAULT_PORT, 9083);
    }
}
