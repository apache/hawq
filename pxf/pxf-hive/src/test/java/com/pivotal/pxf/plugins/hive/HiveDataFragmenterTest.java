package com.pivotal.pxf.plugins.hive;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.logging.Log;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/*
 * Test HiveDataFragmenter
 */
@RunWith(JUnitParamsRunner.class)
public class HiveDataFragmenterTest {
    /* parameterized testing for testparseMetastoreUri */
    private static final Object[] getHostport() {
        /* input data for the tests - each Object[] is an iteration of testparseMetastoreUri() */
        return new Object[]{
                new Object[]{"thrift://isenshackamac.corp.emc.com:9084",
                        "isenshackamac.corp.emc.com",
                        9084}, /* valid uri - expect success */

			/* when parsing fails we expect testparseMetastoreUri() to return default host and port */
                new Object[]{"thrift:/isenshackamac.corp.emc.com:9084",
                        HiveDataFragmenter.METASTORE_DEFAULT_HOST,
                        HiveDataFragmenter.METASTORE_DEFAULT_PORT}, /* invalid uri - expect failure */
                new Object[]{"thrift://isenshackamac.corp.emc.com9084",
                        HiveDataFragmenter.METASTORE_DEFAULT_HOST,
                        HiveDataFragmenter.METASTORE_DEFAULT_PORT}, /* invalid uri - expect failure */
                new Object[]{null,
                        HiveDataFragmenter.METASTORE_DEFAULT_HOST,
                        HiveDataFragmenter.METASTORE_DEFAULT_PORT} /* invalid uri - expect failure */
        };
    }

    @Test
    @Parameters(method = "getHostport")
    public void testparseMetastoreUri(String uri, String expectedHost, int expectedPort) throws Exception {
        Log Log = mock(Log.class);

        HiveDataFragmenter.Metastore ms = HiveDataFragmenter.parseMetastoreUri(uri, Log);
        assertEquals(ms.host, expectedHost);
        assertEquals(ms.port, expectedPort);
    }
}
