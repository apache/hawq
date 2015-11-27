package org.apache.hawq.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hdfs.DFSInputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.*;
import org.mockito.invocation.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tester for the ChunkReader class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ChunkReader.class})
public class ChunkReaderTest {
	
	ChunkReader reader;
	/* Mocking the stream class that accesses the actual data */
	DFSInputStream mockStream;

    /*
     * setUp function called before each test.
	 */
    @Before
    public void setUp() throws Exception {
		mockStream = mock(DFSInputStream.class); 	
    }

    /*
	 * Simulate the empty file case
	 */
    @Test
    public void readEmptyFile() throws Exception {
		reader = new ChunkReader(mockStream);
		when( mockStream.read( (byte [])Mockito.anyObject()) ).thenReturn(0);
		
		Writable out = new ChunkWritable();
		int maxBytesToConsume = 1024*1024;
		assertEquals(0, reader.readLine(out, maxBytesToConsume));
    }

	/*
	 * Read one line
	 */
    @Test
    public void readOneLine() throws Exception {
		reader = new ChunkReader(mockStream);
		when( mockStream.read( (byte [])Mockito.anyObject()) ).thenAnswer(new Answer<java.lang.Number>() {
			@Override
			public java.lang.Number answer(InvocationOnMock invocation) throws Throwable {
				byte[] buf = (byte[]) invocation.getArguments()[0];
				
				byte [] source = "OneLine\nTwoLine\n".getBytes();
				System.arraycopy(source, 0, buf, 0, source.length);
				return new java.lang.Byte(buf[0]);
			}
		});
		
		ChunkWritable out = new ChunkWritable();
		int maxBytesToConsume = 1024*1024;
		// read first line
		assertEquals("OneLine\n".length()
					 , reader.readLine(out, maxBytesToConsume) );
		assertEquals("OneLine\n", new String(out.box) );

		// read second line
		assertEquals("TwoLine\n".length(), reader.readLine(out, maxBytesToConsume) );
		assertEquals("TwoLine\n", new String(out.box) );
    }
	
	/*
	 * Read one line
	 */
    @Test
    public void readChunk() throws Exception {
		reader = new ChunkReader(mockStream);
		when( mockStream.read( (byte [])Mockito.anyObject()) ).thenAnswer(new Answer<java.lang.Number>() {
			@Override
			public java.lang.Number answer(InvocationOnMock invocation) throws Throwable {
				byte[] buf = (byte[]) invocation.getArguments()[0];
				
				byte [] source = "OneLine\nTwoLine\n".getBytes();
				System.arraycopy(source, 0, buf, 0, source.length);
				return new java.lang.Integer(source.length);
			}
		});
		
		ChunkWritable out = new ChunkWritable();
		int maxBytesToConsume = 10; /* make readChunk return after reading the first "chunk": OneLine\nTwoLine\n */
		// read chunk
		assertEquals("OneLine\nTwoLine\n".length()
					 , reader.readChunk(out, maxBytesToConsume) );
		assertEquals("OneLine\nTwoLine\n", new String(out.box) );
    }	
	
}














