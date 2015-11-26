package com.pivotal.hawq.mapreduce.ao.io;

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


import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOSplit;
import com.pivotal.hawq.mapreduce.ao.util.CompressZlib;
import com.pivotal.hawq.mapreduce.util.HAWQConvertUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * Supply analysis of binary file. Get different header in file and analyze them
 * to read correct data next.
 */
public final class HAWQAOFileReader
{

	private final int smallContentArraySize = 4 * 1024 * 1024; // 4M
	private final int largeContentArraySize = 5 * smallContentArraySize; // 20M
	private final int offsetArraySize = 32 * 1024; // 32K

	private FileSystem fs = null;
	private FSDataInputStream dis = null;

	private boolean checksum;
	private int blockSize;
	private String compressType;

	private long fileLength;
	private long readLength;

	// 2M Bytes for memtuples in small content
	private byte[] smallTuple_Varblock = new byte[smallContentArraySize];
	// Default 10M Bytes for memtuples in large content
	private byte[] largeTuple = new byte[largeContentArraySize];
	// Actual data size in memtuples
	private int effectiveSmallTupleLength = -1;
	// Actual data size in memtuples
	private int effectiveLargeTupleLength = -1;

	private int[] offsets = new int[offsetArraySize];
	private int effectiveOffsetLength = -1;
	private int currentPosInOffsets = -1;

	private byte[] buffer = new byte[8];

	public HAWQAOFileReader(Configuration conf, InputSplit split)
			throws IOException
	{
		HAWQAOSplit aosplit = (HAWQAOSplit) split;
		checksum = aosplit.getChecksum();
		compressType = aosplit.getCompressType().toLowerCase();
		if (!compressType.equals("none") && !compressType.equals("quicklz")
				&& !compressType.equals("zlib") && !compressType.equals(""))
			throw new IOException("Compression type " + compressType
					+ " is not supported yet");
		blockSize = aosplit.getBlockSize();
		fileLength = aosplit.getLength();
		readLength = 0;

		Path path = aosplit.getPath();
		fs = path.getFileSystem(conf);
		dis = fs.open(path);
	}

	/**
	 * Read tuple into record
	 * 
	 * @param record
	 * @return false if failed to read a record from file
	 * @throws IOException
	 * @throws HAWQException
	 */
	public boolean readRecord(HAWQAORecord record) throws IOException,
			HAWQException
	{
		if (effectiveSmallTupleLength == -1 && effectiveLargeTupleLength == -1)
		{
			if (readLength >= fileLength)
				return false;
			if (dis.available() == 0)
			{
				if (readLength < fileLength)
					System.err.println("Expect file length: " + fileLength
							+ ", read length in fact: " + readLength);
				return false;
			}

			readMemtuplesFromDis();
			if (effectiveSmallTupleLength == -1
					&& effectiveLargeTupleLength == -1)
				return false;
		}
		record.reset();
		getRecordFromMemtuples(record);
		return true;
	}

	/**
	 * @throws IOException
	 */
	public void close() throws IOException
	{
		if (dis != null)
			dis.close();
	}

	/**
	 * if memtuples is empty, then read bytes from data input stream and saved
	 * in memtuples
	 * 
	 * @throws IOException
	 * @throws HAWQException
	 */
	private void readMemtuplesFromDis() throws HAWQException, IOException
	{
		int blockHeaderBytes_0_3, blockHeaderBytes_4_7;
		do {
			dis.readFully(buffer);
			readLength += 8;
			blockHeaderBytes_0_3 = HAWQConvertUtil.bytesToInt(buffer, 0);
			blockHeaderBytes_4_7 = HAWQConvertUtil.bytesToInt(buffer, 4);
		}while (blockHeaderBytes_0_3 == 0 && blockHeaderBytes_4_7 == 0);

		int reserved0 = (blockHeaderBytes_0_3 >> 31);
		if (reserved0 != 0)
			throw new HAWQException("reserved0 of AOHeader is not 0",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int headerKind = (blockHeaderBytes_0_3 & 0x70000000) >> 28;

		switch (headerKind)
		{
		case AoHeaderKind.SmallContent:
			readMemtuplesFromSmallContent(blockHeaderBytes_0_3,
					blockHeaderBytes_4_7, true);
			break;
		case AoHeaderKind.LargeContent:
			readMemtuplesFromLargeContent(blockHeaderBytes_0_3,
					blockHeaderBytes_4_7);
			break;
		case AoHeaderKind.NonBulkDenseContent:
			throw new HAWQException("Not support NonBulkDenseContent yet");
		case AoHeaderKind.BulkDenseContent:
			throw new HAWQException("Not support BulkDenseContent yet");
		default:
			throw new HAWQException("Unexpected Append-Only header kind "
					+ headerKind, HAWQException.WRONGFILEFORMAT_EXCEPTION);
		}
	}

	/**
	 * Read bytes into memtuples
	 * 
	 * @param blockHeaderBytes_0_3
	 *            byte 0 to byte 3 of small content header
	 * @param blockHeaderBytes_4_7
	 *            byte 4 to byte 7 of small content header
	 * @param isSmall
	 *            true means this small content dose not belong to a large
	 *            content
	 * @throws IOException
	 * @throws HAWQException
	 */
	private void readMemtuplesFromSmallContent(int blockHeaderBytes_0_3,
			int blockHeaderBytes_4_7, boolean isSmall) throws HAWQException,
			IOException
	{
		// Small content header
		int rowCount = (blockHeaderBytes_0_3 & 0x00FFFC00) >> 10;
		int executorBlockKind = (blockHeaderBytes_0_3 & 0x07000000) >> 24;
		int hasFirstRowNum = (blockHeaderBytes_0_3 & 0x08000000) >> 27;
		if (checksum)
		{
			// skip checksum
			dis.readFully(buffer);
			readLength += 8;
		}
		if (hasFirstRowNum == 1)
		{
			// TODO: firstRowNum skip??
			dis.readFully(buffer);
			readLength += 8;
		}
		int uncompressedLen = ((blockHeaderBytes_0_3 & 0x000003FF) << 11)
				| (((blockHeaderBytes_4_7 & 0xFFE00000) >> 21) & 0x000007FF);
		int compressedLen = (blockHeaderBytes_4_7 & 0x001FFFFF);
		int overallByteLen;
		if (compressedLen == 0)
			// Uncompressed
			overallByteLen = AoRelationVersion.RoundUp(uncompressedLen,
					AoRelationVersion.GetLatest());
		else
			overallByteLen = AoRelationVersion.RoundUp(compressedLen,
					AoRelationVersion.GetLatest());
		readLength += overallByteLen;
		switch (executorBlockKind)
		{
		case AoExecutorBlockKind.SingleRow:
			// Single row
			if (isSmall && rowCount != 1)
				throw new HAWQException("Wrong row count for single row: "
						+ rowCount, HAWQException.WRONGFILEFORMAT_EXCEPTION);
			if (compressedLen != 0)
			{
				byte[] tempBytes = new byte[overallByteLen];
				dis.readFully(tempBytes);
				if (compressType.equals("zlib"))
					tempBytes = CompressZlib.decompress(tempBytes);
				else if (compressType.equals("quicklz"))
				{
					throw new IOException("compresstype quicklz is not supported anymore");
				}
				if (tempBytes.length != uncompressedLen)
					throw new IOException("tempBytes.length != uncompressedLen");
				System.arraycopy(tempBytes, 0, smallTuple_Varblock, 0,
						tempBytes.length);
				effectiveSmallTupleLength = tempBytes.length;
			}
			else
			{
				effectiveSmallTupleLength = overallByteLen;
				while (overallByteLen != 0)
					overallByteLen -= dis.read(smallTuple_Varblock,
							effectiveSmallTupleLength - overallByteLen,
							overallByteLen);
			}
			if (isSmall)
			{
				offsets[0] = 0;
				currentPosInOffsets = 0;
				effectiveOffsetLength = 1;
			}
			break;
		case AoExecutorBlockKind.VarBlock:
			// Multiple rows
			if (compressedLen != 0)
			{
				byte[] allbytes = new byte[overallByteLen];
				dis.readFully(allbytes);
				if (compressType.equals("zlib"))
					allbytes = CompressZlib.decompress(allbytes);
				else if (compressType.equals("quicklz"))
				{
					throw new IOException("compresstype quicklz is not supported anymore");
				}
				if (allbytes.length != uncompressedLen)
					throw new IOException("allbytes.length != uncompressedLen");
				overallByteLen = allbytes.length;
				System.arraycopy(allbytes, 0, smallTuple_Varblock, 0,
						allbytes.length);
			}
			else
			{
				int tempLength = overallByteLen;
				while (tempLength != 0)
					tempLength -= dis.read(smallTuple_Varblock, overallByteLen
							- tempLength, tempLength);
			}

			int posInAllBytes = 0;
			int varblockHeader_0_3 = HAWQConvertUtil.bytesToInt(
					smallTuple_Varblock, posInAllBytes);
			posInAllBytes += 4;
			int varblockHeader_4_7 = HAWQConvertUtil.bytesToInt(
					smallTuple_Varblock, posInAllBytes);
			posInAllBytes += 4;
			overallByteLen -= 2 * 4;
			int varBlockVersion = (varblockHeader_0_3 & 0x03000000) >> 24;
			if (varBlockVersion != 1)
				throw new HAWQException("version " + varBlockVersion
						+ " is not 1", HAWQException.WRONGFILEFORMAT_EXCEPTION);
			int reserved_varblock = (varblockHeader_0_3 & 0x7C000000) >> 26;
			if (reserved_varblock != 0)
				throw new HAWQException("Reserved not 0",
						HAWQException.WRONGFILEFORMAT_EXCEPTION);
			int moreReserved_varblock = (varblockHeader_4_7 & 0xFF000000) >> 24;
			if (moreReserved_varblock != 0)
				throw new HAWQException("More reserved not 0",
						HAWQException.WRONGFILEFORMAT_EXCEPTION);
			int itemCount = (varblockHeader_4_7 & 0x00FFFFFF);
			if (rowCount != itemCount)
				throw new HAWQException("row count in content header is "
						+ rowCount + " and item count in varblock header is "
						+ itemCount, HAWQException.WRONGFILEFORMAT_EXCEPTION);
			int itemLenByteSum = varblockHeader_0_3 & 0x00FFFFFF;
			effectiveSmallTupleLength = itemLenByteSum + 8;
			posInAllBytes += itemLenByteSum;
			overallByteLen -= itemLenByteSum;
			int offsetAreSmall = (varblockHeader_0_3 >> 31) & 0x00000001;
			if (offsetAreSmall == 1)
			{
				effectiveOffsetLength = overallByteLen / 2;
				for (int i = 0; i < effectiveOffsetLength; ++i)
				{
					offsets[i] = ((int) HAWQConvertUtil.bytesToShort(
							smallTuple_Varblock, posInAllBytes)) & 0x0000FFFF;
					posInAllBytes += 2;
				}
			}
			else
			{
				effectiveOffsetLength = overallByteLen / 3;
				for (int i = 0; i < effectiveOffsetLength; ++i)
				{
					offsets[i] = HAWQConvertUtil.threeBytesToInt(
							smallTuple_Varblock, posInAllBytes);
					posInAllBytes += 3;
				}
			}
			currentPosInOffsets = 0;
			break;
		default:
			throw new HAWQException("Unexpected executor block kind "
					+ executorBlockKind,
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		}
	}

	private void readMemtuplesFromLargeContent(int blockHeaderBytes_0_3,
			int blockHeaderBytes_4_7) throws HAWQException, IOException
	{
		int reserved1 = (blockHeaderBytes_0_3 & 0x00800000) >> 23;
		if (reserved1 != 0)
			throw new HAWQException(
					"reserved1 of AOHeader_LargeContent is not 0",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int hasFirstRowNum = (blockHeaderBytes_0_3 & 0x08000000) >> 27;
		if (checksum)
		{
			// skip checksum
			dis.readFully(buffer);
			readLength += 8;
		}
		if (hasFirstRowNum == 1)
		{
			// TODO: firstRowNum skip??
			dis.readFully(buffer);
			readLength += 8;
		}
		int executorBlockKind = (blockHeaderBytes_0_3 & 0x07000000) >> 24;
		// AppendOnlyExecutorReadBlock_GetContents:591
		if (executorBlockKind != AoExecutorBlockKind.SingleRow)
			throw new HAWQException(
					"executor block kind of large content should be "
							+ AoExecutorBlockKind.SingleRow + " rather than "
							+ executorBlockKind,
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int largeRowCount = ((blockHeaderBytes_0_3 & 0x007FFFFF) << 2)
				| (((blockHeaderBytes_4_7 & 0xC0000000) >> 30) & 0x00000003);
		if (largeRowCount != 1)
			throw new HAWQException(
					"row count in large content should be 1 rather than "
							+ largeRowCount,
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int largeContentLength = blockHeaderBytes_4_7 & 0x3FFFFFFF;
		int smallNumOfLarge = (largeContentLength - 1) / blockSize + 1;
		effectiveLargeTupleLength = 0;
		for (int i = 0; i < smallNumOfLarge; ++i)
		{
			dis.readFully(buffer);
			readLength += 8;
			int smallblockHeaderBytes_0_3 = HAWQConvertUtil.bytesToInt(buffer,
					0);
			int smallblockHeaderBytes_4_7 = HAWQConvertUtil.bytesToInt(buffer,
					4);
			readMemtuplesFromSmallContent(smallblockHeaderBytes_0_3,
					smallblockHeaderBytes_4_7, false);
			if (effectiveLargeTupleLength + effectiveSmallTupleLength > largeTuple.length)
			{
				// buffer for large content is not big enough, each allocate 2M
				// Bytes for largeMemtuples
				byte[] temp = new byte[largeTuple.length
						+ smallContentArraySize * 2];
				System.arraycopy(largeTuple, 0, temp, 0,
						effectiveLargeTupleLength);
				largeTuple = temp;
			}
			System.arraycopy(smallTuple_Varblock, 0, largeTuple,
					effectiveLargeTupleLength, effectiveSmallTupleLength);
			effectiveLargeTupleLength += effectiveSmallTupleLength;
			effectiveSmallTupleLength = -1;
		}
		if (largeContentLength != effectiveLargeTupleLength)
			throw new HAWQException("Fail to read tuple from file",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		offsets = new int[1];
		offsets[0] = 0;
		currentPosInOffsets = 0;
	}

	/**
	 * Get tuple from array memtuples
	 * 
	 * @return array contains values of one tuple(must be imperfect)
	 * @throws IOException
	 * @throws HAWQException
	 */
	private void getRecordFromMemtuples(HAWQAORecord record)
			throws IOException, HAWQException
	{
		if (effectiveLargeTupleLength != -1)
		{
			// large tuple
			record.setMemtuples(largeTuple, 0, effectiveLargeTupleLength - 1);
			effectiveLargeTupleLength = -1;
		}
		else
		{
			// small tuples
			if (currentPosInOffsets == effectiveOffsetLength - 1
					|| offsets[currentPosInOffsets + 1] == 0)
			{
				record.setMemtuples(smallTuple_Varblock,
									offsets[currentPosInOffsets],
									effectiveSmallTupleLength - 1);
				effectiveSmallTupleLength = -1;
				effectiveOffsetLength = -1;
				currentPosInOffsets = -1;
			}
			else
			{
				record.setMemtuples(smallTuple_Varblock,
									offsets[currentPosInOffsets],
									offsets[currentPosInOffsets + 1] - 1);
			}
			++currentPosInOffsets;
		}
	}

	public static class AoHeaderKind
	{
		public static final int SmallContent = 1;
		public static final int LargeContent = 2;
		public static final int NonBulkDenseContent = 3;
		public static final int BulkDenseContent = 4;
	}

	public static class AoExecutorBlockKind
	{
		public static final int None = 0;
		public static final int VarBlock = 1;
		public static final int SingleRow = 2;
		public static final int Max = Integer.MAX_VALUE;
	}

	public static class AoRelationVersion
	{
		public static final int None = 0;
		public static final int Original = 1;
		public static final int Aligned64bit = 2;
		public static final int Max = Integer.MAX_VALUE;

		public static int GetLatest()
		{
			return Aligned64bit;
		}

		public static int RoundUp(int length, int version)
		{
			return (version > Original) ? (((length + 7) / 8) * 8)
					: (((length + 3) / 4) * 4);
		}
	}
}
