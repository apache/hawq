package org.apache.hawq.pxf.plugins.hdfs.utilities;

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


import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.List;

/**
 * HdfsUtilities class exposes helper methods for PXF classes.
 */
public class HdfsUtilities {
    private static final Log LOG = LogFactory.getLog(HdfsUtilities.class);
    private static Configuration config = new Configuration();
    private static CompressionCodecFactory factory = new CompressionCodecFactory(
            config);

    /**
     * Hdfs data sources are absolute data paths. Method ensures that dataSource
     * begins with '/'.
     *
     * @param dataSource The HDFS path to a file or directory of interest.
     *            Retrieved from the client request.
     * @return an absolute data path
     */
    public static String absoluteDataPath(String dataSource) {
        return (dataSource.charAt(0) == '/') ? dataSource : "/" + dataSource;
    }

    /*
     * Helper routine to get a compression codec class
     */
    private static Class<? extends CompressionCodec> getCodecClass(Configuration conf,
                                                                   String name) {

        Class<? extends CompressionCodec> codecClass;
        try {
            codecClass = conf.getClassByName(name).asSubclass(
                    CompressionCodec.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec " + name
                    + " was not found.", e);
        }
        return codecClass;
    }

    /**
     * Helper routine to get compression codec through reflection.
     *
     * @param conf configuration used for reflection
     * @param name codec name
     * @return generated CompressionCodec
     */
    public static CompressionCodec getCodec(Configuration conf, String name) {
        return ReflectionUtils.newInstance(getCodecClass(conf, name), conf);
    }

    /**
     * Helper routine to get compression codec class by path (file suffix).
     *
     * @param path path of file to get codec for
     * @return matching codec class for the path. null if no codec is needed.
     */
    private static Class<? extends CompressionCodec> getCodecClassByPath(String path) {

        Class<? extends CompressionCodec> codecClass = null;
        CompressionCodec codec = factory.getCodec(new Path(path));
        if (codec != null) {
            codecClass = codec.getClass();
        }
        LOG.debug((codecClass == null ? "No codec" : "Codec " + codecClass)
                + " was found for file " + path);
        return codecClass;
    }

    /**
     * Returns true if the needed codec is splittable. If no codec is needed
     * returns true as well.
     *
     * @param path path of the file to be read
     * @return if the codec needed for reading the specified path is splittable.
     */
    public static boolean isSplittableCodec(Path path) {

        final CompressionCodec codec = factory.getCodec(path);
        if (null == codec) {
            return true;
        }

        return codec instanceof SplittableCompressionCodec;
    }

    /**
     * Checks if requests should be handle in a single thread or not.
     *
     * @param dataDir hdfs path to the data source
     * @param compCodec the fully qualified name of the compression codec
     * @return if the request can be run in multi-threaded mode.
     */
    public static boolean isThreadSafe(String dataDir, String compCodec) {

        Class<? extends CompressionCodec> codecClass = (compCodec != null) ? HdfsUtilities.getCodecClass(
                config, compCodec) : HdfsUtilities.getCodecClassByPath(dataDir);
        /* bzip2 codec is not thread safe */
        return (codecClass == null || !BZip2Codec.class.isAssignableFrom(codecClass));
    }

    /**
     * Prepares byte serialization of a file split information (start, length,
     * hosts) using {@link ObjectOutputStream}.
     *
     * @param fsp file split to be serialized
     * @return byte serialization of fsp
     * @throws IOException if I/O errors occur while writing to the underlying
     *             stream
     */
    public static byte[] prepareFragmentMetadata(FileSplit fsp)
            throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(
                byteArrayStream);
        objectStream.writeLong(fsp.getStart());
        objectStream.writeLong(fsp.getLength());
        objectStream.writeObject(fsp.getLocations());

        return byteArrayStream.toByteArray();
    }

    /**
     * Parses fragment metadata and return matching {@link FileSplit}.
     *
     * @param inputData request input data
     * @return FileSplit with fragment metadata
     */
    public static FileSplit parseFragmentMetadata(InputData inputData) {
        try {
            byte[] serializedLocation = inputData.getFragmentMetadata();
            if (serializedLocation == null) {
                throw new IllegalArgumentException(
                        "Missing fragment location information");
            }

            ByteArrayInputStream bytesStream = new ByteArrayInputStream(
                    serializedLocation);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            long start = objectStream.readLong();
            long end = objectStream.readLong();

            String[] hosts = (String[]) objectStream.readObject();

            FileSplit fileSplit = new FileSplit(new Path(
                    inputData.getDataSource()), start, end, hosts);

            LOG.debug("parsed file split: path " + inputData.getDataSource()
                    + ", start " + start + ", end " + end + ", hosts "
                    + ArrayUtils.toString(hosts));

            return fileSplit;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception while reading expected fragment metadata", e);
        }
    }

    /**
     * Accessing the Avro file through the "unsplittable" API just to get the
     * schema. The splittable API (AvroInputFormat) which is the one we will be
     * using to fetch the records, does not support getting the Avro schema yet.
     *
     * @param conf Hadoop configuration
     * @param dataSource Avro file (i.e fileName.avro) path
     * @return the Avro schema
     * @throws IOException if I/O error occurred while accessing Avro schema file
     */
    public static Schema getAvroSchema(Configuration conf, String dataSource)
            throws IOException {
        FsInput inStream = new FsInput(new Path(dataSource), conf);
        DatumReader<GenericRecord> dummyReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dummyFileReader = new DataFileReader<>(
                inStream, dummyReader);
        Schema schema = dummyFileReader.getSchema();
        dummyFileReader.close();
        return schema;
    }

    /**
     * Returns string serialization of list of fields. Fields of binary type
     * (BYTEA) are converted to octal representation to make sure they will be
     * relayed properly to the DB.
     *
     * @param complexRecord list of fields to be stringified
     * @param delimiter delimiter between fields
     * @return string of serialized fields using delimiter
     */
    public static String toString(List<OneField> complexRecord, String delimiter) {
        StringBuilder buff = new StringBuilder();
        String delim = ""; // first iteration has no delimiter
        if(complexRecord == null)
            return "";
        for (OneField complex : complexRecord) {
            if (complex.type == DataType.BYTEA.getOID()) {
                /** Serialize byte array as string */
                buff.append(delim);
                Utilities.byteArrayToOctalString((byte[]) complex.val, buff);
            } else {
                buff.append(delim).append(complex.val);
            }
            delim = delimiter;
        }
        return buff.toString();
    }
}
