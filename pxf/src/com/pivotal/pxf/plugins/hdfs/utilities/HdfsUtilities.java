package com.pivotal.pxf.plugins.hdfs.utilities;

import com.pivotal.pxf.api.utilities.InputData;
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

/*
 * HdfsUtilities class exposes helper methods for PXF classes
 */
public class HdfsUtilities {
    private static Log Log = LogFactory.getLog(HdfsUtilities.class);
    private static CompressionCodecFactory factory =
            new CompressionCodecFactory(new Configuration());

    /*
     * Helper routine to get a compression codec class
     */
    private static Class<? extends CompressionCodec> getCodecClass(
            Configuration conf, String name) {

        Class<? extends CompressionCodec> codecClass;
        try {
            codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
        }
        return codecClass;
    }

    /**
     * Helper routine to get compression codec through reflection
     *
     * @param conf configuration used for reflection
     * @param name codec name
     * @return generated CompressionCodec
     */
    public static CompressionCodec getCodec(Configuration conf, String name) {
        return ReflectionUtils.newInstance(getCodecClass(conf, name), conf);
    }

    /**
     * Helper routine to get compression codec class by path (file suffix)
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
        Log.debug((codecClass == null ? "No codec" : "Codec " + codecClass)
                + " was found for file " + path);
        return codecClass;
    }

    /**
     * Returns true if the needed codec is splittable.
     * If no codec is needed returns true as well.
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
     * @param inputData container holding all parameters
     * @return if the request can be run in multithreaded mode.
     */
    public static boolean isThreadSafe(InputData inputData) {
        Configuration conf = new Configuration();
        String dataDir = inputData.path();
        if (!inputData.threadSafe()) {
            return false;
        }
        String writeCodec = inputData.compressCodec();
        Class<? extends CompressionCodec> codecClass = (writeCodec != null)
                ? HdfsUtilities.getCodecClass(conf, writeCodec)
                : HdfsUtilities.getCodecClassByPath(dataDir);
        /* bzip2 codec is not thread safe */
        return (codecClass == null || !BZip2Codec.class.isAssignableFrom(codecClass));
    }

    /**
     * Prepare byte serialization of a file split information
     * (start, length, hosts) using {@link ObjectOutputStream}.
     *
     * @param fsp file split to be serialized
     * @return byte serialization of fsp
     * @throws IOException
     */
    public static byte[] prepareFragmentMetadata(FileSplit fsp) throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeLong(fsp.getStart());
        objectStream.writeLong(fsp.getLength());
        objectStream.writeObject(fsp.getLocations());

        return byteArrayStream.toByteArray();
    }

    /**
     * Parse fragment metadata and return matching {@link FileSplit}
     *
     * @param inputData request input data
     * @return FileSplit with fragment metadata
     */
    public static FileSplit parseFragmentMetadata(InputData inputData) {
        try {
            byte[] serializedLocation = inputData.getFragmentMetadata();
            if (serializedLocation == null) {
                throw new IllegalArgumentException("Missing fragment location information");
            }

            ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedLocation);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            long start = objectStream.readLong();
            long end = objectStream.readLong();

            String[] hosts = (String[]) objectStream.readObject();

            FileSplit fileSplit = new FileSplit(new Path(inputData.path()),
                    start,
                    end,
                    hosts);

            Log.debug("parsed file split: path " + inputData.path() +
                    ", start " + start + ", end " + end +
                    ", hosts " + ArrayUtils.toString(hosts));

            return fileSplit;

        } catch (Exception e) {
            throw new RuntimeException("Exception while reading expected fragment metadata", e);
        }
    }
}
