package org.apache.hawq.pxf.plugins.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY;
import static org.apache.hadoop.mapreduce.lib.input.LineRecordReader.MAX_LINE_LENGTH;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSInputStream.ReadStatistics;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * ChunkRecordReader is designed for fast reading of a file split. The idea is
 * to bring chunks of data instead of single records. The chunks contain many
 * records and the chunk end is not aligned on a record boundary. The size of
 * the chunk is a class hardcoded parameter - CHUNK_SIZE. This behaviour sets
 * this reader apart from the other readers which will fetch one record and stop
 * when reaching a record delimiter.
 */
public class ChunkRecordReader implements
        RecordReader<LongWritable, ChunkWritable> {
    private static final Log LOG = LogFactory.getLog(ChunkRecordReader.class.getName());

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private long fileLength;
    private ChunkReader in;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    private int maxLineLength;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private static final int CHUNK_SIZE = 1024 * 1024;

    /**
     * Translates the FSDataInputStream into a DFSInputStream.
     */
    private DFSInputStream getInputStream() {
        return (DFSInputStream) (fileIn.getWrappedStream());
    }

    /**
     * Returns statistics of the input stream's read operation: total bytes
     * read, bytes read locally, bytes read in short-circuit (directly from file
     * descriptor).
     *
     * @return an instance of ReadStatistics class
     */
    public ReadStatistics getReadStatistics() {
        return getInputStream().getReadStatistics();
    }

    /**
     * Constructs a ChunkRecordReader instance.
     *
     * @param job the job configuration
     * @param split contains the file name, begin byte of the split and the
     *            bytes length
     * @throws IOException if an I/O error occurs when accessing the file or
     *             creating input stream to read from it
     */
    public ChunkRecordReader(Configuration job, FileSplit split)
            throws IOException {
        maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        validateLength(maxLineLength);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        job.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
        final FileSystem fs = file.getFileSystem(job);
        fs.setVerifyChecksum(false);
        fileIn = fs.open(file, ChunkReader.DEFAULT_BUFFER_SIZE);
        fileLength = getInputStream().getFileLength();
        if (isCompressedInput()) {
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
                        fileIn, decompressor, start, end,
                        SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = new ChunkReader(cIn);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn; // take pos from compressed stream
            } else {
                in = new ChunkReader(codec.createInputStream(fileIn,
                        decompressor));
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            in = new ChunkReader(fileIn);
            filePosition = fileIn;
        }
        /*
         * If this is not the first split, we always throw away first record
         * because we always (except the last split) read one extra line in
         * next() method.
         */
        if (start != 0) {
            start += in.readLine(new ChunkWritable(), maxBytesToConsume(start));
        }
        this.pos = start;
    }

    /**
     * Used by the client of this class to create the 'key' output parameter for
     * next() method.
     *
     * @return an instance of LongWritable
     */
    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    /**
     * Used by the client of this class to create the 'value' output parameter
     * for next() method.
     *
     * @return an instance of ChunkWritable
     */
    @Override
    public ChunkWritable createValue() {
        return new ChunkWritable();
    }

    /**
     * Fetches the next data chunk from the file split. The size of the chunk is
     * a class hardcoded parameter - CHUNK_SIZE. This behaviour sets this reader
     * apart from the other readers which will fetch one record and stop when
     * reaching a record delimiter.
     *
     * @param key - output parameter. When method returns will contain the key -
     *            the number of the start byte of the chunk
     * @param value - output parameter. When method returns will contain the
     *            value - the chunk, a byte array inside the ChunkWritable
     *            instance
     * @return false - when end of split was reached
     * @throws IOException if an I/O error occurred while reading the next chunk
     *             or line
     */
    @Override
    public synchronized boolean next(LongWritable key, ChunkWritable value)
            throws IOException {
        /*
         * Usually a record is spread between the end of current split and the
         * beginning of next split. So when reading the last record in the split
         * we usually need to cross over to the next split. This tricky logic is
         * implemented in ChunkReader.readLine(). In order not to rewrite this
         * logic we will read the lust chunk in the split with readLine(). For a
         * split of 120M, reading the last 1M line by line doesn't have a huge
         * impact. Applying a factor to the last chunk to make sure we start
         * before the last record.
         */
        float factor = 1.5f;
        int limit = (int) (factor * CHUNK_SIZE);
        long curPos = getFilePosition();
        int newSize = 0;

        while (curPos <= end) {
            key.set(pos);

            if ((end - curPos) > limit) {
                newSize = in.readChunk(value, CHUNK_SIZE);
            } else {
                newSize = in.readLine(value,
                        Math.max(maxBytesToConsume(pos), maxLineLength));
            }
            if (newSize == 0) {
                break;
            }

            pos += newSize;

            if (pos == fileLength) { /*
                                      * in case text file last character is not
                                      * a linefeed
                                      */
                if (value.box[value.box.length - 1] != '\n') {
                    int newLen = value.box.length + 1;
                    byte[] tmp = new byte[newLen];
                    System.arraycopy(value.box, 0, tmp, 0, newLen - 1);
                    tmp[newLen - 1] = '\n';
                    value.box = tmp;
                }
            }

            return true;
        }
        /*
         * if we got here, either newSize was 0 or curPos is bigger than end
         */

        return false;
    }

    /**
     * Gets the progress within the split.
     */
    @Override
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start)
                    / (float) (end - start));
        }
    }

    /**
     * Returns the position of the unread tail of the file
     *
     * @return pos - start byte of the unread tail of the file
     */
    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    /**
     * Closes the input stream.
     */
    @Override
    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    private void validateLength(int maxLineLength) {
        if (maxLineLength <= 0)
            throw new IllegalArgumentException(
                    "maxLineLength must be a positive value");
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }

    private int maxBytesToConsume(long pos) {
        return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(
                Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }
} // class ChunkRecordReader
