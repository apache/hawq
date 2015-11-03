package org.apache.hawq.pxf.service.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.Arrays;

/**
 * This class stores text using standard UTF8 encoding. It provides methods to
 * serialize, deserialize. The type of length is integer and is serialized using
 * zero-compressed format.
 */
public class Text implements Writable {

    // for write
    private byte[] buf;
    private static final Log LOG = LogFactory.getLog(Text.class);
    int curLoc;
    private static final char LINE_DELIMITER = '\n';
    private static final int BUF_SIZE = 1024;
    private static final int EOF = -1;

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            return Charset.forName("UTF-8").newEncoder().onMalformedInput(
                    CodingErrorAction.REPORT).onUnmappableCharacter(
                    CodingErrorAction.REPORT);
        }
    };
    private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charset.forName("UTF-8").newDecoder().onMalformedInput(
                    CodingErrorAction.REPORT).onUnmappableCharacter(
                    CodingErrorAction.REPORT);
        }
    };
    private byte[] bytes;
    private int length;

    public Text() {
        bytes = EMPTY_BYTES;
        buf = new byte[BUF_SIZE];
    }

    /**
     * Construct from a string.
     *
     * @param string input string
     */
    public Text(String string) {
        set(string);
    }

    /**
     * Construct from another text.
     *
     * @param utf8 text to copy
     */
    public Text(Text utf8) {
        set(utf8);
    }

    /**
     * Construct from a byte array.
     *
     * @param utf8 input byte array
     */
    public Text(byte[] utf8) {
        set(utf8);
    }

    public static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    public static long readVLong(DataInput stream) throws IOException {
        byte firstByte = stream.readByte();
        int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
            byte b = stream.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

    public static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    public static String decode(byte[] utf8, int start, int length)
            throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length), true);
    }

    /**
     * Converts the provided byte array to a String using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     *
     * @param utf8 UTF-8 encoded byte array
     * @param start start point
     * @param length length of array
     * @param replace whether to replace malformed input with substitution
     *            character
     * @return decoded string
     * @throws MalformedInputException if a malformed input is used
     * @throws CharacterCodingException if the conversion failed
     */
    public static String decode(byte[] utf8, int start, int length,
                                boolean replace)
            throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length), replace);
    }

    private static String decode(ByteBuffer utf8, boolean replace)
            throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if (replace) {
            decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(utf8).toString();
        // set decoder back to its default value: REPORT
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If the
     * input is malformed, invalid chars are replaced by a default value.
     *
     * @param string string to encode
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     *         ByteBuffer.limit()
     * @throws CharacterCodingException if conversion failed
     */
    public static ByteBuffer encode(String string)
            throws CharacterCodingException {
        return encode(string, true);
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     *
     * @param string string to encode
     * @param replace whether to replace malformed input with substitution character
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     *         ByteBuffer.limit()
     * @throws MalformedInputException if a malformed input is used
     * @throws CharacterCodingException if the conversion failed
     */
    public static ByteBuffer encode(String string, boolean replace)
            throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes = encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }

    /**
     * Returns the raw bytes; however, only data up to {@link #getLength()} is
     * valid.
     *
     * @return raw bytes of byte array
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Returns the number of bytes in the byte array
     *
     * @return number of bytes in byte array
     */
    public int getLength() {
        return length;
    }

    /**
     * Sets to contain the contents of a string.
     *
     * @param string input string
     */
    public void set(String string) {
        try {
            ByteBuffer bb = encode(string, true);
            bytes = bb.array();
            length = bb.limit();
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Should not have happened "
                    + e.toString());
        }
    }

    /**
     * Sets to a UTF-8 byte array.
     *
     * @param utf8 input UTF-8 byte array
     */
    public void set(byte[] utf8) {
        set(utf8, 0, utf8.length);
    }

    /**
     * Copies a text.
     *
     * @param other text object to copy.
     */
    public void set(Text other) {
        set(other.getBytes(), 0, other.getLength());
    }

    /**
     * Sets the Text to range of bytes.
     *
     * @param utf8 the data to copy from
     * @param start the first position of the new string
     * @param len the number of bytes of the new string
     */
    public void set(byte[] utf8, int start, int len) {
        setCapacity(len, false);
        System.arraycopy(utf8, start, bytes, 0, len);
        this.length = len;
    }

    /**
     * Appends a range of bytes to the end of the given text.
     *
     * @param utf8 the data to copy from
     * @param start the first position to append from utf8
     * @param len the number of bytes to append
     */
    public void append(byte[] utf8, int start, int len) {
        setCapacity(length + len, true);
        System.arraycopy(utf8, start, bytes, length, len);
        length += len;
    }

    /**
     * Clears the string to empty.
     */
    public void clear() {
        length = 0;
    }

    /*
     * Sets the capacity of this Text object to <em>at least</em>
     * <code>len</code> bytes. If the current buffer is longer, then the
     * capacity and existing content of the buffer are unchanged. If
     * <code>len</code> is larger than the current capacity, the Text object's
     * capacity is increased to match.
     *
     * @param len the number of bytes we need
     *
     * @param keepData should the old data be kept
     */
    private void setCapacity(int len, boolean keepData) {
        if (bytes == null || bytes.length < len) {
            byte[] newBytes = new byte[len];
            if (bytes != null && keepData) {
                System.arraycopy(bytes, 0, newBytes, 0, length);
            }
            bytes = newBytes;
        }
    }

    /**
     * Convert text back to string
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        try {
            return decode(bytes, 0, length);
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Should not have happened "
                    + e.toString());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.write(bytes, 0, getLength());
    }

    /**
     * deserialize
     */
    @Override
    public void readFields(DataInput inputStream) throws IOException {

        byte c;
        curLoc = 0;
        clear();
        while ((c = (byte) ((DataInputStream) inputStream).read()) != EOF) {
            buf[curLoc] = c;
            curLoc++;

            if (c == LINE_DELIMITER) {
                LOG.trace("read one line, size " + curLoc);
                break;
            }

            if (isBufferFull()) {
                flushBuffer();
            }
        }

        if (!isBufferEmpty()) {
            // the buffer doesn't end with a line break.
            if (c == EOF) {
                LOG.warn("Stream ended without line break");
            }
            flushBuffer();
        }
    }

    private boolean isBufferEmpty() {
        return (curLoc == 0);
    }

    private boolean isBufferFull() {
        return (curLoc == BUF_SIZE);
    }

    private void flushBuffer() {
        append(buf, 0, curLoc);
        curLoc = 0;
    }

    /**
     * Returns true iff <code>o</code> is a Text with the same contents.
     */
    @Override
    public boolean equals(Object o) {
        return (o instanceof Text && Arrays.equals(bytes, ((Text) o).bytes));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}