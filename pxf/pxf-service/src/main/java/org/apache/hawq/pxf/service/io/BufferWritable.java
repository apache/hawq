package org.apache.hawq.pxf.service.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.UnsupportedOperationException;

/**
 * A serializable object for transporting a byte array through the Bridge
 * framework
 */
public class BufferWritable implements Writable {

    byte[] buf = null;

    /**
     * Constructs a BufferWritable. Copies the buffer reference and not the
     * actual bytes. This class is used when we intend to transport a buffer
     * through the Bridge framework without copying the data each time the
     * buffer is passed between the Bridge objects.
     *
     * @param inBuf buffer
     */
    public BufferWritable(byte[] inBuf) {
        buf = inBuf;
    }

    /**
     * Serializes the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOutput</code> to serialize this object into.
     * @throws IOException if the buffer was not set
     */
    @Override
    public void write(DataOutput out) throws IOException {
        if (buf == null)
            throw new IOException("BufferWritable was not set");
        out.write(buf);
    }

    /**
     * Deserializes the fields of this object from <code>in</code>.
     * <p>
     * For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.
     * </p>
     *
     * @param in <code>DataInput</code> to deserialize this object from
     * @throws UnsupportedOperationException this function is not supported
     */
    @Override
    public void readFields(DataInput in) {
        throw new UnsupportedOperationException(
                "BufferWritable.readFields() is not implemented");
    }

    /**
     * Appends given app's buffer to existing buffer.
     * <br>
     * Not efficient - requires copying both this and the appended buffer.
     *
     * @param app buffer to append
     */
    public void append(byte[] app) {
        if (buf == null) {
            buf = app;
            return;
        }
        if (app == null) {
            return;
        }

        byte[] newbuf = new byte[buf.length + app.length];
        System.arraycopy(buf, 0, newbuf, 0, buf.length);
        System.arraycopy(app, 0, newbuf, buf.length, app.length);
        buf = newbuf;
    }
}
