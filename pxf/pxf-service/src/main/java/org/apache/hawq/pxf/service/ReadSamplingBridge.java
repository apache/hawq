package org.apache.hawq.pxf.service;

import java.io.DataInputStream;
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.AnalyzeUtils;
import org.apache.hawq.pxf.service.utilities.ProtocolData;

/**
 * ReadSamplingBridge wraps a ReadBridge, and returns only some of the output
 * records, based on a ratio sample. The sample to pass or discard a record is
 * done after all of the processing is completed (
 * {@code accessor -> resolver -> output builder}) to make sure there are no
 * chunks of data instead of single records. <br>
 * The goal is to get as uniform as possible sampling. This is achieved by
 * creating a bit map matching the precision of the sampleRatio, so that for a
 * ratio of 0.034, a bit-map of 1000 bits will be created, and 34 bits will be
 * set. This map is matched against each read record, discarding ones with a 0
 * bit and continuing until a 1 bit record is read.
 */
public class ReadSamplingBridge implements Bridge {

    ReadBridge bridge;

    float sampleRatio;
    BitSet sampleBitSet;
    int bitSetSize;
    int sampleSize;
    int curIndex;

    static private Log Log = LogFactory.getLog(ReadSamplingBridge.class);;

    /**
     * C'tor - set the implementation of the bridge.
     *
     * @param protData input containing sampling ratio
     * @throws Exception if the sampling ratio is wrong
     */
    public ReadSamplingBridge(ProtocolData protData) throws Exception {
        bridge = new ReadBridge(protData);

        this.sampleRatio = protData.getStatsSampleRatio();
        if (sampleRatio < 0.0001 || sampleRatio > 1.0) {
            throw new IllegalArgumentException(
                    "sampling ratio must be a value between 0.0001 and 1.0. "
                            + "(value = " + sampleRatio + ")");
        }

        calculateBitSetSize();

        this.sampleBitSet = AnalyzeUtils.generateSamplingBitSet(bitSetSize,
                sampleSize);
        this.curIndex = 0;
    }

    private void calculateBitSetSize() {

        sampleSize = (int) (sampleRatio * 10000);
        bitSetSize = 10000;

        while ((bitSetSize > 100) && (sampleSize % 10 == 0)) {
            bitSetSize /= 10;
            sampleSize /= 10;
        }
        Log.debug("bit set size = " + bitSetSize + " sample size = "
                + sampleSize);
    }

    /**
     * Fetches next sample, according to the sampling ratio.
     */
    @Override
    public Writable getNext() throws Exception {
        Writable output = bridge.getNext();

        // sample - if bit is false, advance to the next object
        while (!sampleBitSet.get(curIndex)) {

            if (output == null) {
                break;
            }
            incIndex();
            output = bridge.getNext();
        }

        incIndex();
        return output;
    }

    private void incIndex() {
        curIndex = (++curIndex) % bitSetSize;
    }

    @Override
    public boolean beginIteration() throws Exception {
        return bridge.beginIteration();
    }

    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {
        return bridge.setNext(inputStream);
    }

    @Override
    public boolean isThreadSafe() {
        return bridge.isThreadSafe();
    }
}
