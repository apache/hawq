package com.pivotal.pxf.api;

import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

import java.util.LinkedList;
import java.util.List;

/**
 * Abstract class that defines the splitting of a data resource into fragments that can be processed in parallel.
 */
public abstract class Fragmenter extends Plugin {
    protected List<Fragment> fragments;

    /**
     * Constructs a Fragmenter.
     *
     * @param metaData the input data
     */
    public Fragmenter(InputData metaData) {
        super(metaData);
        fragments = new LinkedList<>();
    }

    /**
     * Gets the fragments of a given path (source name and location of each fragment).
     * Used to get fragments of data that could be read in parallel from the different segments.
     *
     * @return list of data fragments
     * @throws Exception
     */
    public abstract List<Fragment> getFragments() throws Exception;
}
