package com.pivotal.pxf.api.utilities;


/*
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, Analyzer, ...)
 * Manages the meta data
 */
public class Plugin {
    protected InputData inputData;

    /*
     * C'tor
     */
    public Plugin(InputData input) {
        this.inputData = input;
    }

    /**
     * Checks if the plugin is thread safe or not, based on inputData.
     *
     * @return if plugin is thread safe or not
     */
    public boolean isThreadSafe() {
        return true;
    }
}
