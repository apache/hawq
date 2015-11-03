package org.apache.hawq.pxf.api.utilities;


/**
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, Analyzer, ...).
 * Manages the meta data.
 */
public class Plugin {
    protected InputData inputData;

    /**
     * Constructs a plugin.
     *
     * @param input the input data
     */
    public Plugin(InputData input) {
        this.inputData = input;
    }

    /**
     * Checks if the plugin is thread safe or not, based on inputData.
     *
     * @return true if plugin is thread safe
     */
    public boolean isThreadSafe() {
        return true;
    }
}
