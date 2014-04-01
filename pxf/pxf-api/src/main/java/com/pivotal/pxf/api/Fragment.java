package com.pivotal.pxf.api;

/**
 * Fragment holds a data fragment' information.
 * {@link Fragmenter#getFragments} returns a list of fragments.
 */
public class Fragment {
    /**
     * File path+name, table name, etc.
     */
    private String sourceName;

    /**
     * Fragment index (incremented per sourceName).
     */
    private int index;

    /**
     * Fragment replicas (1 or more).
     */
    private String[] replicas;

    /**
     * Fragment metadata information (starting point + length, region location, etc.).
     */
    private byte[] metadata;

    /**
     * ThirdParty data added to a fragment. Ignored if null.
     */
    private byte[] userData;

    /**
     * Constructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param hosts the replicas
     * @param metadata the meta data (Starting point + length, region location, etc.).
     */
    public Fragment(String sourceName,
                    String[] hosts,
                    byte[] metadata) {
        this.sourceName = sourceName;
        this.replicas = hosts;
        this.metadata = metadata;
    }

    /**
     * Constructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param hosts the replicas
     * @param metadata the meta data (Starting point + length, region location, etc.).
     * @param userData third party data added to a fragment.
     */
    public Fragment(String sourceName,
                    String[] hosts,
                    byte[] metadata,
                    byte[] userData) {
        this.sourceName = sourceName;
        this.replicas = hosts;
        this.metadata = metadata;
        this.userData = userData;
    }

    public String getSourceName() {
        return sourceName;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String[] getReplicas() {
        return replicas;
    }

    public void setReplicas(String[] replicas) {
        this.replicas = replicas;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public void setMetadata(byte[] metadata) {
        this.metadata = metadata;
    }

    public byte[] getUserData() {
        return userData;
    }

    public void setUserData(byte[] userData) {
        this.userData = userData;
    }
}
