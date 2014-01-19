package com.pivotal.pxf.api;

/*
 * Fragment holds a data fragment' information.
 * Fragmenter.getFragments() returns a list of fragments.
 */
public class Fragment {
    private String sourceName;    // File path+name, table name, etc.
    private int index;        // Fragment index (incremented per sourceName)
    private String[] replicas;    // Fragment replicas (1 or more)
    private byte[] metadata;    // Fragment metadata information (starting point + length, region location, etc.)

    private byte[] userData;    // ThirdParty data added to a fragment. Ignored if null

    public Fragment(String sourceName,
                    String[] hosts,
                    byte[] metadata) {
        this.sourceName = sourceName;
        this.replicas = hosts;
        this.metadata = metadata;
    }

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
