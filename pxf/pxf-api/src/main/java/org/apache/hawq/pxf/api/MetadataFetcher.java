package org.apache.hawq.pxf.api;

/**
 * Abstract class that defines getting metadata of a table.
 */
public abstract class MetadataFetcher {
    protected Metadata metadata;

    /**
     * Constructs a MetadataFetcher.
     *
     */
    public MetadataFetcher() {

    }

    /**
     * Gets a metadata of a given table
     *
     * @param tableName table name
     * @return metadata of given table
     * @throws Exception if metadata information could not be retrieved
     */
    public abstract Metadata getTableMetadata(String tableName) throws Exception;
}
