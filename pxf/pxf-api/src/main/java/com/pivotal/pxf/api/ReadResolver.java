package com.pivotal.pxf.api;

import java.util.List;

/**
 * Interface that defines the deserialization of one record brought from the {@link ReadAccessor}.
 * All deserialization methods (e.g, Writable, Avro, ...) implement this interface.
 */
public interface ReadResolver {
    /**
     * Gets the {@link OneField} list of one row.
     *
     * @param row the row to get the fields from
     * @return the {@link OneField} list of one row.
     * @throws Exception
     */
    List<OneField> getFields(OneRow row) throws Exception;
}
