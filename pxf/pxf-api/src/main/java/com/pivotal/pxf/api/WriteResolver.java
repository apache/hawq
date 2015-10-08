package com.pivotal.pxf.api;

import java.util.List;

/**
 * Interface that defines the serialization of data read from the DB
 * into a OneRow object.
 * This interface is implemented by all serialization methods (e.g, Writable, Avro, ...).
 */
public interface WriteResolver {
    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws Exception if constructing a row from the fields failed
     */
    OneRow setFields(List<OneField> record) throws Exception;
}
