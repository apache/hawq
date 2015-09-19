package com.pivotal.pxf.service;

import com.pivotal.pxf.api.MetadataFetcher;

/**
 * Factory class for creation of {@link MetadataFetcher} objects. 
 * The actual {@link MetadataFetcher} object is "hidden" behind an {@link MetadataFetcher} 
 * abstract class which is returned by the MetadataFetcherFactory. 
 */
public class MetadataFetcherFactory {
    static public MetadataFetcher create(String fetcherName) throws Exception {
        return (MetadataFetcher) Class.forName(fetcherName).newInstance();
    }
}
