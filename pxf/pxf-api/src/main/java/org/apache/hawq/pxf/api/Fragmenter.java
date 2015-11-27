package org.apache.hawq.pxf.api;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.util.LinkedList;
import java.util.List;

/**
 * Abstract class that defines the splitting of a data resource into fragments
 * that can be processed in parallel.
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
     * Gets the fragments of a given path (source name and location of each
     * fragment). Used to get fragments of data that could be read in parallel
     * from the different segments.
     *
     * @return list of data fragments
     * @throws Exception if fragment list could not be retrieved
     */
    public abstract List<Fragment> getFragments() throws Exception;

    /**
     * Default implementation of statistics for fragments. The default is:
     * <ul>
     * <li>number of fragments - as gathered by {@link #getFragments()}</li>
     * <li>first fragment size - 64MB</li>
     * <li>total size - number of fragments times first fragment size</li>
     * </ul>
     * Each fragmenter implementation can override this method to better match
     * its fragments stats.
     *
     * @return default statistics
     * @throws Exception if statistics cannot be gathered
     */
    public FragmentsStats getFragmentsStats() throws Exception {
        List<Fragment> fragments = getFragments();
        long fragmentsNumber = fragments.size();
        return new FragmentsStats(fragmentsNumber,
                FragmentsStats.DEFAULT_FRAGMENT_SIZE, fragmentsNumber
                        * FragmentsStats.DEFAULT_FRAGMENT_SIZE);
    }
}
