package org.apache.hawq.pxf.api;

import java.util.Arrays;

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
     * Profile name, recommended for reading given Fragment.
     */
    private String profile;

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

    public Fragment(String sourceName,
            String[] hosts, 
            byte[] metadata,
            byte[] userData,
            String profile) {
        this(sourceName, hosts, metadata, userData);
        this.profile = profile;
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

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + index;
		result = prime * result + Arrays.hashCode(metadata);
		result = prime * result + ((profile == null) ? 0 : profile.hashCode());
		result = prime * result + Arrays.hashCode(replicas);
		result = prime * result + ((sourceName == null) ? 0 : sourceName.hashCode());
		result = prime * result + Arrays.hashCode(userData);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Fragment other = (Fragment) obj;
		if (index != other.index)
			return false;
		if (!Arrays.equals(metadata, other.metadata))
			return false;
		if (profile == null) {
			if (other.profile != null)
				return false;
		} else if (!profile.equals(other.profile))
			return false;
		if (!Arrays.equals(replicas, other.replicas))
			return false;
		if (sourceName == null) {
			if (other.sourceName != null)
				return false;
		} else if (!sourceName.equals(other.sourceName))
			return false;
		if (!Arrays.equals(userData, other.userData))
			return false;
		return true;
	}

}
