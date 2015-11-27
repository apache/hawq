package com.pivotal.hawq.mapreduce;

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


@SuppressWarnings("serial")
public class HAWQException extends Exception {

    private int etype;

    public static final int DEFAULT_EXCEPTION = 0;
    public static final int WRONGFILEFORMAT_EXCEPTION = 1;

    public HAWQException() {
    }

	/**
	 * Constructor
	 * 
	 * @param emsg
	 *            error message
	 */
    public HAWQException(String emsg) {
        super(emsg);
    }

	/**
	 * Constructor
	 * 
	 * @param emsg
	 *            error message
	 * @param etype
	 *            0 is default type, and 1 means wrong file format exception
	 */
    public HAWQException(String emsg, int etype) {
        super(emsg);
        this.etype = etype;
    }

    @Override
    public String getMessage() {
        StringBuffer buffer = new StringBuffer();
        switch (etype) {
            case WRONGFILEFORMAT_EXCEPTION:
                buffer.append("Wrong File Format: ");
                buffer.append(super.getMessage());
                return buffer.toString();
            default:
                buffer.append(super.getMessage());
                return buffer.toString();
        }
    }
}
