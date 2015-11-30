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


/**
 * Thrown when Accessor/Resolver failes to parse {@link org.apache.hawq.pxf.api.utilities.InputData#userData}.
 */
public class UserDataException extends Exception {

    /**
     * Constructs an UserDataException
     *
     * @param cause the cause of this exception
     */
    public UserDataException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an UserDataException
     *
     * @param message the cause of this exception
     */
    public UserDataException(String message) {
        super(message);
    }
}
