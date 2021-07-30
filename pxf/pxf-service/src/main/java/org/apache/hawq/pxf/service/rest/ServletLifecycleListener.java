package org.apache.hawq.pxf.service.rest;

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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import org.apache.hawq.pxf.service.utilities.Log4jConfigure;
import org.apache.hawq.pxf.service.utilities.SecureLogin;

/**
 * Listener on lifecycle events of our webapp
 */
public class ServletLifecycleListener implements ServletContextListener {

    private static final Log LOG = LogFactory.getLog(ServletContextListener.class);

	/**
	 * Called after the webapp has been initialized.
	 *
	 * 1. Initializes log4j.
	 * 2. Initiates a Kerberos login when Hadoop security is on.
	 */
	@Override
	public void contextInitialized(ServletContextEvent event) {
		// 1. Initialize log4j:
		Log4jConfigure.configure(event);

		LOG.info("webapp initialized");

		// 2. Initiate secure login
		SecureLogin.login();
	}

	/**
	 * Called before the webapp is about to go down
	 */
	@Override
	public void contextDestroyed(ServletContextEvent event) {
		LOG.info("webapp about to go down");
	}
}
