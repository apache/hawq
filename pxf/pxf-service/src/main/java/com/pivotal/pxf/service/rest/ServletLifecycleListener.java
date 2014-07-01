package com.pivotal.pxf.service.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import com.pivotal.pxf.service.utilities.Log4jConfigure;
import com.pivotal.pxf.service.utilities.SecureLogin;

/** 
 * Listener on lifecycle events of our webapp
 */
public class ServletLifecycleListener implements ServletContextListener {

    private static Log LOG = LogFactory.getLog(ServletContextListener.class);

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
