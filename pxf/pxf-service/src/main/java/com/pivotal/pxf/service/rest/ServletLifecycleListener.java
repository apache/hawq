package com.pivotal.pxf.service.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import com.pivotal.pxf.service.utilities.SecureLogin;

/* 
 * Listener on lifecycle events of our webapp
 */
public class ServletLifecycleListener implements ServletContextListener {

    private static Log LOG = LogFactory.getLog(ServletContextListener.class);

	/*
	 * called after the webapp has been initialized
	 *
	 * initiates a Kerberos login when Hadoop security is on
	 */
	@Override
	public void contextInitialized(ServletContextEvent arg0)
	{
		LOG.info("webapp initialized");
		SecureLogin.login();
	}

	/*
	 * called before the webapp is about to go down
	 */
	@Override
	public void contextDestroyed(ServletContextEvent arg0)
	{
		LOG.info("webapp about to go down");
	}
}
