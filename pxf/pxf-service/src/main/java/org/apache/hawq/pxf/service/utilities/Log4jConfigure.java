package org.apache.hawq.pxf.service.utilities;

import java.io.File;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

public class Log4jConfigure {

    private static Log LOG = LogFactory.getLog(Log4jConfigure.class);
	
    /**
     * Initializes log4j logging for the webapp.
     * 
     * Reads log4j properties file location from log4jConfigLocation parameter in web.xml.
     * When not using aboslute path, the path starts from the webapp root directory.
	 * If the file can't be read, reverts to default configuration file under
	 * WEB-INF/classes/pxf-log4j.properties.
	 * 
     * @param event Servlet context, used to determine webapp root directory.
     */
	public static void configure(ServletContextEvent event) {
		
		final String defaultLog4jLocation = "WEB-INF/classes/pxf-log4j.properties";

		ServletContext context = event.getServletContext();
        String log4jConfigLocation = context.getInitParameter("log4jConfigLocation");

        if (!log4jConfigLocation.startsWith(File.separator)) {
        	log4jConfigLocation = context.getRealPath("") + File.separator + log4jConfigLocation;
        }
        
        // revert to default properties file if file doesn't exist
        File log4jConfigFile = new File (log4jConfigLocation);
        if (!log4jConfigFile.canRead()) {
        	log4jConfigLocation = context.getRealPath("") + File.separator + defaultLog4jLocation;
        }
		PropertyConfigurator.configure(log4jConfigLocation); 
        LOG.info("log4jConfigLocation = " + log4jConfigLocation);
	}
}
