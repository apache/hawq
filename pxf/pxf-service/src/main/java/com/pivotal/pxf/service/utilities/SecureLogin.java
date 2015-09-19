package com.pivotal.pxf.service.utilities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

/*
 * This class relies heavily on Hadoop API to
 * - Check need for secure login in Hadoop
 * - Parse and load .xml configuration file
 * - Do a Kerberos login with a kaytab file
 * - convert _HOST in Kerberos principal to current hostname
 *
 * It uses Hadoop Configuration to parse XML configuration files
 * It uses Hadoop Security to modify principal and perform the login
 *
 * The major limitation in this class is its dependency
 * on Hadoop. If Hadoop security is off, no login will be performed
 * regardless of connector being used.
 */
public class SecureLogin {
    private static Log LOG = LogFactory.getLog(SecureLogin.class);
	private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
	private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";

	public static void login() {
		try {
			Configuration config = new Configuration();
			config.addResource("pxf-site.xml");

			SecurityUtil.login(config, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);
		} catch (Exception e)
		{
			LOG.error("PXF service login failed");
			throw new RuntimeException(e);
		}
	}
}
