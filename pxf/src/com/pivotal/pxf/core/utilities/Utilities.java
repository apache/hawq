package com.pivotal.pxf.core.utilities;

import com.pivotal.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;

/*
 * Utilities class exposes helper method for PXF classes
 */
public class Utilities {
    private static final Log LOG = LogFactory.getLog(Utilities.class);

    /*
     * Creates an object using the class name.
     * The class name can be a class located in the CLASSPATH or in pxf plugins jar directory
     */
    public static Object createAnyInstance(Class confClass, String className, InputData metaData) throws Exception {
        Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            LOG.debug(className + " not found on the CLASSPATH. Trying dynamic loading");
            cls = loadClassFromJars(className);
        } catch (NoClassDefFoundError e) {
            LOG.debug(className + " dependencies not found on the CLASSPATH. Trying dynamic loading");
            cls = loadClassFromJars(className);
        }
        Constructor con = cls.getConstructor(confClass);
        try {
            return con.newInstance(metaData);
        } catch (InvocationTargetException e) {
            /*
             * We are creating resolvers, accessors and fragmenters using the reflection framework. If for example, a resolver, during its
			 * instantiation - in the c'tor, will throw an exception, the Resolver's exception will reach the Reflection
			 * layer and there it will be wrapped inside an InvocationTargetException. Here we are above the
			 * Reflection layer and we need to unwrap the Resolver's initial exception and throw it instead of the
			 * wrapper InvocationTargetException so that our initial Exception text will be displayed
			 * in psql instead of the message: "Internal Server Error"
			 */
            throw (e.getCause() != null)
                    ? new Exception(e.getCause()) // getCause() returns a Throwable
                    : e;
        }
    }

    private static Class<?> loadClassFromJars(String className) throws Exception {
        File jarsPath = new File(Utilities.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .getParentFile();
        File[] jars = jarsPath.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && pathname.toString().toLowerCase().endsWith(".jar");
            }
        });
        URL[] jarUrls = new URL[jars.length];
        for (int i = 0; i < jars.length; i++) {
            jarUrls[i] = jars[i].toURI().toURL();
        }
        return new URLClassLoader(jarUrls).loadClass(className);
    }
}
