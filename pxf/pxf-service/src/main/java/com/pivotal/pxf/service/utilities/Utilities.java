package com.pivotal.pxf.service.utilities;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.utilities.InputData;

/**
 * Utilities class exposes helper method for PXF classes
 */
public class Utilities {
    private static final Log LOG = LogFactory.getLog(Utilities.class);
     
    /**
     * Creates an object using the class name.
     * The class name has to be a class located in the webapp's CLASSPATH.
     * 
     * @param confClass the class of the metaData used to initialize the instance
     * @param className a class name to be initialized.
     * @param metaData input data used to initialize the class
     * @return Initialized instance of given className
     * @throws Exception
     */
    public static Object createAnyInstance(Class<?> confClass, String className, InputData metaData) throws Exception {
        Class<?> cls = Class.forName(className);
        Constructor<?> con = cls.getConstructor(confClass);
        try {
            return con.newInstance(metaData);
        } catch (InvocationTargetException e) {
			/*
			 * We are creating resolvers, accessors and fragmenters using the
			 * reflection framework. If for example, a resolver, during its
			 * instantiation - in the c'tor, will throw an exception, the
			 * Resolver's exception will reach the Reflection layer and there it
			 * will be wrapped inside an InvocationTargetException. Here we are
			 * above the Reflection layer and we need to unwrap the Resolver's
			 * initial exception and throw it instead of the wrapper
			 * InvocationTargetException so that our initial Exception text will
			 * be displayed in psql instead of the message:
			 * "Internal Server Error"
			 */
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
    }
}
