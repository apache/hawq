package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;
import java.io.DataOutputStream;
import java.lang.ClassNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import org.apache.hadoop.io.Writable;

/*
 * Utilites class exposes helper method for GPFUSION classes
 */
public class Utilities
{
	static private String packageScope = "com.emc.greenplum.gpdb.hdfsconnector.";
	
	/*
	 * creates an object using the class name. The class name can be a class located in the gphdfs
	 * package in which case the scope of the package name applies, or it can be a third-party
	 * class, located in $GPHOME/lib/hadoop/schemafiles, in which case the package name does not
	 * apply
	 */
	public static Object createAnyInstance(Class confClass,String className, BaseMetaData metaData) throws Exception
	{
		Object instance = null;
		
		try 
		{
			instance = createInstance(confClass, packageScope + className, metaData);
		}
		catch (ClassNotFoundException e)
		{
			instance = createInstance(confClass, className, metaData);
		}
		
		return instance;
	}
	
	/*
	 * creates an object using the class name, using on reflection
	 */
	static Object createInstance(Class confClass, String className, BaseMetaData metaData) throws Exception
	{
		try 
		{
			Class<?> cls = Class.forName(className);
			Constructor con = cls.getConstructor(new Class[]{confClass});
			return  con.newInstance(new Object[]{metaData});
		}
		catch (ClassNotFoundException e)
		{			
			throw new ClassNotFoundException("Class " + className + " could not be found on the CLASSPATH. " + e.getMessage());
		}
		catch (InvocationTargetException e)
		{
			/*
			 * We are creating resolvers, accessors and fragmenters using the reflection framework. If for example, a resolver, during its
			 * instantiation - in the c'tor, will throw an exception, the Resolver's exception will reach the Reflection
			 * layer and there it will be wrapped inside an InvocationTargetException. Here we are above the 
			 * Reflection layer and we need to unwrap the Resolver's initial exception and throw it instead of the
			 * wrapper InvocationTargetException so that our initial Exception text will be displayed
			 * in psql instead of the message: "Internal Server Error"
			 */
			if (e.getTargetException() != null)
				throw (Exception)(e.getTargetException()); /* getTargetException() returns a Throwable */
			throw e;
		}
	}
}
