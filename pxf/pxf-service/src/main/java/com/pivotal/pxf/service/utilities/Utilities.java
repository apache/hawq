package com.pivotal.pxf.service.utilities;

import com.pivotal.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Utilities class exposes helper method for PXF classes
 */
public class Utilities {
    private static final Log LOG = LogFactory.getLog(Utilities.class);
    private static final Path PXF_DIR = getPxfDir();
    private static volatile URLClassLoader pxfClassLoader = getPxfClassLoader();

    private static Path getPxfDir() {
        try {
            return Paths.get(Utilities.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        } catch (Exception e) {
            LOG.error(e);
            return Paths.get("");
        }
    }

    static {
        /**
         * Start PXF directory watcher thread.
         */
        new Thread(new PxfDirWatch()).start();
    }

    /**
     * Creates an object using the class name.
     * The class name can be a class located in the CLASSPATH or in pxf plugins jar directory
     */
    public static Object createAnyInstance(Class confClass, String className, InputData metaData) throws Exception {
        Class<?> cls = Class.forName(className, true, pxfClassLoader);
        Constructor con = cls.getConstructor(confClass);
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

    private static URLClassLoader getPxfClassLoader() {
        try (DirectoryStream<Path> pxfJars = Files.newDirectoryStream(PXF_DIR, "*.jar")) {
            List<URL> pxfUrls = new ArrayList<>();
            for (Path pxfJar : pxfJars) {
                pxfUrls.add(pxfJar.toUri().toURL());
            }
            return new URLClassLoader(pxfUrls.toArray(new URL[pxfUrls.size()]), Utilities.class.getClassLoader());
        } catch (IOException e) {
            LOG.error(e);
            return null;
        }
    }

    /**
     * PXF directory watcher thread.
     * The watcher wait endlessly on take(),
     * And awakes when a create,delete or modify event is triggered in PXF_DIR.
     * It then creates a new PxfClassLoader to be used by createAnyInstance's Class.forName().
     */
    private static class PxfDirWatch implements Runnable {
        @Override
        public void run() {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                WatchKey watchKey = PXF_DIR.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
                while (true) {
                    watcher.take();
                    pxfClassLoader = getPxfClassLoader();
                    List<WatchEvent<?>> events = watchKey.pollEvents();
                    if (LOG.isInfoEnabled()) {
                        for (WatchEvent<?> event : events) {
                            if (event.kind() == OVERFLOW) {
                                continue;
                            }
                            LOG.info(event.kind() + " " + PXF_DIR.resolve((Path) event.context()));
                        }
                    }
                    if (!watchKey.reset()) {
                        LOG.error("PXF directory " + PXF_DIR
                                + " isn't accessible, Stopping PXF directory watcher thread");
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.error(e);
            }
        }
    }
}
