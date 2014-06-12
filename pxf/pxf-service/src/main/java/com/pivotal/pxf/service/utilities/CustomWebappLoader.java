package com.pivotal.pxf.service.utilities;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.loader.WebappLoader;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;

/**
 * A WebappLoader that allows a customized classpath to be added through configuration in context xml.
 * Any additional classpath entry will be added to the default webapp classpath.
 * <p/>
 * <pre>
 * &lt;Context>
 *   &lt;Loader className="com.pivotal.pxf.service.utilities.CustomWebappLoader"
 *              classpathFiles="/somedir/classpathFile1;/somedir/classpathFile2"/>
 * &lt;/Context>
 * </pre>
 */
public class CustomWebappLoader extends WebappLoader {
	private static final Log LOG = LogFactory.getLog(CustomWebappLoader.class);

	/**
	 * Classpath files containing path entries, separated by new line.
	 * e.g:
	 * somedir
	 * anotherdir/somejar.jar
	 * anotherone/hadoop*.jar
	 * anotherone/pxf*[0-9].jar
	 * Unix wildcard convention can be used to match a number of files 
	 * (e.g. <code>*</code>, <code>[0-9]</code>). 
	 * These files must exist - if they can't be read an exception will be thrown.
	 */
	private String classpathFiles;
	/**
	 * Secondary classpath files - if these files are unavailable only a warning will be logged.
	 */
	private String secondaryClasspathFiles;

	/**
	 * Constructs a WebappLoader with no defined parent class loader (actual parent will be the system class loader).
	 */
	public CustomWebappLoader() {
		super();
	}

	/**
	 * Constructs a WebappLoader with the specified class loader to be defined as the parent for this ClassLoader.
	 *
	 * @param parent The parent class loader
	 */
	public CustomWebappLoader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * <code>classpathFiles</code> attribute is automatically set from the context xml file.
	 *
	 * @param classpathFiles Files separated by <code>;</code> Which contains <code>;</code> separated list of path entries.
	 */
	public void setClasspathFiles(String classpathFiles) {
		this.classpathFiles = classpathFiles;
	}

	/**
	 * <code>secondaryClasspathFiles</code> attribute is automatically set from the context xml file.
	 *
	 * @param secondaryClasspathFiles Files separated by <code>;</code> Which contains <code>;</code> separated list of path entries.
	 */
	public void setSecondaryClasspathFiles(String secondaryClasspathFiles) {
		this.secondaryClasspathFiles = secondaryClasspathFiles;
	}

	/**
	 * Implements {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
	 *
	 * @throws LifecycleException if this component detects a fatal error that prevents this component from being used.
	 */
	@Override
	protected void startInternal() throws LifecycleException {

		addRepositories(classpathFiles, true);
		addRepositories(secondaryClasspathFiles, false);
		
		super.startInternal();
	}

	private void addRepositories(String classpathFiles, boolean throwException) throws LifecycleException {
		
		for (String classpathFile : classpathFiles.split(";")) {
			
			String classpath = readClasspathFile(classpathFile, throwException);
			if (classpath == null) {
				continue;
			}
			
			ArrayList<String> classpathEntries = trimEntries(classpath.split("[\\r\\n]+"));
			LOG.info("Classpath file " + classpathFile + " has " + classpathEntries.size() + " entries");
			
			for (String entry : classpathEntries) {
				LOG.debug("Trying to load entry " + entry);
				int repositoriesCount = 0;
				// TODO: change to general globbing, not just '*'.
				if (entry.contains("*")) {
					try (DirectoryStream<Path> repositories = newDirectoryStream(Paths.get(entry).getParent(),
							Paths.get(entry).getFileName().toString())) { 
						for (Path repository : repositories) {
							if (addPathToRepository(repository, entry)) {
								repositoriesCount++;
							}
						}
					} catch (IOException e) {
						LOG.warn("Failed to load entry " + entry + ": " + e);
					} 
					if (repositoriesCount == 0) {
						LOG.warn("Entry " + entry + " doesn't match any files");
					}
				} else {
					if (addPathToRepository(Paths.get(entry), entry)) {
						repositoriesCount++;
					}
				}
				LOG.debug("Loaded " + repositoriesCount + " repositories from entry " + entry);
			}
		}
	}

	private String readClasspathFile(String classpathFile, boolean throwException) throws LifecycleException {
		String classpath = null;
		try {
			LOG.info("Trying to read classpath file " + classpathFile);
			classpath = new String(readAllBytes(get(classpathFile)));
		} catch (IOException ioe) {
			LOG.warn("Failed to read classpath file: " + ioe);
			if (throwException) {
				throw new LifecycleException("Failed to read classpath file: " + ioe, ioe);
			}
		}
		return classpath;
	}

	/**
	 * Returns a list of valid classpath entries, excluding null, empty and comment lines.
	 * @param classpathEntries original entries
	 * @return valid entries
	 */
	private ArrayList<String> trimEntries(String[] classpathEntries) {
		
		ArrayList<String> trimmed = new ArrayList<String>();
		int line = 0;
		for (String entry : classpathEntries) {
			
			line++;
			if (entry == null) {
				LOG.debug("Skipping entry #" + line + " (null)");
				continue;
			}
			
			entry = entry.trim();
			if (entry.isEmpty() || entry.startsWith("#")) {
				LOG.debug("Skipping entry #" + line + " (" + entry + ")");
				continue;
			}
			trimmed.add(entry);
		}
		return trimmed;
	}
	
	private boolean addPathToRepository(Path path, String entry) {
		boolean status = false;
		
		try {
			URI pathUri = path.toUri();
			String pathUriStr = pathUri.toString();
			File file = new File(pathUri);
			if (!file.canRead()) {
				throw new FileNotFoundException(pathUriStr + " cannot be read");
			}
			addRepository(pathUriStr);
			LOG.debug("Repository " + pathUriStr + " added from entry " + entry);
			status = true;
		} catch (Exception e) {
			LOG.warn("Failed to load path " + path + " to repository: " + e);
		}
		
		return status;
	}
	
}


