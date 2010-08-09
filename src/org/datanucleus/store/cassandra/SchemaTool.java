/**********************************************************************

Copyright (c) 2010 Pedro Gomes and Universidade do Minho.(Schema for Cassandra)
(Based on datanucleus-rdbms. Copyright (c) 2003 Andy Jefferson and others. All rights reserved. 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
 
Contributors:
2004 Joerg Van Frantzius - changes to support a form of DDL output
2004 Erik Bengtson - dbinfo() mode
2004 Andy Jefferson - added "mapping" property to allow ORM files
    ...
 **********************************************************************/
package org.datanucleus.store.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManagerFactoryImpl;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.FileMetaData;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.CommandLine;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * SchemaTool providing an interface for the maintenance of schemas. These
 * utilities include:-
 * <ul>
 * <li>creation of tables representing classes specified in input data</li>
 * <li>deletion of tables representing classes specified in input data</li>
 * <li>validation of tables representing classes specified in input data</li>
 * <li>details about the datastore</li>
 * </ul>
 */
public class SchemaTool extends PersistenceConfiguration {
	/** Localiser for messages. */
	private static final Localiser LOCALISER = Localiser.getInstance(
			"org.datanucleus.store.cassandra.Localisation",
			CassandraStoreManager.class.getClassLoader());

	/** ObjectManagerFactory Context **/
	private OMFContext omfContext;

	/** Command Line **/
	private CommandLine cmd;

	/** default command line arguments **/
	private String[] defaultArgs;

	/** Name of a file containing properties. */
	private String propsFileName = null;

	/** create mode **/
	public static final int SCHEMATOOL_CREATE_MODE = 1;

	/** delete mode **/
	public static final int SCHEMATOOL_DELETE_MODE = 2;

	/** validate mode **/
	public static final int SCHEMATOOL_VALIDATE_MODE = 3;

	/** database info mode **/
	public static final int SCHEMATOOL_DATABASE_INFO_MODE = 4;

	/** schema info mode **/
	public static final int SCHEMATOOL_SCHEMA_INFO_MODE = 5;

	/** schema tool mode **/
	public static final String SCHEMATOOL_OPTION_MODE = "datanucleus.schemaTool.mode";

	/** schema tool in verbose **/
	public static final String SCHEMATOOL_OPTION_VERBOSE = "datanucleus.schemaTool.verbose";

	/** properties file for schema tool **/
	public static final String SCHEMATOOL_OPTION_PROPERTIES_FILE = "datanucleus.schemaTool.propertiesFile";

	/** Property specifying the name of a DDL file **/
	public static final String SCHEMATOOL_OPTION_DDLFILE = "datanucleus.schemaTool.ddlFile";

	/** create complete DDL, not only for missing elements **/
	public static final String SCHEMATOOL_OPTION_COMPLETEDDL = "datanucleus.schemaTool.completeDdl";

	/** output help for schema tool **/
	public static final String SCHEMATOOL_OPTION_HELP = "datanucleus.schemaTool.help";

	public static NucleusLogger LOGGER = NucleusLogger
			.getLoggerInstance("DataNucleus.SchemaTool");

	/**
	 * Entry method when invoked from the command line.
	 * 
	 * @param args
	 *            List of options for processing by the available methods in
	 *            this class.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		SchemaTool tool = new SchemaTool();
		tool.setCommandLineArgs(args);

		if (tool.isHelp()) {
			System.out.println(LOCALISER.msg(false, "014023"));
			System.out.println(LOCALISER.msg(false, "014024"));
			System.out.println(LOCALISER.msg(false, "014025"));
			System.out.println(tool.cmd.toString());
			System.out.println(LOCALISER.msg(false, "014034"));
			System.out.println(LOCALISER.msg(false, "014035"));
			System.exit(0);
		}

		// Mode of operation
		int mode = SCHEMATOOL_CREATE_MODE;
		String msg = null;
		if (tool.getModeName() != null) {
			if (tool.getModeName().equals("create")) {
				mode = SCHEMATOOL_CREATE_MODE;
				msg = LOCALISER.msg(false, "014000", ObjectManagerFactoryImpl
						.getVersionNumber());
			} else if (tool.getModeName().equals("delete")) {
				mode = SCHEMATOOL_DELETE_MODE;
				msg = LOCALISER.msg(false, "014001", ObjectManagerFactoryImpl
						.getVersionNumber());
			} else if (tool.getModeName().equals("validate")) {
				mode = SCHEMATOOL_VALIDATE_MODE;
				msg = LOCALISER.msg(false, "014002", ObjectManagerFactoryImpl
						.getVersionNumber());
			} else if (tool.getModeName().equals("dbinfo")) {
				mode = SCHEMATOOL_DATABASE_INFO_MODE;
				msg = LOCALISER.msg(false, "014003", ObjectManagerFactoryImpl
						.getVersionNumber());
			} else if (tool.getModeName().equals("schemainfo")) {
				mode = SCHEMATOOL_SCHEMA_INFO_MODE;
				msg = LOCALISER.msg(false, "014004", ObjectManagerFactoryImpl
						.getVersionNumber());
			}
		} else {
			msg = LOCALISER.msg(false, "014000", ObjectManagerFactoryImpl
					.getVersionNumber());
		}
		LOGGER.info(msg);
		System.out.println(msg);

		// Classpath
		msg = LOCALISER.msg(false, "014005");
		LOGGER.info(msg);
		if (tool.isVerbose()) {
			System.out.println(msg);
		}
		StringTokenizer tokeniser = new StringTokenizer(System
				.getProperty("java.class.path"), File.pathSeparator);
		while (tokeniser.hasMoreTokens()) {
			msg = LOCALISER.msg(false, "014006", tokeniser.nextToken());
			LOGGER.info(msg);
			if (tool.isVerbose()) {
				System.out.println(msg);
			}
		}
		if (tool.isVerbose()) {
			System.out.println();
		}

		// DDL file
		String ddlFilename = tool.getDdlFile();
		if (ddlFilename != null) {
			msg = LOCALISER.msg(false, tool.getCompleteDdl() ? "014018"
					: "014019", ddlFilename);
			LOGGER.info(msg);
			if (tool.isVerbose()) {
				System.out.println(msg);
				System.out.println();
			}
		}

		// Create a PMF for use with this mode
		JDOPersistenceManagerFactory pmf = null;
		try {
			if (tool.getPropsFileName() != null) {
				pmf = getPMFForMode(mode, tool.getApi(), tool
						.getPersistenceProperties(), tool
						.getStringProperty("datanucleus.PersistenceUnitName"),
						ddlFilename, tool.isVerbose());
			} else {
				pmf = getPMFForMode(mode, tool.getApi(), null, tool
						.getStringProperty("datanucleus.PersistenceUnitName"),
						ddlFilename, tool.isVerbose());
			}
		} catch (Exception e) {
			// Unable to create a PMF so likely input errors
			LOGGER.error("Error creating PMF", e);
			System.out.println(LOCALISER.msg(false, "014008", e.getMessage()));
			System.exit(1);
			return;
		}

		List classNames = null;
		if (mode != SCHEMATOOL_SCHEMA_INFO_MODE
				&& mode != SCHEMATOOL_DATABASE_INFO_MODE) {
			// Find the names of the classes to be processed
			// This will load up all MetaData for the specified input and throw
			// exceptions where errors are found
			try {
				MetaDataManager metaDataMgr = ((ObjectManagerFactoryImpl) pmf)
						.getOMFContext().getMetaDataManager();
				ClassLoaderResolver clr = ((ObjectManagerFactoryImpl) pmf)
						.getOMFContext().getClassLoaderResolver(null);

				FileMetaData[] filemds = getFileMetaDataForInput(
						metaDataMgr,
						clr,
						tool.isVerbose(),
						tool
								.getStringProperty("datanucleus.PersistenceUnitName"),
						tool.getDefaultArgs());
				classNames = new ArrayList();
				if (filemds == null) {
					msg = LOCALISER.msg(false, "014021");
					LOGGER.error(msg);
					System.out.println(msg);
					System.exit(2);
					return;
				}
				for (int i = 0; i < filemds.length; i++) {
					for (int j = 0; j < filemds[i].getNoOfPackages(); j++) {
						for (int k = 0; k < filemds[i].getPackage(j)
								.getNoOfClasses(); k++) {
							String className = filemds[i].getPackage(j)
									.getClass(k).getFullClassName();
							if (!classNames.contains(className)) {
								classNames.add(className);
							}
						}
					}
				}
			} catch (Exception e) {
				// Exception will have been logged and sent to System.out in
				// "getFileMetaDataForInput()"
				System.exit(2);
				return;
			}
		}

		// Run SchemaTool
		try {
			if (mode == SCHEMATOOL_CREATE_MODE) {
				tool.createSchema(pmf, classNames);
			} else if (mode == SCHEMATOOL_DELETE_MODE) {
				tool.deleteSchema(pmf, classNames);
			} else if (mode == SCHEMATOOL_VALIDATE_MODE) {
				tool.validateSchema(pmf, classNames);
			} else if (mode == SCHEMATOOL_DATABASE_INFO_MODE) {
				StoreManager srm = ((JDOPersistenceManager) pmf
						.getPersistenceManager()).getObjectManager()
						.getStoreManager();
				srm.printInformation("DATASTORE", System.out);
			} else if (mode == SCHEMATOOL_SCHEMA_INFO_MODE) {
				StoreManager srm = ((JDOPersistenceManager) pmf
						.getPersistenceManager()).getObjectManager()
						.getStoreManager();
				srm.printInformation("SCHEMA", System.out);
			}

			msg = LOCALISER.msg(false, "014043");
			LOGGER.info(msg);
			System.out.println(msg);
		} catch (Exception e) {
			msg = LOCALISER.msg(false, "014037", e.getMessage());
			System.out.println(msg);
			LOGGER.error(msg, e);
			System.exit(2);
			return;
		}
	}

	/**
	 * Constructor
	 */
	public SchemaTool() {
		omfContext = new OMFContext(this);
		cmd = new CommandLine();
		cmd.addOption("create", "create", null, LOCALISER.msg(false, "014026"));
		cmd.addOption("delete", "delete", null, LOCALISER.msg(false, "014027"));
		cmd.addOption("validate", "validate", null, LOCALISER.msg(false,
				"014028"));
		cmd.addOption("dbinfo", "dbinfo", null, LOCALISER.msg(false, "014029"));
		cmd.addOption("schemainfo", "schemainfo", null, LOCALISER.msg(false,
				"014030"));
		cmd.addOption("ddlFile", "ddlFile", "ddlFile", LOCALISER.msg(false,
				"014031"));
		cmd.addOption("completeDdl", "completeDdl", null, LOCALISER.msg(false,
				"014032"));
		cmd.addOption("api", "api", "api", "API Adapter (JDO, JPA, etc)");
		cmd.addOption("help", "help", null, LOCALISER.msg(false, "014033"));
		cmd.addOption("v", "verbose", null, "verbose output");
		cmd.addOption("pu", "persistenceUnit", "<persistence-unit>",
				"name of the persistence unit to handle the schema for");
		cmd.addOption("props", "properties", "props",
				"path to a properties file");
	}

	/**
	 * Method to create a PersistenceManagerFactory for the specified mode of
	 * SchemaTool
	 * 
	 * @param mode
	 *            Mode of operation of SchemaTool
	 * @param api
	 *            Persistence API
	 * @param userProps
	 *            Map containing user provided properties (usually input via a
	 *            file)
	 * @param persistenceUnitName
	 *            Name of the persistence-unit (if any)
	 * @param ddlFile
	 *            Name of a file to output DDL to
	 * @param verbose
	 *            Verbose mode
	 * @return The PersistenceManagerFactory to use
	 * @throws NucleusException
	 *             Thrown if an error occurs in creating the required PMF
	 */
	protected static JDOPersistenceManagerFactory getPMFForMode(int mode,
			String api, Map userProps, String persistenceUnitName,
			String ddlFile, boolean verbose) {
		Map props = new HashMap();
		if (persistenceUnitName != null) {
			// persistence-unit specified so take the persistence properties
			// from that
			// Note that this will create the persistence with the properties
			// for the <persistence-unit>
			props.put("javax.jdo.option.PersistenceUnitName",
					persistenceUnitName);
		}

		if (userProps != null) {
			// Properties specified by the user in a file
			props.putAll(userProps);
		} else {
			// Properties specified via System properties (only support
			// particular ones)
			String[] propNames = { "datanucleus.ConnectionURL",
					"datanucleus.ConnectionDriverName",
					"datanucleus.ConnectionUserName",
					"datanucleus.ConnectionPassword", "datanucleus.Mapping",
					"javax.jdo.option.ConnectionURL",
					"javax.jdo.option.ConnectionDriverName",
					"javax.jdo.option.ConnectionUserName",
					"javax.jdo.option.ConnectionPassword",
					"javax.jdo.option.Mapping" };
			for (int i = 0; i < propNames.length; i++) {
				if (System.getProperty(propNames[i]) != null) {
					props.put(propNames[i], System.getProperty(propNames[i]));
				}
			}

			if (persistenceUnitName == null
					&& (props.get("javax.jdo.option.ConnectionURL") == null)
					&& (props.get("datanucleus.ConnectionURL") == null)
					&& (props.get("javax.persistence.jdbc.url") == null)) {
				// No URL or persistence-unit defined, so fall back to some
				// (undocumented) properties file
				File file = new File(System.getProperty("user.home")
						+ "/datanucleus.properties");
				if (file.exists()) {
					try {
						InputStream is = new FileInputStream(file);
						Properties fileProps = new Properties();
						fileProps.load(is);
						props.putAll(fileProps);
						is.close();
					} catch (IOException ioe) {
						// Ignore this
					}
				} else {
					throw new NucleusException(LOCALISER.msg("014041"));
				}
			}
		}

		// Use our PMF
		props.put("javax.jdo.PersistenceManagerFactoryClass",
				JDOPersistenceManagerFactory.class.getName());

		// Set the API
		props.put("datanucleus.persistenceApiName", api);

		// Tag on the mandatory props that we must have for each mode
		props.put("datanucleus.autoStartMechanism", "None"); // Don't want
																// autostart
																// interference
		if (mode == SCHEMATOOL_CREATE_MODE) {
			if (ddlFile != null) {
				// the tables must not be created in the DB, so do not validate
				// (DDL is being output to a file)
				props.put("datanucleus.validateConstraints", "false");
				props.put("datanucleus.validateColumns", "false");
				props.put("datanucleus.validateTables", "false");
			}
			props.put("datanucleus.autoCreateSchema", "true");
			props.put("datanucleus.autoCreateTables", "true");
			props.put("datanucleus.autoCreateConstraints", "true");
			props.put("datanucleus.fixedDatastore", "false");
			props.put("datanucleus.readOnlyDatastore", "false");
			props.put("datanucleus.rdbms.checkExistTablesOrViews", "true");
		} else if (mode == SCHEMATOOL_DELETE_MODE) {
			props.put("datanucleus.fixedDatastore", "false");
			props.put("datanucleus.readOnlyDatastore", "false");
		} else if (mode == SCHEMATOOL_VALIDATE_MODE) {
			props.put("datanucleus.autoCreateSchema", "false");
			props.put("datanucleus.autoCreateTables", "false");
			props.put("datanucleus.autoCreateConstraints", "false");
			props.put("datanucleus.autoCreateColumns", "false");
			props.put("datanucleus.validateTables", "true");
			props.put("datanucleus.validateColumns", "true");
			props.put("datanucleus.validateConstraints", "true");
		}

		// Create the PMF that we will use to communicate with the datastore
		JDOPersistenceManagerFactory pmf = new JDOPersistenceManagerFactory(
				props);

		if (verbose) {
			String msg = LOCALISER.msg(false, "014020");
			LOGGER.info(msg);
			System.out.println(msg);

			Map pmfProps = pmf.getPersistenceProperties();
			Set keys = pmfProps.keySet();
			List keyNames = new ArrayList(keys);
			Collections.sort(keyNames);
			Iterator keyNamesIter = keyNames.iterator();
			while (keyNamesIter.hasNext()) {
				String key = (String) keyNamesIter.next();
				if (key.startsWith("datanucleus")
						&& !key.equals("datanucleus.connectionpassword")) { // Don't
																			// output
																			// passwords
					msg = LOCALISER.msg(false, "014022", key, props.get(key));
					LOGGER.info(msg);
					System.out.println(msg);
				}
			}
			System.out.println();
		}
		return pmf;
	}

	/**
	 * Method to take the input for SchemaTool and returns the FileMetaData that
	 * it implies. The input should either be a persistence-unit name, or a set
	 * of input files.
	 * 
	 * @param metaDataMgr
	 *            Manager for MetaData
	 * @param clr
	 *            ClassLoader resolver
	 * @param verbose
	 *            Whether to put message verbosely
	 * @param persistenceUnitName
	 *            Name of the "persistence-unit"
	 * @param inputFiles
	 *            Input metadata/class files
	 * @return The FileMetaData for the input
	 * @throws NucleusException
	 *             Thrown if error(s) occur in processing the input
	 */
	protected static FileMetaData[] getFileMetaDataForInput(
			MetaDataManager metaDataMgr, ClassLoaderResolver clr,
			boolean verbose, String persistenceUnitName, String[] inputFiles) {
		FileMetaData[] filemds = null;

		String msg = null;
		if (inputFiles == null && persistenceUnitName == null) {
			msg = LOCALISER.msg(false, "014007");
			LOGGER.error(msg);
			System.out.println(msg);
			throw new NucleusUserException(msg);
		}

		if (persistenceUnitName != null) {
			// Schema management via "persistence-unit"
			msg = LOCALISER.msg(false, "014015", persistenceUnitName);
			LOGGER.info(msg);
			if (verbose) {
				System.out.println(msg);
				System.out.println();
			}

			// The PMF will have initialised the MetaDataManager with the
			// persistence-unit
			filemds = metaDataMgr.getFileMetaData();
		} else {
			// Schema management via "Input Files" (metadata/class)
			msg = LOCALISER.msg(false, "014009");
			LOGGER.info(msg);
			if (verbose) {
				System.out.println(msg);
			}
			for (int i = 0; i < inputFiles.length; i++) {
				String entry = LOCALISER.msg(false, "014010", inputFiles[i]);
				LOGGER.info(entry);
				if (verbose) {
					System.out.println(entry);
				}
			}
			if (verbose) {
				System.out.println();
			}

			// Read in the specified MetaData files - errors in MetaData will
			// return exceptions and so we stop
			try {
				// Split the input files into MetaData files and classes
				LOGGER.debug(LOCALISER.msg(false, "014011", ""
						+ inputFiles.length));
				HashSet metadataFiles = new HashSet();
				HashSet classNames = new HashSet();
				for (int i = 0; i < inputFiles.length; i++) {
					if (inputFiles[i].endsWith(".class")) {
						// Class file
						URL classFileURL = null;
						try {
							classFileURL = new URL("file:" + inputFiles[i]);
						} catch (Exception e) {
							msg = LOCALISER.msg(false, "014013", inputFiles[i]);
							LOGGER.error(msg);
							throw new NucleusUserException(msg);
						}

						String className = ClassUtils
								.getClassNameForFileURL(classFileURL);
						classNames.add(className);
					} else {
						// MetaData file
						metadataFiles.add(inputFiles[i]);
					}
				}

				// Initialise the MetaDataManager using the mapping files and
				// class names
				FileMetaData[] filemds1 = metaDataMgr.loadMetadataFiles(
						(String[]) metadataFiles
								.toArray(new String[metadataFiles.size()]),
						null);
				FileMetaData[] filemds2 = metaDataMgr.loadClasses(
						(String[]) classNames.toArray(new String[classNames
								.size()]), null);
				filemds = new FileMetaData[filemds1.length + filemds2.length];
				int pos = 0;
				for (int i = 0; i < filemds1.length; i++) {
					filemds[pos++] = filemds1[i];
				}
				for (int i = 0; i < filemds2.length; i++) {
					filemds[pos++] = filemds2[i];
				}
				LOGGER.debug(LOCALISER.msg(false, "014012", ""
						+ inputFiles.length));
			} catch (Exception e) {
				// Error reading input files
				msg = LOCALISER.msg(false, "014014", e.getMessage());
				LOGGER.error(msg, e);
				System.out.println(msg);
				if (e instanceof NucleusException) {
					throw (NucleusException) e;
				}
				throw new NucleusUserException(msg, e);
			}
		}

		return filemds;
	}

	/**
	 * Method to handle the creation of the schema for a set of classes. If no
	 * classes are supplied then assumes that all "known" classes (for this PMF)
	 * should be processed.
	 * 
	 * @param pmf
	 *            PersistenceManagerFactory to use when generating the schema
	 * @param inputClassNames
	 *            names of all classes whose schema is to be created
	 * @throws Exception
	 *             Thrown when either an error occurs parsing the MetaData, or
	 *             the DB definition is not defined.
	 */
	public void createSchema(JDOPersistenceManagerFactory pmf,
			List inputClassNames) throws Exception {
		ArrayList classNames = new ArrayList();
		if (inputClassNames == null || inputClassNames.size() == 0) {
			// Add all "known" classes
			MetaDataManager mmgr = pmf.getOMFContext().getMetaDataManager();
			Collection classesWithMetadata = mmgr.getClassesWithMetaData();
			classNames.addAll(classesWithMetadata);
		} else {
			// Add all input classes
			classNames.addAll(inputClassNames);
		}

		if (classNames.size() > 0) {

			ExecutionContext ec = ((JDOPersistenceManager) pmf
					.getPersistenceManager()).getObjectManager()
					.getExecutionContext();

			FileWriter ddlFileWriter = null;
			String ddlFilename = getDdlFile();

			if (ddlFilename == null) {

				ddlFilename = "datanucleus.schema";
			}

			if (ddlFilename != null) {
				// Open the DDL file for writing
				File ddlFile = StringUtils.getFileForFilename(ddlFilename);
				if (ddlFile.exists()) {
					// Delete existing file
					ddlFile.delete();
				}
				if (ddlFile.getParentFile() != null
						&& !ddlFile.getParentFile().exists()) {
					// Make sure the directory exists
					ddlFile.getParentFile().mkdirs();
				}
				ddlFile.createNewFile();
				ddlFileWriter = new FileWriter(ddlFile);

				SimpleDateFormat fmt = new SimpleDateFormat(
						"dd/MM/yyyy HH:mm:ss");
				ddlFileWriter
						.write("------------------------------------------------------------------\n");
				ddlFileWriter.write("-- DataNucleus SchemaTool " + "(version "
						+ ObjectManagerFactoryImpl.getVersionNumber() + ")"
						+ " ran at " + fmt.format(new java.util.Date()) + "\n");
				ddlFileWriter
						.write("------------------------------------------------------------------\n");
				if (getCompleteDdl()) {
					ddlFileWriter
							.write("-- Complete schema required for the following classes:-\n");
				} else {
					ddlFileWriter.write("-- Schema diff for "
							+ pmf.getConnectionURL()
							+ " and the following classes:-\n");
				}

				Iterator classNameIter = classNames.iterator();
				while (classNameIter.hasNext()) {
					ddlFileWriter
							.write("--     " + classNameIter.next() + "\n");
				}
				ddlFileWriter.write("--\n");
			}

			String[] classNameArray = (String[]) classNames
					.toArray(new String[classNames.size()]);
			printClassColumnFamilies(ec, classNameArray, ec
					.getClassLoaderResolver(), ddlFileWriter);

			if (ddlFileWriter != null) {
				ddlFileWriter.close();
			}

		} else {
			String msg = LOCALISER.msg(false, "014039");
			LOGGER.error(msg);
			System.out.println(msg);

			throw new Exception(msg);
		}
	}

	public void printClassColumnFamilies(ExecutionContext ec,
			String[] classNames, ClassLoaderResolver clr, FileWriter ddlwritter)
			throws Exception {

		Iterator iter = ((AbstractStoreManager) ec.getStoreManager())
				.getMetaDataManager().getReferencedClasses(classNames, clr)
				.iterator();
		CassandraStoreManager cassandraStoreManager = (CassandraStoreManager) ec
				.getStoreManager();
		CassandraConnectionInfo info = cassandraStoreManager
				.getConnectionInfo();
		String keySpace = info.getKeyspace();

		ddlwritter.write("<Keyspace Name=\"" + keySpace + "\">\n");

		while (iter.hasNext()) {
			ClassMetaData cmd = (ClassMetaData) iter.next();

			if (cmd.isEmbeddedOnly()) {
				// Nothing to do. Only persisted as SCO.
				NucleusLogger.DATASTORE.info(LOCALISER.msg("032012", cmd
						.getFullClassName()));
			} else {
				InheritanceMetaData imd = cmd.getInheritanceMetaData();
				//if (imd.getStrategy() == InheritanceStrategy.NEW_TABLE
					//	|| imd.getStrategy() == InheritanceStrategy.COMPLETE_TABLE) {
					
					ddlwritter.write("<ColumnFamily Name=\"" + cmd.getName()
							+ "\" CompareWith=\"BytesType\"/>\n");

				//}
			}

		}

		String keyspaceInfo = "<ReplicaPlacementStrategy>"
				+ cassandraStoreManager.getReplicaPlacementStrategy()
				+ "</ReplicaPlacementStrategy>\n" + "<ReplicationFactor>"
				+ cassandraStoreManager.getReplicationFactor()
				+ "</ReplicationFactor>\n" + "<EndPointSnitch>"
				+ cassandraStoreManager.getEndPointSnitch()
				+ "</EndPointSnitch>\n" + "</Keyspace>\n";
		ddlwritter.write(keyspaceInfo);

	}

	/**
	 * Method to handle the deletion of a schema's tables. Not implemented has
	 * the database does not support live schema changes at this point.
	 * 
	 * @throws Exception
	 *             the database does not support this option.
	 */
	public void deleteSchema(JDOPersistenceManagerFactory pmf,
			List inputClassNames) throws Exception {

		String msg = LOCALISER.msg(false, "SchemaTool.DeleteNotSupported");
		LOGGER.info(msg);
		System.out.println(msg);

		throw new Exception(msg);

	}

	/**
	 * 
	 * TODO check method function in the plug-in and if possible changes caused
	 * by Cassandra, it seems it does not depends on the store manager....
	 * Method to handle the validation of a schema's tables. () If no classes
	 * are supplied then assumes that all "known" classes (for this PMF) should
	 * be processed.
	 * 
	 * @param pmf
	 *            PersistenceManagerFactory to use when generating the schema
	 * @param inputClassNames
	 *            names of all classes whose schema is to be created
	 * @throws Exception
	 *             Thrown when either an error occurs parsing the MetaData, or
	 *             the DB definition is not defined.
	 */
	public void validateSchema(JDOPersistenceManagerFactory pmf,
			List inputClassNames) throws Exception {
		ArrayList classNames = new ArrayList();
		if (inputClassNames == null || inputClassNames.size() == 0) {
			// Add all "known" classes
			MetaDataManager mmgr = pmf.getOMFContext().getMetaDataManager();
			Collection classesWithMetadata = mmgr.getClassesWithMetaData();
			classNames.addAll(classesWithMetadata);
		} else {
			// Add all input classes
			classNames.addAll(inputClassNames);
		}

		if (classNames != null && classNames.size() > 0) {
			// Create a PersistenceManager for this store and validate
			ExecutionContext ec = ((JDOPersistenceManager) pmf
					.getPersistenceManager()).getObjectManager()
					.getExecutionContext();
			StoreManager storeMgr = ec.getStoreManager();

			String[] classNameArray = (String[]) classNames
					.toArray(new String[classNames.size()]);
			storeMgr.addClasses(classNameArray, ec.getClassLoaderResolver()); // Validates
																				// since
																				// we
																				// have
																				// the
																				// flags
																				// set
		} else {
			String msg = LOCALISER.msg(false, "014039");
			LOGGER.error(msg);
			System.out.println(msg);

			throw new Exception(msg);
		}
	}

	/**
	 * Initialize the command line arguments
	 * 
	 * @param args
	 *            Command line args
	 */
	public void setCommandLineArgs(String[] args) {
		cmd.parse(args);

		defaultArgs = cmd.getDefaultArgs();

		// populate options
		if (cmd.hasOption("api")) {
			omfContext.setApi(cmd.getOptionArg("api"));
			setApi(cmd.getOptionArg("api"));
		}

		setPersistenceProperties(omfContext.getApiAdapter()
				.getDefaultFactoryProperties());

		if (cmd.hasOption("create")) {
			setProperty(SCHEMATOOL_OPTION_MODE, "create");
		} else if (cmd.hasOption("delete")) {
			setProperty(SCHEMATOOL_OPTION_MODE, "delete");
		} else if (cmd.hasOption("validate")) {
			setProperty(SCHEMATOOL_OPTION_MODE, "validate");
		} else if (cmd.hasOption("dbinfo")) {
			setProperty(SCHEMATOOL_OPTION_MODE, "dbinfo");
		} else if (cmd.hasOption("schemainfo")) {
			setProperty(SCHEMATOOL_OPTION_MODE, "schemainfo");
		}
		if (cmd.hasOption("help")) {
			setProperty(SCHEMATOOL_OPTION_HELP, "true");
		}
		if (cmd.hasOption("ddlFile")) {
			setProperty(SCHEMATOOL_OPTION_DDLFILE, cmd.getOptionArg("ddlFile"));
		}
		if (cmd.hasOption("completeDdl")) {
			setProperty(SCHEMATOOL_OPTION_COMPLETEDDL, "true");
		}
		if (cmd.hasOption("pu")) {
			setProperty("datanucleus.PersistenceUnitName", cmd
					.getOptionArg("pu"));
		}
		if (cmd.hasOption("props")) {
			propsFileName = cmd.getOptionArg("props");
			setProperty("datanucleus.propertiesFile", propsFileName);
		}
		if (cmd.hasOption("v")) {
			setProperty(SCHEMATOOL_OPTION_VERBOSE, "true");
		}
	}

	/**
	 * @return the help
	 */
	public boolean isHelp() {
		return getBooleanProperty(SCHEMATOOL_OPTION_HELP);
	}

	/**
	 * @param help
	 *            the help to set
	 */
	public void setHelp(boolean help) {
		setProperty(SCHEMATOOL_OPTION_HELP, new Boolean(help));
	}

	/**
	 * Mutator for the flag to output complete DDL (when using DDL file)
	 * 
	 * @param completeDdl
	 *            Whether to return complete DDL
	 * @return The SchemaTool instance
	 */
	public SchemaTool setCompleteDdl(boolean completeDdl) {
		setProperty(SCHEMATOOL_OPTION_COMPLETEDDL, new Boolean(completeDdl));
		return this;
	}

	/**
	 * @return whether to use generate DDL (or just update DDL)
	 */
	public boolean getCompleteDdl() {
		return getBooleanProperty(SCHEMATOOL_OPTION_COMPLETEDDL);
	}

	/**
	 * @return the mode
	 */
	public String getModeName() {
		return getStringProperty(SCHEMATOOL_OPTION_MODE);
	}

	/**
	 * @param mode
	 *            the mode to set
	 * @return The SchemaTool instance
	 */
	public SchemaTool setModeName(String mode) {
		setProperty(SCHEMATOOL_OPTION_MODE, mode);
		return this;
	}

	/**
	 * Acessor for the API (JDO, JPA)
	 * 
	 * @return the API
	 */
	public String getApi() {
		return getStringProperty("datanucleus.persistenceApiName");
	}

	/**
	 * Mutator for the API (JDO, JPA)
	 * 
	 * @param api
	 *            the API
	 * @return The SchemaTool instance
	 */
	public SchemaTool setApi(String api) {
		setProperty("datanucleus.persistenceApiName", api);
		return this;
	}

	/**
	 * Acessor for the persistence unit name
	 * 
	 * @return the unit name
	 */
	public String getPersistenceUnitName() {
		return getStringProperty("datanucleus.PersistenceUnitName");
	}

	/**
	 * Mutator for the persistence-unit name.
	 * 
	 * @param unit
	 *            Unit name
	 * @return The SchemaTool instance
	 */
	public SchemaTool setPersistenceUnitName(String unit) {
		setProperty("datanucleus.PersistenceUnitName", unit);
		return this;
	}

	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return getBooleanProperty(SCHEMATOOL_OPTION_VERBOSE);
	}

	/**
	 * @param verbose
	 *            the verbose to set
	 * @return The SchemaTool instance
	 */
	public SchemaTool setVerbose(boolean verbose) {
		setProperty(SCHEMATOOL_OPTION_VERBOSE, new Boolean(verbose));
		return this;
	}

	/**
	 * @return the defaultArgs
	 */
	public String[] getDefaultArgs() {
		return defaultArgs;
	}

	/**
	 * Accessor for the DDL filename
	 * 
	 * @return the file to use when outputing the DDL
	 */
	public String getDdlFile() {
		return getStringProperty(SCHEMATOOL_OPTION_DDLFILE);
	}

	/**
	 * Mutator for the DDL file
	 * 
	 * @param file
	 *            the file to use when outputting the DDL
	 * @return The SchemaTool instance
	 */
	public SchemaTool setDdlFile(String file) {
		setProperty(SCHEMATOOL_OPTION_DDLFILE, file);
		return this;
	}

	/**
	 * Acessor for the properties file name (optional).
	 * 
	 * @return the props file name
	 */
	public String getPropsFileName() {
		return propsFileName;
	}
}