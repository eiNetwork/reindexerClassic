package org.vufind;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.econtent.ExtractEContentFromMarc;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;

import org.vufind.DeleteDependentRecords;
//import org.strands.StrandsProcessor;  not  used BA+ 08/13/2014


/**
 * Runs the nightly reindex process to update solr index based on the latest
 * export from the ILS.
 * 
 * Reindex process does the following steps: 
 * 1) Runs export process to extract
 * marc records from the ILS (if applicable) 
 * 
 * @author Mark Noble <mnoble@turningleaftech.com>
 * 
 */
public class ReindexProcess {

	private static Logger logger	= Logger.getLogger(ReindexProcess.class);

	//General configuration
	private static String serverName;
	private static String indexSettings;
	private static Ini configIni;
	private static String solrPort;
	
	//Reporting information
	private static long reindexLogId;
	private static long startTime;
	private static long endTime;
	
	//Variables to determine what sub processes to run.
	private static boolean reloadDefaultSchema = false;
	private static boolean updateSolr = true;
	private static boolean updateResources = true;
	private static boolean loadEContentFromMarc = false;
	private static boolean exportStrandsCatalog = false;
	private static boolean exportOPDSCatalog = true;
	private static boolean updateAlphaBrowse = true;
	private static String idsToProcess = null;
	private static boolean DeleteERecordsinDBNotinMarcOrOD = false;
	private static boolean callAPIAddOnly = false;
	private static boolean AddNonMarcODRecords = false;
	private static boolean OverDriveAvailabilityAPI = false;
	
	//Database connections and prepared statements
	private static Connection vufindConn = null;
	private static Connection reindexerConn = null;
	
	private static PreparedStatement updateCronLogLastUpdatedStmt;
	private static PreparedStatement addNoteToCronLogStmt;
	
	
	
	/**
	 * Starts the reindexing process
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		startTime = new Date().getTime();
		// Get the configuration filename
		if (args.length == 0) {
			System.out.println("Please enter the server to index as the first parameter");
			System.exit(1);
		}
		serverName = args[0];
		System.setProperty("reindex.process.serverName", serverName);
		
		if (args.length > 1){
			indexSettings = args[1];
		}
		
		initializeReindex();
		
		// Runs the export process to extract marc records from the ILS (if applicable)
		runExportScript();
		
		//Reload schemas
		if (reloadDefaultSchema){
			reloadDefaultSchemas();
		}
		
		//Process all records (marc records, econtent that has been added to the database, and resources)
		ArrayList<IRecordProcessor> recordProcessors;
		recordProcessors = loadRecordProcesors();
		try {
			if (recordProcessors.size() > 0){
				//Do processing of marc records with record processors loaded above. 
				// includes indexing records
				// extracting eContent from records
				// Updating resource information
				// Saving records to strands - may need to move to resources if we are doing partial exports
				processMarcRecords(recordProcessors);
								
				//Process eContent records that have been saved to the database. 
				processEContentRecords(recordProcessors);
								
				//Do processing of resources as needed (for extraction of resources).
				processResources(recordProcessors);
										
				int errorCount = 0, processedCount = 0;
				ProcessorResults firstResults = null;
				for (IRecordProcessor processor : recordProcessors){
					processor.finish();
					ProcessorResults results = processor.getResults();
					if( results != null ) {
						if( firstResults == null ) {
							firstResults = results;
						}
						errorCount += results.getNumErrors();
						processedCount += results.getRecordsProcessed();
					}
				}
				
				// swap cores if needed
				if( firstResults != null ) {
					if( (configIni.get("Reindex", "skipCoreSwap") != null) && configIni.get("Reindex", "skipCoreSwap").equals("true") ) {
						firstResults.addNote("configuration specifies to skip core swap, not swapping");
						logger.info("Cores are **NOT** swapped: configuration said to skip");
					//Do not pass the import if more than 1% of the records have errors 
					} else if (errorCount <= processedCount * .01){
						firstResults.addNote("index passed checks, swapping cores so new index is active.");
						logger.info("Cores are swapped: number of successes above threshold");
						URLPostResponse response = Util.getURL("http://localhost:" + solrPort + "/solr/admin/cores?action=SWAP&core=biblio2&other=biblio", logger);
						if (!response.isSuccess()){
							firstResults.addNote("Error swapping cores " + response.getMessage());
						}else{
							firstResults.addNote("Result of swapping cores " + response.getMessage());
						}
					}else{
						firstResults.addNote("index did not pass check, not swapping");
						logger.info("Cores are **NOT** swapped: too many errors");
					}
					firstResults.saveResults();
				}
				
				//BA+++ added to cleanup dependent records from delete of items in econtent_record not in Marc or Overdrive
				if ( isDeleteERecordsinDBNotinMarcOrOD() ) {
					DeleteDependentRecords delRecs = new DeleteDependentRecords(logger, vufindConn);
					delRecs.ExecuteDeletes();
				}				
			}
		} catch (Error e) {
			logger.error("Error processing reindex ", e);
			addNoteToCronLog("Error processing reindex " + e.toString());
		}
		
		// Send completion information
		endTime = new Date().getTime();
		sendCompletionMessage(recordProcessors);
		
		String basePath = configIni.get("Reindex", "basePath");
		logger.info("Restarting Solr for " + serverName);
		try {
			SystemUtil.executeCommand(basePath + "/restartSolr.sh", logger);
		} catch (IOException e) {
			logger.error("Error restarting Solr", e);
		}

		logger.info("Resetting circ_trans time for " + serverName);
		try {
			SystemUtil.executeCommand("/usr/bin/php " + basePath + "/rollBackCircTransTime.php", logger);
		} catch (IOException e) {
			logger.error("Error resetting circ_trans time", e);
		}

		addNoteToCronLog("Finished Reindex for " + serverName);
		logger.info("Finished Reindex for " + serverName);
	}
	
	private static void reloadDefaultSchemas() {
		logger.info("Reloading schemas from default");
		try {
			//Copy default schemas from biblio to biblio2 and econtent
			logger.debug("Copying " + "../solr/biblio/conf/schema.xml" + " to " + "../solr/biblio2/conf/schema.xml");
			if (!Util.copyFile(new File("../solr/biblio/conf/schema.xml"), new File("../solr/biblio2/conf/schema.xml"))){
				logger.info("Unable to copy schema to biblio2");
				addNoteToCronLog("Unable to copy schema to biblio2");
			}
			logger.debug("Copying " + "../solr/biblio/conf/schema.xml" + " to " + "../solr/econtent/conf/schema.xml");
			if (!Util.copyFile(new File("../solr/biblio/conf/schema.xml"), new File("../solr/econtent/conf/schema.xml"))){
				logger.info("Unable to copy schema to econtent");
				addNoteToCronLog("Unable to copy schema to econtent");
			}
			logger.debug("Copying " + "../solr/biblio/conf/schema.xml" + " to " + "../solr/econtent2/conf/schema.xml");
			if (!Util.copyFile(new File("../solr/biblio/conf/schema.xml"), new File("../solr/econtent2/conf/schema.xml"))){
				logger.info("Unable to copy schema to econtent");
				addNoteToCronLog("Unable to copy schema to econtent");
			}
		} catch (IOException e) {
			logger.error("error reloading copying default scehmas", e);
			addNoteToCronLog("error reloading copying default scehmas " + e.toString());
		}
	}

	private static ArrayList<IRecordProcessor> loadRecordProcesors(){
		ArrayList<IRecordProcessor> supplementalProcessors = new ArrayList<IRecordProcessor>();
		if (updateSolr){
			MarcIndexer marcIndexer = new MarcIndexer();
			addNoteToCronLog("Initializing MarcIndexer");
			if (marcIndexer.init(configIni, serverName, reindexLogId, vufindConn, logger)){
				supplementalProcessors.add(marcIndexer);
			}else{
				logger.error("Could not initialize marcIndexer");
				System.exit(1);
			}
		}
		if (updateResources){
			addNoteToCronLog("Initializing UpdateResourceInformation");
			UpdateResourceInformation resourceUpdater = new UpdateResourceInformation();
			if (resourceUpdater.init(configIni, serverName, reindexLogId, vufindConn, logger)){
				supplementalProcessors.add(resourceUpdater);
			}else{
				logger.error("Could not initialize resourceUpdater");
				System.exit(1);
			}
		}
		if (loadEContentFromMarc){
			addNoteToCronLog("Initializing ExtractEContentFromMarc");
			ExtractEContentFromMarc econtentExtractor = new ExtractEContentFromMarc();
			if (econtentExtractor.init(configIni, serverName, reindexLogId, vufindConn, reindexerConn, logger)){
				supplementalProcessors.add(econtentExtractor);
				if ( isDeleteERecordsinDBNotinMarcOrOD() ) {
					int delRecs = econtentExtractor.deleteOverDriveTitlesInDb();
					logger.debug("deleteOverDriveTitlesInDb  Not in Overdrive  - " + delRecs);
				}
			}else{
				logger.error("Could not initialize econtentExtractor");
				System.exit(1);
			}
		}
		if (updateAlphaBrowse){
			addNoteToCronLog("Initializing AlphaBrowseProcessor");
			AlphaBrowseProcessor alphaBrowseProcessor = new AlphaBrowseProcessor();
			if (alphaBrowseProcessor.init(configIni, serverName, reindexLogId, vufindConn, logger)){
				supplementalProcessors.add(alphaBrowseProcessor);
			}else{
				logger.error("Could not initialize strandsProcessor");
				System.exit(1);
			}
		}
		if (exportOPDSCatalog){
			// 14) Generate OPDS catalog
		}				
		
		return supplementalProcessors;
	}

	private static void processResources(ArrayList<IRecordProcessor> supplementalProcessors) {
		ArrayList<IResourceProcessor> resourceProcessors = new ArrayList<IResourceProcessor>();
		for (IRecordProcessor processor: supplementalProcessors){
			if (processor instanceof IResourceProcessor){
				resourceProcessors.add((IResourceProcessor)processor);
			}
		}
		if (resourceProcessors.size() == 0){
			return;
		}
		
		logger.info("Processing resources");
		addNoteToCronLog("Processing resources");
		try {
			int resourcesProcessed = 0;
			long batchCount = 0;
			PreparedStatement resourceCountStmt = vufindConn.prepareStatement("SELECT count(id) FROM resource", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet resourceCountRs = resourceCountStmt.executeQuery();
			if (resourceCountRs.next()){
				long numResources = resourceCountRs.getLong(1);
				logger.info("There are " + numResources + " resources currently loaded");
				long firstResourceToProcess = 0;
				long batchSize = 25000;
				PreparedStatement allResourcesStmt = vufindConn.prepareStatement("SELECT * FROM resource LIMIT ?, ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				while (firstResourceToProcess <= numResources){
					logger.debug("processing batch " + ++batchCount + " from " + firstResourceToProcess + " to " + (firstResourceToProcess + batchSize));
					allResourcesStmt.setLong(1, firstResourceToProcess);
					allResourcesStmt.setLong(2, batchSize);
					ResultSet allResources = allResourcesStmt.executeQuery();
					while (allResources.next()){
						for (IResourceProcessor resourceProcessor : resourceProcessors){
							resourceProcessor.processResource(allResources);
						}
					}
					allResources.close();
					firstResourceToProcess += batchSize;
					resourcesProcessed++;
					if (resourcesProcessed % 1000 == 0){
						updateLastUpdateTime();
					}
				}
			}
			resourceCountRs.close();
		} catch (Exception e) {
			logger.error("Exception processing resources", e);
			System.out.println("Exception processing resources " + e.toString());
			addNoteToCronLog("Exception processing resources " + e.toString());
		} catch (Error e) {
			logger.error("Error processing resources", e);
			System.out.println("Error processing resources " + e.toString());
			addNoteToCronLog("Error processing resources " + e.toString());
		}
	}

	private static void processEContentRecords(ArrayList<IRecordProcessor> supplementalProcessors) {
		logger.info("Processing econtent records");
		addNoteToCronLog("Processing econtent records");
		ArrayList<IEContentProcessor> econtentProcessors = new ArrayList<IEContentProcessor>();
		for (IRecordProcessor processor: supplementalProcessors){
			if (processor instanceof IEContentProcessor){
				econtentProcessors.add((IEContentProcessor)processor);
			}
		}
		if (econtentProcessors.size() == 0){
			return;
		}
	}

	private static void processMarcRecords(ArrayList<IRecordProcessor> supplementalProcessors) {
		ArrayList<IMarcRecordProcessor> marcProcessors = new ArrayList<IMarcRecordProcessor>();
		for (IRecordProcessor processor: supplementalProcessors){
			if (processor instanceof IMarcRecordProcessor){
				marcProcessors.add((IMarcRecordProcessor)processor);
			}
		}
		if (marcProcessors.size() == 0){
			return;
		}
		
		MarcProcessor marcProcessor = new MarcProcessor();
		marcProcessor.init(serverName, configIni, vufindConn, logger);
		
		if (supplementalProcessors.size() > 0){
			logger.info("Processing exported marc records");
			addNoteToCronLog("Processing exported marc records");
			marcProcessor.processMarcFiles(marcProcessors, logger);
			}
	}

	private static void runExportScript() {
		String extractScript = configIni.get("Reindex", "extractScript");
		if (extractScript.length() > 0) {
			addNoteToCronLog("Running extract script " + extractScript);
			
			logger.info("Running export script");
			try {
				String reindexResult = SystemUtil.executeCommand(extractScript, logger);
				logger.info("Result of extractScript (" + extractScript + ") was " + reindexResult);
				addNoteToCronLog("Result of extractScript (" + extractScript + ") was " + reindexResult);
			} catch (IOException e) {
				logger.error("Error running extract script, stopping reindex process", e);
				addNoteToCronLog("Error running extract script, stopping reindex process " + e.toString());
				System.exit(1);
			}
		}
	}

	public static void addNoteToCronLog(String note) {
	}
	
	public static void updateLastUpdateTime(){
		try {
			updateCronLogLastUpdatedStmt.setLong(1, new Date().getTime() / 1000);
			updateCronLogLastUpdatedStmt.setLong(2, reindexLogId);
			updateCronLogLastUpdatedStmt.executeUpdate();
			//Sleep for a little bit to make sure we don't block connectivity for other programs 
			Thread.sleep(5);
			//Thread.yield();
		} catch (SQLException e) {
			logger.error("Error setting last updated time in Cron Log", e);
		} catch (InterruptedException e) {
			logger.error("Sleep interrupted", e);
		}
	}

	private static void initializeReindex() {
		// Delete the existing reindex.log file
		File solrmarcLog = new File("../logs/reindex.log");
		if (solrmarcLog.exists()){
			solrmarcLog.delete();
		}
		for (int i = 1; i <= 10; i++){
			solrmarcLog = new File("../logs/reindex.log." + i);
			if (solrmarcLog.exists()){
				solrmarcLog.delete();
			}
		}
		solrmarcLog = new File("../logs/solrmarc.log");
		if (solrmarcLog.exists()){
			solrmarcLog.delete();
		}
		for (int i = 1; i <= 4; i++){
			solrmarcLog = new File("../logs/solrmarc.log." + i);
			if (solrmarcLog.exists()){
				solrmarcLog.delete();
			}
		}
		
		// Initialize the logger
		File log4jFile = new File("log4j.properties");
		if (log4jFile.exists()) {
			PropertyConfigurator.configure(log4jFile.getAbsolutePath());
		} else {
			System.out.println("Could not find log4j configuration " + log4jFile.getAbsolutePath());
			System.exit(1);
		}
		
		logger.info("Starting Reindex for " + serverName);

		// Parse the configuration file
		configIni = loadConfigFile("config.ini");
		
		if (indexSettings != null){
			logger.info("Loading index settings from override file " + indexSettings);
			String indexSettingsName = "../conf/" + indexSettings + ".ini";
			File indexSettingsFile = new File(indexSettingsName);
			if (!indexSettingsFile.exists()) {
				indexSettingsName = "../conf/" + indexSettings + ".ini";
				indexSettingsFile = new File(indexSettingsName);
				if (!indexSettingsFile.exists()) {
					logger.error("Could not find indexSettings file " + indexSettings);
					System.exit(1);
				}
			}
			try {
				Ini indexSettingsIni = new Ini();
				indexSettingsIni.load(new FileReader(indexSettingsFile));
				for (Section curSection : indexSettingsIni.values()){
					for (String curKey : curSection.keySet()){
						logger.debug("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
						//System.out.println("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
						configIni.put(curSection.getName(), curKey, curSection.get(curKey));
					}
				}
			} catch (InvalidFileFormatException e) {
				logger.error("IndexSettings file is not valid.  Please check the syntax of the file.", e);
			} catch (IOException e) {
				logger.error("IndexSettings file could not be read.", e);
			}
		}
		
		// tell them right now whether we plan to swap at the end
		if( (configIni.get("Reindex", "skipCoreSwap") != null) && configIni.get("Reindex", "skipCoreSwap").equals("true") ) {
			logger.info("This run will **NOT** swap the cores when it finishes (configuration says to skip)");
		} else {
			logger.info("This run will swap the cores when it finishes (pending a success check)");
		}

		solrPort = configIni.get("Reindex", "solrPort");
		if (solrPort == null || solrPort.length() == 0) {
			logger.error("You must provide the port where the solr index is loaded in the import configuration file");
			System.exit(1);
		}
		
		String reloadDefaultSchemaStr = configIni.get("Reindex", "reloadDefaultSchema");
		if (reloadDefaultSchemaStr != null){
			reloadDefaultSchema = Boolean.parseBoolean(reloadDefaultSchemaStr);
		}
		String updateSolrStr = configIni.get("Reindex", "updateSolr");
		if (updateSolrStr != null){
			updateSolr = Boolean.parseBoolean(updateSolrStr);
		}
		String updateResourcesStr = configIni.get("Reindex", "updateResources");
		if (updateResourcesStr != null){
			updateResources = Boolean.parseBoolean(updateResourcesStr);
		}
		String exportStrandsCatalogStr = configIni.get("Reindex", "exportStrandsCatalog");
		if (exportStrandsCatalogStr != null){
			exportStrandsCatalog = Boolean.parseBoolean(exportStrandsCatalogStr);
		}
		String exportOPDSCatalogStr = configIni.get("Reindex", "exportOPDSCatalog");
		if (exportOPDSCatalogStr != null){
			exportOPDSCatalog = Boolean.parseBoolean(exportOPDSCatalogStr);
		}
		String loadEContentFromMarcStr = configIni.get("Reindex", "loadEContentFromMarc");
		if (loadEContentFromMarcStr != null){
			loadEContentFromMarc = Boolean.parseBoolean(loadEContentFromMarcStr);
		}
		String updateAlphaBrowseStr  = configIni.get("Reindex", "updateAlphaBrowse");
		if (updateAlphaBrowseStr != null){
			updateAlphaBrowse = Boolean.parseBoolean(updateAlphaBrowseStr);
		}
		String DeleteERecordsinDBNotinMarcOrODStr = configIni.get("Reindex", "DeleteERecordsinDBNotinMarcOrOD");
		if (DeleteERecordsinDBNotinMarcOrODStr != null){
			DeleteERecordsinDBNotinMarcOrOD = Boolean.parseBoolean(DeleteERecordsinDBNotinMarcOrODStr);
		}
		
		String callAPIAddOnlyStr = configIni.get("Reindex", "callAPIAddOnly");
		if (callAPIAddOnlyStr != null){
			callAPIAddOnly = Boolean.parseBoolean(callAPIAddOnlyStr);
		}
		String AddNonMarcODRecordsStr = configIni.get("Reindex", "AddNonMarcODRecords");
		if (AddNonMarcODRecordsStr != null){
			AddNonMarcODRecords = Boolean.parseBoolean(AddNonMarcODRecordsStr);
		}

		String OverDriveAvailabilityAPIStr = configIni.get("Reindex", "OverDriveAvailabilityAPI");
		if (OverDriveAvailabilityAPIStr != null){
			OverDriveAvailabilityAPI = Boolean.parseBoolean(OverDriveAvailabilityAPIStr);
		}
		
		
		logger.info("Setting up database connections");
		//Setup connections to vufind and econtent databases
		String databaseConnectionInfo = Util.cleanIniValue(configIni.get("Database", "database_vufind_jdbc"));
		if (databaseConnectionInfo == null || databaseConnectionInfo.length() == 0) {
			logger.error("VuFind Database connection information not found in Database Section.  Please specify connection information in database_vufind_jdbc.");
			System.exit(1);
		}
		try {
			vufindConn = DriverManager.getConnection(databaseConnectionInfo);
		} catch (SQLException e) {
			logger.error("Could not connect to vufind database", e);
			System.exit(1);
		}
		
		String reindexerDBConnectionInfo = Util.cleanIniValue(configIni.get("Database", "database_reindexer_jdbc"));
		if (reindexerDBConnectionInfo == null || reindexerDBConnectionInfo.length() == 0) {
			logger.error("Database connection information for reindexer database not found in Database Section.  Please specify connection information as database_reindexer_jdbc key.");
			System.exit(1);
		}
		try {
			reindexerConn = DriverManager.getConnection(reindexerDBConnectionInfo);
		} catch (SQLException e) {
			logger.error("Could not connect to reindexer database", e);
			System.exit(1);
		}
		
		//Start a reindex log entry 
		try {
			logger.info("Creating log entry for index");
			PreparedStatement createLogEntryStatement = vufindConn.prepareStatement("INSERT INTO reindex_log (startTime, lastUpdate, notes) VALUES (?, ?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
			createLogEntryStatement.setLong(1, new Date().getTime() / 1000);
			createLogEntryStatement.setLong(2, new Date().getTime() / 1000);
			createLogEntryStatement.setString(3, "Initialization complete");
			createLogEntryStatement.executeUpdate();
			ResultSet generatedKeys = createLogEntryStatement.getGeneratedKeys();
			if (generatedKeys.next()){
				reindexLogId = generatedKeys.getLong(1);
			}
			generatedKeys.close();
			
			updateCronLogLastUpdatedStmt = vufindConn.prepareStatement("UPDATE reindex_log SET lastUpdate = ? WHERE id = ?");
			addNoteToCronLogStmt = vufindConn.prepareStatement("UPDATE reindex_log SET notes = ?, lastUpdate = ? WHERE id = ?");
		} catch (SQLException e) {
			logger.error("Unable to create log entry for reindex process", e);
			System.exit(0);
		}
		
		idsToProcess = Util.cleanIniValue(configIni.get("Reindex", "idsToProcess"));
		if (idsToProcess == null || idsToProcess.length() == 0){
			idsToProcess = null;
			logger.debug("Did not load a set of idsToProcess");
		}else{
			logger.debug("idsToProcess = " + idsToProcess);
		}
		
	}
	
	private static void sendCompletionMessage(ArrayList<IRecordProcessor> recordProcessors){
		logger.info("Reindex Results");
		logger.info("Processor, Records Processed, eContent Processed, Resources Processed, Errors, Added, Updated, Deleted, Skipped");
		for (IRecordProcessor curProcessor : recordProcessors){
			ProcessorResults results = curProcessor.getResults();
			logger.info(results.toCsv());
		}
		long elapsedTime = endTime - startTime;
		float elapsedMinutes = (float)elapsedTime / (float)(60000); 
		logger.info("Time elapsed: " + elapsedMinutes + " minutes");
		
		try {
			PreparedStatement finishedStatement = vufindConn.prepareStatement("UPDATE reindex_log SET endTime = ? WHERE id = ?");
			finishedStatement.setLong(1, new Date().getTime() / 1000);
			finishedStatement.setLong(2, reindexLogId);
			finishedStatement.executeUpdate();
		} catch (SQLException e) {
			logger.error("Unable to update reindex log with completion time.", e);
		}
	}
	
	private static Ini loadConfigFile(String filename){
		//First load the default config file
		String configName = "../conf/" + filename;
		logger.info("Loading configuration from " + configName);
		File configFile = new File(configName);
		if (!configFile.exists()) {
			logger.error("Could not find configuration file " + configName);
			System.exit(1);
		}

		// Parse the configuration file
		Ini ini = new Ini();
		try {
			ini.load(new FileReader(configFile));
		} catch (InvalidFileFormatException e) {
			logger.error("Configuration file is not valid.  Please check the syntax of the file.", e);
		} catch (FileNotFoundException e) {
			logger.error("Configuration file could not be found.  You must supply a configuration file in conf called config.ini.", e);
		} catch (IOException e) {
			logger.error("Configuration file could not be read.", e);
		}
		
		//Now override with the site specific configuration
		String siteSpecificFilename = "../conf/" + filename;
		logger.info("Loading site specific config from " + siteSpecificFilename);
		File siteSpecificFile = new File(siteSpecificFilename);
		if (!siteSpecificFile.exists()) {
			logger.error("Could not find server specific config file");
			System.exit(1);
		}
		try {
			Ini siteSpecificIni = new Ini();
			siteSpecificIni.load(new FileReader(siteSpecificFile));
			for (Section curSection : siteSpecificIni.values()){
				for (String curKey : curSection.keySet()){
					//logger.debug("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
					//System.out.println("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
					ini.put(curSection.getName(), curKey, curSection.get(curKey));
				}
			}
		} catch (InvalidFileFormatException e) {
			logger.error("Site Specific config file is not valid.  Please check the syntax of the file.", e);
		} catch (IOException e) {
			logger.error("Site Specific config file could not be read.", e);
		}
		return ini;
	}
	public static Connection getVufindConn(){
		return vufindConn;
	}
	public static boolean isDeleteERecordsinDBNotinMarcOrOD() {
		return DeleteERecordsinDBNotinMarcOrOD;
	}

	public static void setCallAPIAddOnly(
			boolean callAPIAddOnly) {
		ReindexProcess.callAPIAddOnly = callAPIAddOnly;
	}

	public static boolean isCallAPIAddOnly() {
		return callAPIAddOnly;
	}
	
	public static void setAddNonMarcODRecords(
			boolean AddNonMarcODRecords) {
		ReindexProcess.AddNonMarcODRecords = AddNonMarcODRecords;
	}

	public static boolean isAddNonMarcODRecords() {
		return AddNonMarcODRecords;
	}
	
	public static void setOverDriveAvailabilityAPI(
			boolean OverDriveAvailabilityAPI) {
		ReindexProcess.OverDriveAvailabilityAPI = OverDriveAvailabilityAPI;
	}

	public static boolean isOverDriveAvailabilityAPI() {
		return OverDriveAvailabilityAPI;
	}
}