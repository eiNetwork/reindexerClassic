package org.econtent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.ini4j.Ini;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.vufind.LexileData;
import org.vufind.MarcRecordDetails;
import org.vufind.IMarcRecordProcessor;
import org.vufind.IRecordProcessor;
import org.vufind.MarcProcessor;
import org.vufind.ProcessorResults;
import org.vufind.ReindexProcess;
import org.vufind.URLPostResponse;
import org.vufind.Util;

import au.com.bytecode.opencsv.CSVWriter;
/**
 * Run this export to build the file to import into VuFind
 * SELECT econtent_record.id, sourceUrl, item_type, filename, folder INTO OUTFILE 'd:/gutenberg_files.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' FROM econtent_record INNER JOIN econtent_item on econtent_record.id = econtent_item.recordId  WHERE source = 'Gutenberg';

 * @author Mark Noble
 *
 */

public class ExtractEContentFromMarc implements IMarcRecordProcessor, IRecordProcessor{
	private MarcProcessor marcProcessor;
	private String solrPort;
	private Logger logger;
	private ConcurrentUpdateSolrServer updateServer;
	
	private String localWebDir;
	
	private boolean extractEContentFromUnchangedRecords;
	private boolean checkOverDrive;
	private String econtentDBConnectionInfo;
	
	private String vufindUrl;
	
	private HashMap<String, EcontentRecordInfo> overDriveTitlesWithoutIlsId = new HashMap<String, EcontentRecordInfo>();
	private HashMap<String, EcontentRecordInfo> overDriveIdsFromEContent = new HashMap<String, EcontentRecordInfo>();
	
	private PreparedStatement getAllOverDriveTitles;
	private PreparedStatement getIndexedMetaData;
	private PreparedStatement getExternalFormats;
	
	// BA++ Setup additional statements to delete records from db not in OverDrive
	private PreparedStatement					deleteEContentRecordnotinOverdrive;
	
	public ProcessorResults results;
	
	private HashMap<String, OverDriveRecordInfo> overDriveTitles = new HashMap<String, OverDriveRecordInfo>();
	
	private HashMap<String, String> processedOverDriveRecords = new HashMap<String, String>();
	private HashMap<String, ArrayList<String>> duplicateOverDriveRecordsInMillennium = new HashMap<String, ArrayList<String>>();
	private HashMap<String, MarcRecordDetails> millenniumRecordsNotInOverDrive = new HashMap<String, MarcRecordDetails>();
	private HashSet<String> recordsWithoutOverDriveId = new HashSet<String>(); 

	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Logger logger) {
		return init(configIni, serverName, reindexLogId, vufindConn, null, logger);
	}
	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Connection reindexerConn, Logger logger) {
		this.logger = logger;
		//Import a marc record into the eContent core. 
		if (!loadConfig(configIni, logger)){
			return false;
		}
		results = new ProcessorResults("Extract eContent from ILS", reindexLogId, vufindConn, logger);
		solrPort = configIni.get("Reindex", "solrPort");
		
		localWebDir = configIni.get("Site", "local");
		
		//Initialize the updateServer
		updateServer = new ConcurrentUpdateSolrServer("http://localhost:" + solrPort + "/solr/biblio2", 500, 10);
		
		//Check to see if we should clear the existing index
		String clearEContentRecordsAtStartOfIndexVal = configIni.get("Reindex", "clearEContentRecordsAtStartOfIndex");
		boolean clearEContentRecordsAtStartOfIndex;
		if (clearEContentRecordsAtStartOfIndexVal == null){
			clearEContentRecordsAtStartOfIndex = false;
		}else{
			clearEContentRecordsAtStartOfIndex = Boolean.parseBoolean(clearEContentRecordsAtStartOfIndexVal);
		}
		results.addNote("clearEContentRecordsAtStartOfIndex = " + clearEContentRecordsAtStartOfIndex);
		if (clearEContentRecordsAtStartOfIndex){
			logger.info("Clearing existing econtent records from index");
			results.addNote("clearing existing econtent records");
			URLPostResponse response = Util.postToURL("http://localhost:" + solrPort + "/solr/biblio2/update/?commit=true", "<delete><query>recordtype:EContent</query></delete>", logger);
			if (!response.isSuccess()){
				results.addNote("Error clearing existing econtent records " + response.getMessage());
			}
		}
		
		String extractEContentFromUnchangedRecordsVal = configIni.get("Reindex", "extractEContentFromUnchangedRecords");
		if (extractEContentFromUnchangedRecordsVal == null){
			extractEContentFromUnchangedRecords = false;
		}else{
			extractEContentFromUnchangedRecords = Boolean.parseBoolean(extractEContentFromUnchangedRecordsVal);
		}
		if (clearEContentRecordsAtStartOfIndex) extractEContentFromUnchangedRecords = true;
		results.addNote("extractEContentFromUnchangedRecords = " + extractEContentFromUnchangedRecords);
		
		String checkOverDriveVal = configIni.get("Reindex", "checkOverDrive");
		if (checkOverDriveVal == null){
			checkOverDrive = true;
		}else{
			checkOverDrive = Boolean.parseBoolean(checkOverDriveVal);
		}
		
		try {
			//Connect to the reindexer database
			getAllOverDriveTitles = reindexerConn.prepareStatement("SELECT * FROM externalData", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getIndexedMetaData = reindexerConn.prepareStatement("SELECT * FROM indexedMetaData WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getExternalFormats = reindexerConn.prepareStatement("SELECT externalFormatId, externalDataId, formatId, formatLink, dateAdded, dateUpdated, sourceId, externalFormatId, externalFormatName, externalFormatNumber, displayFormat, formatCategory FROM externalFormats JOIN format ON (externalFormats.formatId=format.id) WHERE externalFormats.externalDataId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} catch (Exception ex) {
			// handle any errors
			logger.error("Error initializing econtent extraction ", ex);
			return false;
		}finally{
			results.saveResults();
		}
		if( !checkOverDrive ) {
			return true;
		} else {
			try {				
				return loadOverDriveTitlesFromDatabase(configIni);
			} catch( JSONException ex ) {
				// handle any errors
				logger.error("Error loading econtent titles from database ", ex);
				return false;
			}
		}
		
	}
	
	private boolean loadOverDriveTitlesFromDatabase(Ini configIni) throws JSONException {
		// Lessa : Added this try/catch block
		try {
			results.addNote("Loading OverDrive information from database");
			results.saveResults();
			
			// execute the query
			ResultSet products = getAllOverDriveTitles.executeQuery();
			int numLoaded = 0;
			while( products.next() ){
				OverDriveRecordInfo curRecord = loadOverDriveRecordFromDB(products);
				if( curRecord != null ) {
					logger.debug("Loading record " + curRecord.getId());
					overDriveTitles.put(curRecord.getId(), curRecord);
					numLoaded++;
				}
			}
			// BP ==> Baked in library name so it matches our previous reindexing stats workflow for the help desk
			logger.info("Carnegie Library of Pittsburgh (PA) collection has " + numLoaded + " products in it. Count from Reindexer DB.");
			products.close();
		} catch (Exception e) {
			results.addNote("error loading information from OverDrive Database " + e.toString());
			results.incErrors();
			logger.error("Error loading overdrive titles", e);
			return false;
		}		
		return true;
	}

	private OverDriveRecordInfo loadOverDriveRecordFromDB(ResultSet titleResults) throws JSONException {
		OverDriveRecordInfo curRecord = new OverDriveRecordInfo();
		try
		{
			curRecord.setDatabaseId(titleResults.getInt("id"));
			curRecord.setId(titleResults.getString("externalId"));
			logger.debug("Processing overdrive title " + curRecord.getId());
			//BA+++  sortTitle
			
			// grab the formats and metadata for this record
			getIndexedMetaData.setInt(1, titleResults.getInt("id"));
			ResultSet metaData = getIndexedMetaData.executeQuery();
			if(metaData.next()) {
				getExternalFormats.setInt(1, titleResults.getInt("id"));
				ResultSet formats = getExternalFormats.executeQuery();
				
				curRecord.setTitle(metaData.getString("title"));
				curRecord.setSortTitle(metaData.getString("title_sort"));
				curRecord.setSeries(metaData.getString("series"));
				//BA+++Author ok for Marc?
				curRecord.setAuthor(metaData.getString("author"));
				
				while ( formats.next() ){
					curRecord.setFormatCategory(formats.getString("formatCategory"));
					curRecord.getFormats().add(formats.getString("formatId"));
				}
				curRecord.setCoverImage(metaData.getString("thumbnail"));
				metaData.close();
				formats.close();
			} else {
				logger.error("No metadata to load");
				return null;
			}
		} catch ( SQLException e ) {
			logger.error("Error loading title " + curRecord.getId() + " from database " + e.toString());
			return null;
		}
		return curRecord;
	}

	//BA++++  MetaData
	private void loadOverDriveMetaData(OverDriveRecordInfo overDriveInfo) {
		logger.debug("Loading metadata for " + overDriveInfo.getId() );
		try {
			getIndexedMetaData.setInt(1, overDriveInfo.getDatabaseId());
			ResultSet data = getIndexedMetaData.executeQuery();
			data.next();
			getExternalFormats.setInt(1, overDriveInfo.getDatabaseId());
			ResultSet format = getExternalFormats.executeQuery();
			
			//logger.debug("Setting up overDriveInfo object");
			overDriveInfo.setEdition(data.getString("edition"));
			overDriveInfo.setPublisher(data.getString("publisher"));
			if( data.getString("publishDate") != null )
			{
				try {
					SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
					Date datePublished = formatter2.parse(data.getString("publishDate").substring(0, 10));
					overDriveInfo.setPublishDate(datePublished.getTime());
				} catch (ParseException e) {
					logger.error("Exception parsing " + data.getString("publishDate"));
				}
			}

			if( data.getString("language") != null )
			{
				JSONArray languages = new JSONArray( data.getString("language") );
				for (int i = 0; i < languages.length(); i++){
					JSONObject language = languages.getJSONObject(i);
					overDriveInfo.getLanguages().add(language.getString("name"));
				}
			}
			//logger.debug("Set languages");
			
			while (format.next()){
				OverDriveItem curItem = new OverDriveItem();
				//logger.debug("Create new overdrive item");
				curItem.setFormatId(format.getString("externalFormatId"));
				//logger.debug("format id " + format.getString("id"));
				curItem.setFormat(format.getString("externalFormatName"));
				//logger.debug("format name " + format.getString("name"));
				curItem.setFormatNumeric(format.getInt("externalFormatNumber"));
				//logger.debug("Numeric format");
				overDriveInfo.getItems().put(curItem.getFormatId(), curItem);
				//logger.debug("Set formats");
			}
				
			format.close();
			data.close();
			//logger.debug("Done setting up overDriveInfo object");	
		} catch (JSONException e) {
			logger.error("Error loading meta data for title " + overDriveInfo.getId() + " " + e.toString());
			results.addNote("Error loading meta data for title " + overDriveInfo.getId() + " " + e.toString());
			results.incErrors();
		} catch (SQLException e) {
			logger.error("Error loading meta data for title " + overDriveInfo.getId() + " " + e.toString());
			results.addNote("Error loading meta data for title " + overDriveInfo.getId() + " " + e.toString());
			results.incErrors();
		}
	}
	
	@Override
	public boolean processMarcRecord(MarcProcessor marcProcessor, MarcRecordDetails recordInfo, int recordStatus, Logger logger) {
		this.marcProcessor = marcProcessor; 
		try {
			results.incRecordsProcessed();
			if (!recordInfo.isEContent()){
				results.incSkipped();
				return false;
			}
			
			//logger.debug("Record is eContent, processing");
			//Record is eContent, get additional details about how to process it.
			HashMap<String, DetectionSettings> detectionSettingsBySource = recordInfo.getEContentDetectionSettings();
			if (detectionSettingsBySource == null || detectionSettingsBySource.size() == 0){
				//error("Record " + recordInfo.getId() + " was tagged as eContent, but we did not get detection settings for it.");
				results.addNote("Record " + recordInfo.getId() + " was tagged as eContent, but we did not get detection settings for it.");
				results.incErrors();
				return false;
			}
			
			for (String source : detectionSettingsBySource.keySet()){
				logger.debug("Record " + recordInfo.getId() + " is eContent, source is " + source);
				DetectionSettings detectionSettings = detectionSettingsBySource.get(source);
				//Generally should only have one source, but in theory there could be multiple sources for a single record
				String accessType = detectionSettings.getAccessType();				
				
				//Check to see if the record already exists
				String ilsId = recordInfo.getId();
				boolean importRecordIntoDatabase = true;
				long eContentRecordId = -1;
				if (ilsId.length() == 0){
					logger.warn("ILS Id could not be found in the marc record, importing.  Running this file multiple times could result in duplicate records in the catalog.");
				}else{
					//Add to database
					importRecordIntoDatabase = true;
				}
				
				boolean recordAdded = false;
				String overDriveId = recordInfo.getExternalId();
				if (overDriveId != null){
					OverDriveRecordInfo overDriveInfo = overDriveTitles.get(overDriveId);
					if (overDriveInfo != null){
						//If we do not have an eContentRecordId already, check to see if there is one based on the 
						//overdrive id
						if (eContentRecordId == -1 && overDriveTitlesWithoutIlsId.containsKey(overDriveId)){
							EcontentRecordInfo eContentRecordInfo = overDriveTitlesWithoutIlsId.get(overDriveId);
							importRecordIntoDatabase = false;
							eContentRecordId = eContentRecordInfo.getRecordId();
						}
					}else{
						logger.debug("Did not find overdrive information for id " + overDriveId);
					}
				}
				if (importRecordIntoDatabase){
					//Add to database
					//BA++++ investigate settings source
					eContentRecordId = updateAPIDataFromMarc(recordInfo, logger, source, accessType, ilsId, eContentRecordId);
					recordAdded = (eContentRecordId != -1);
				}
				
				//logger.debug("Finished initial insertion/update recordAdded = " + recordAdded);
				
				if (recordAdded){
					//addItemsToEContentRecord(recordInfo, logger, source, detectionSettings, eContentRecordId);
				}else{
					logger.debug("Record NOT processed successfully.");
				}
			}
			
			//logger.debug("Finished processing record");
			return true;
		} catch (Exception e) {
			logger.error("Error extracting eContent for record " + recordInfo.getId(), e);
			results.incErrors();
			results.addNote("Error extracting eContent for record " + recordInfo.getId() + " " + e.toString());
			return false;
		}finally{
			if (results.getRecordsProcessed() % 100 == 0){
				results.saveResults();
			}
		}
	}

    /*
	private void addItemsToEContentRecord(MarcRecordDetails recordInfo, Logger logger, String source, DetectionSettings detectionSettings, long eContentRecordId) {
		//Non threaded implementation for adding items
		boolean itemsAdded = true;
		if (source.toLowerCase().startsWith("overdrive")){
			itemsAdded = setupOverDriveItems(recordInfo, eContentRecordId, detectionSettings, logger);
		}else if (detectionSettings.isAdd856FieldsAsExternalLinks()){
			//Automatically setup 856 links as external links
			setupExternalLinks(recordInfo, eContentRecordId, detectionSettings, logger);
		}
		if (itemsAdded){
			logger.debug("Items added successfully.");
			reindexRecord(recordInfo, eContentRecordId, logger);
		};
	}

	protected synchronized void setupExternalLinks(MarcRecordDetails recordInfo, long eContentRecordId, DetectionSettings detectionSettings, Logger logger) {
		//Get existing links from the record
		ArrayList<LinkInfo> allLinks = new ArrayList<LinkInfo>();
		try {
			existingEContentRecordLinks.setLong(1, eContentRecordId);
			ResultSet allExistingUrls = existingEContentRecordLinks.executeQuery();
			while (allExistingUrls.next()){
				LinkInfo curLinkInfo = new LinkInfo();
				curLinkInfo.setItemId(allExistingUrls.getLong("id"));
				curLinkInfo.setLink(allExistingUrls.getString("link"));
				curLinkInfo.setLibraryId(allExistingUrls.getLong("libraryId"));
				curLinkInfo.setItemType(allExistingUrls.getString("item_type"));
				allLinks.add(curLinkInfo);
			}
			allExistingUrls.close();
		} catch (SQLException e) {
			results.incErrors();
			results.addNote("Could not load existing links for eContentRecord " + eContentRecordId);
			return;
		}
		//logger.debug("Found " + allLinks.size() + " existing links");
		
		//Add the links that are currently available for the record
		ArrayList<LibrarySpecificLink> sourceUrls;
		try {
			sourceUrls = recordInfo.getSourceUrls();
		} catch (IOException e1) {
			results.incErrors();
			results.addNote("Could not load source URLs for " + recordInfo.getId() + " " + e1.toString());
			return;
		}
		//logger.debug("Found " + sourceUrls.size() + " urls for " + recordInfo.getId());
		if (sourceUrls.size() == 0){
			results.addNote("Warning, could not find any urls for " + recordInfo.getId() + " source " + detectionSettings.getSource() + " protection type " + detectionSettings.getAccessType());
		}
		for (LibrarySpecificLink curLink : sourceUrls){
			//Look for an existing link
			LinkInfo linkForSourceUrl = null;
			for (LinkInfo tmpLinkInfo : allLinks){
				if (tmpLinkInfo.getLibraryId() == curLink.getLibrarySystemId()){
					linkForSourceUrl = tmpLinkInfo;
				}
			}
			addExternalLink(linkForSourceUrl, curLink, eContentRecordId, detectionSettings, logger);
			if (linkForSourceUrl != null){
				allLinks.remove(linkForSourceUrl);
			}
		}
		
		//Remove any links that no longer exist
		//logger.debug("There are " + allLinks.size() + " links that need to be deleted");
		for (LinkInfo tmpLinkInfo : allLinks){
			try {
				deleteEContentItem.setLong(1, tmpLinkInfo.getItemId());
				deleteEContentItem.executeUpdate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected void reindexRecord(MarcRecordDetails recordInfo, final long eContentRecordId, final Logger logger) {
		//Do direct indexing of the record
		try {
			//String xmlDoc = recordInfo.createXmlDoc();
			getEContentRecordStmt.setLong(1, eContentRecordId);
			ResultSet eContentRecordRS = getEContentRecordStmt.executeQuery();
			getItemsForEContentRecordStmt.setLong(1, eContentRecordId);
			ResultSet eContentItemsRS = getItemsForEContentRecordStmt.executeQuery();
			getAvailabilityForEContentRecordStmt.setLong(1, eContentRecordId);
			ResultSet eContentAvailabilityRS = getAvailabilityForEContentRecordStmt.executeQuery();
			
			SolrInputDocument doc = recordInfo.getEContentSolrDocument(eContentRecordId, eContentRecordRS, eContentItemsRS, eContentAvailabilityRS);
			if (doc != null){
				//Post to the Solr instance
				//logger.debug("Added document to solr");
				updateServer.add(doc);
				//updateServer.add(doc, 60000);
				//results.incAdded();
			}else{
				results.incErrors();
			}
			eContentRecordRS.close();
			eContentItemsRS.close();
			eContentAvailabilityRS.close();
		} catch (Exception e) {
			results.addNote("Error creating xml doc for record " + recordInfo.getId() + " " + e.toString());
			e.printStackTrace();
		}
	}
	*/

	private long updateAPIDataFromMarc(MarcRecordDetails recordInfo, Logger logger, String source, String accessType, String ilsId, long eContentRecordId)
			throws SQLException, IOException {
		logger.debug("Updating ils id " + ilsId + " with marc data.");
		//results.incAdded();
		String overDriveId = recordInfo.getExternalId();
		OverDriveRecordInfo ODrecordInfo = overDriveTitles.get(overDriveId);
		if( ODrecordInfo == null ) {
			logger.debug("No overdrive information for " + overDriveId);
			return -1;
		}
		// update these fields
		ODrecordInfo.setTitle(recordInfo.getTitle());
		ODrecordInfo.setAuthor(recordInfo.getAuthor());
		ODrecordInfo.setDescription(recordInfo.getDescription());
		Collection<String> marcSubjects = recordInfo.getBrowseSubjects(false).values();
		if( marcSubjects.size() > 0) {
			HashSet<String> subjects = new HashSet<String>();
			for (String val:marcSubjects) {
				subjects.add(val.concat(""));
			}
			ODrecordInfo.setSubjects(subjects);
		}
		Object languages = recordInfo.getMappedField("language");
		HashSet<String> vals = new HashSet<String>();
		if( languages instanceof String ) {
			vals.add((String)languages);
		} else {
			@SuppressWarnings("unchecked")
			Set<String> set = (Set<String>) languages;
			for(String val:set) {
				vals.add(val);
			}
		}
		ODrecordInfo.setLanguages(vals);
		ODrecordInfo.setPublisher(Util.trimTo(255, recordInfo.getFirstFieldValueInSet("publisher")));
		ODrecordInfo.setEdition(recordInfo.getFirstFieldValueInSet("edition"));
		//ODrecordInfo.setPublishDate(recordInfo.getFirstFieldValueInSet("publishDate"));
		ODrecordInfo.setSeries(Util.getCRSeparatedString(recordInfo.getMappedField("series")));
		return ODrecordInfo.getDatabaseId();
	}
	
	Pattern overdriveIdPattern = Pattern.compile("[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", Pattern.CANON_EQ);
	protected boolean setupOverDriveItems(MarcRecordDetails recordInfo, long eContentRecordId, DetectionSettings detectionSettings, Logger logger){
		ArrayList<LibrarySpecificLink> sourceUrls;
		try {
			sourceUrls = recordInfo.getSourceUrls();
		} catch (IOException e) {
			results.incErrors();
			results.addNote("Could not load source URLs for overdrive record " + recordInfo.getId() + " " + e.toString());
			return false;
		}
		logger.debug("Found " + sourceUrls.size() + " urls for overdrive id " + recordInfo.getId());
		//Check the items within the record to see if there are any location specific links
		String overDriveId = null;
		for(LibrarySpecificLink link : sourceUrls){
			Matcher RegexMatcher = overdriveIdPattern.matcher(link.getUrl());
			if (RegexMatcher.find()) {
				overDriveId = RegexMatcher.group().toLowerCase();
				break;
			}
		}
		if (overDriveId != null){
			OverDriveRecordInfo overDriveInfo = overDriveTitles.get(overDriveId);
			if (overDriveInfo == null){
				//results.incErrors();
				//results.addNote("Did not find overdrive information for id " + overDriveId + " in information loaded from the API.");
				//logger.debug("Did not find overdrive information for id " + overDriveId + " in information loaded from the API.");
				millenniumRecordsNotInOverDrive.put(overDriveId, recordInfo);
				return false;
			}else{
				//Check to see if we have already processed this id
				if (processedOverDriveRecords.containsKey(overDriveId)){
					ArrayList<String> duplicateRecords;
					if (duplicateOverDriveRecordsInMillennium.containsKey(overDriveId)){
						duplicateRecords = duplicateOverDriveRecordsInMillennium.get(overDriveId);
					}else{
						duplicateRecords = new ArrayList<String>();
						duplicateRecords.add(processedOverDriveRecords.get(overDriveId));
						duplicateOverDriveRecordsInMillennium.put(overDriveId, duplicateRecords);
					}
					duplicateRecords.add(recordInfo.getId());
					return false;
				}else{
					//processedOverDriveRecords.put(overDriveId, overDriveId);
					//overDriveTitles.remove(overDriveId);
					return true;
				}
			}
		}else{
			//results.incErrors();
			recordsWithoutOverDriveId.add(recordInfo.getId());
			//results.addNote("Did not find overdrive id for record " + recordInfo.getId() + " " + eContentRecordId);
			return false;
		}
	}
	
	protected void deleteRecord(long eContentRecordId, Logger logger){
		try {
			updateServer.deleteById("econtentRecord" + eContentRecordId);
		} catch (Exception e) {
			results.addNote("Error deleting for econtentRecord" + eContentRecordId + " " + e.toString());
			results.incErrors();
			e.printStackTrace();
		}
	}

	protected boolean loadConfig(Ini configIni, Logger logger) {
		econtentDBConnectionInfo = Util.cleanIniValue(configIni.get("Database", "database_econtent_jdbc"));
		if (econtentDBConnectionInfo == null || econtentDBConnectionInfo.length() == 0) {
			logger.error("Database connection information for eContent database not found in General Settings.  Please specify connection information in a econtentDatabase key.");
			return false;
		}
		
		vufindUrl = configIni.get("Site", "url");
		if (vufindUrl == null || vufindUrl.length() == 0) {
			logger.error("Unable to get URL for VuFind in General settings.  Please add a vufindUrl key.");
			return false;
		}
		
		return true;		
	}	
	private void addOverDriveTitlesWithoutMarcToIndex(){
		results.addNote("Adding OverDrive titles without marc records to index");
		for (String overDriveId : overDriveTitles.keySet()){
			OverDriveRecordInfo recordInfo = overDriveTitles.get(overDriveId);
			logger.debug("Adding OverDrive record without MARC to index " + recordInfo.getId());
			loadOverDriveMetaData(recordInfo);
			try {
				//Reindex the record
				SolrInputDocument doc = createSolrDocForOverDriveRecord(recordInfo, recordInfo.getDatabaseId());
				updateServer.add(doc);
			} catch (Exception e) {
				logger.error("Error processing eContent record " + overDriveId , e);
				results.incErrors();
				results.addNote("Error processing eContent record " + overDriveId + " " + e.toString());
			}
		}
	}
	
	private SolrInputDocument createSolrDocForOverDriveRecord(OverDriveRecordInfo recordInfo, long econtentRecordId) {
		logger.info("add Solr info for OD record " + econtentRecordId);
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", "econtentRecord" + econtentRecordId);
		
		doc.addField("collection", "Allegheny County Catalog");
		int numHoldings = 0;
		OverDriveAvailabilityInfo curAvailability = recordInfo.getAvailabilityInfo();
		numHoldings += (curAvailability == null) ? 0 : curAvailability.getCopiesOwned();
		doc.addField("institution", "Digital Collection");
		doc.addField("building", "Digital Collection");
		if (curAvailability != null && curAvailability.isAvailable()){
			doc.addField("available_at", "Digital Collection");
		}
		if (recordInfo.getLanguages().size() > 0){
			for (String curLanguage : recordInfo.getLanguages()){
				doc.addField("language", curLanguage);
			}
		}
		
		String firstFormat = null;
		LexileData lexileData = null;
		Set<String> econtentDevices = new HashSet<String>();
		for (OverDriveItem curItem : recordInfo.getItems().values()){
			logger.debug("adding " + curItem.getFormat() + " to " + econtentRecordId);
			doc.addField("format", curItem.getFormat());
			if (firstFormat == null){
				firstFormat = curItem.getFormat().replace(" ", "_");
			}
			
			if (curItem.getIdentifier() != null){
				doc.addField("isbn", curItem.getIdentifier());
				if (lexileData == null){
					String isbn = curItem.getIdentifier();
					if (isbn.indexOf(" ") > 0) {
						isbn = isbn.substring(0, isbn.indexOf(" "));
					}
					if (isbn.length() == 10){
						isbn = Util.convertISBN10to13(isbn);
					}
					if (isbn.length() == 13){
						lexileData = marcProcessor.getLexileDataForIsbn(isbn);
					}
				}
			}
		}
		doc.addField("author", recordInfo.getAuthor());
		for (String curContributor : recordInfo.getContributors()){
			doc.addField("author2", curContributor);
		}
		//BA++  leave ok will return sortTitle
		doc.addField("title", recordInfo.getTitle());
		doc.addField("title_full", recordInfo.getTitle());
		doc.addField("title_sort", recordInfo.getSortTitle());
		for (String curSubject : recordInfo.getSubjects()){
			//doc.addField("subject_facet", curSubject);
			doc.addField("topic", curSubject);
			doc.addField("topic_facet", curSubject);
		}
		doc.addField("publisher", recordInfo.getPublisher());
		if( recordInfo.getPublishDate() != null ) {
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
			String publishDate = formatter2.format(recordInfo.getPublishDate());
			if ( publishDate != null && publishDate.length() > 5 ) {
				publishDate = publishDate.substring(0,4);
			}
			doc.addField("publishDate", publishDate);
			doc.addField("publishDateSort", recordInfo.getPublishDate());
			//BA+++  date_added, time_since_added
			doc.addField("date_added", getDateAdded(recordInfo.getPublishDate()));
			doc.addField("time_since_added", getTimeSinceAddedForDate(new Date(recordInfo.getPublishDate())));
		}
		doc.addField("edition", recordInfo.getEdition());
		doc.addField("description", recordInfo.getDescription());
		doc.addField("series", recordInfo.getSeries());
		//Deal with always available titles by reducing hold count
		if (numHoldings > 1000){
			numHoldings = 5;
		}
		doc.addField("num_holdings", Integer.toString(numHoldings));
		
		if (lexileData != null){
			doc.addField("lexile_score", lexileData.getLexileScore());
			doc.addField("lexile_code", lexileData.getLexileCode());
		}
		for (String curDevice : econtentDevices){
			doc.addField("econtent_device", curDevice);
		}
		doc.addField("externalId", recordInfo.getId());
		doc.addField("econtent_source", "OverDrive");
		doc.addField("econtent_protection_type", "external");
		doc.addField("recordtype", "EContent");
		doc.addField("format_category", recordInfo.getFormatCategory());
		
		if( recordInfo.getCoverImage() != null && recordInfo.getCoverImage() != "") {
			doc.addField("thumbnail", recordInfo.getCoverImage());
		}
		
		Collection<String> allFieldNames = doc.getFieldNames();
		StringBuffer fieldValues = new StringBuffer();
		for (String fieldName : allFieldNames){
			if (fieldValues.length() > 0) fieldValues.append(" ");
			fieldValues.append(doc.getFieldValue(fieldName));
		}
		doc.addField("allfields", fieldValues.toString());
		
		return doc;
	}

	//BA++ delete from db where no OverDrive record
	//Method won't run if config file DeleteERecordsinDBNotinMarcOrOD not set to true
	public int deleteOverDriveTitlesInDb(){
		int ctr = 0;
		results.addNote("Removing Non-OverDrive titles from econtent_record");
		if ( overDriveTitles.size() > 45000 )
		{			
			for (String overDriveId : overDriveIdsFromEContent.keySet()){
				try {
					if ( ! overDriveTitles.containsKey(overDriveId) ){
						removeEContentRecordFromDb(overDriveId);
						ctr++;
					}				
				} catch (Exception e) {
					logger.error("Error processing delete econtent record " + overDriveId , e);
					results.incErrors();
					results.addNote("Error processing econtent record " + overDriveId + " " + e.toString());
				}
			}	
		}
		else {
			logger.info(overDriveTitles.size() +" OverDrive records - less than 45,000");
		}
		return ctr;
	}

	private void removeEContentRecordFromDb(String overDriveId) throws SQLException, IOException {
		try {													
			deleteEContentRecordnotinOverdrive.setString(1, overDriveId);
			deleteEContentRecordnotinOverdrive.executeUpdate();
			logger.debug("Deleted econtent_record by external ID " + overDriveId);
		} catch (SQLException e) {
			logger.error("Unable to delete record - removeEContentRecordFromDb ", e);
		}
		
	}
	
	
	@Override
	public void finish() {
		if (overDriveTitles.size() > 0){
			if ( ReindexProcess.isAddNonMarcODRecords() )
			{
				results.addNote(overDriveTitles.size() + " overdrive titles were found using the OverDrive API but did not have an associated MARC record.");
				results.saveResults();
				addOverDriveTitlesWithoutMarcToIndex();
			}
		}
	
		// dump out the count of updated records
		logger.info("loaded " + (overDriveTitles.size() + processedOverDriveRecords.size()) + " overdrive titles unique in shared collection.");
	
		//Make sure that the index is good and swap indexes
		results.addNote("calling final commit on index");
		logger.info("calling final commit on index");
		
		
		try {
			results.addNote("calling final commit on index");
			
			URLPostResponse response = Util.postToURL("http://localhost:" + solrPort + "/solr/biblio2/update/", "<commit />", logger);
			if (!response.isSuccess()){
				results.incErrors();
				results.addNote("Error committing changes " + response.getMessage());
			}

			results.addNote("optimizing index");
			logger.info("optimizing index");
			try {
				URLPostResponse optimizeResponse = Util.postToURL("http://localhost:" + solrPort + "/solr/biblio2/update/", "<optimize />", logger);
				if (!optimizeResponse.isSuccess()){
					results.addNote("Error optimizing index " + optimizeResponse.getMessage());
				}
			} catch (Exception e) {	
				results.addNote("Error optimizing index");
			}

		} catch (Exception e) {
			results.addNote("Error finalizing index " + e.toString());
			results.incErrors();
			logger.error("Error finalizing index ", e);
		}
		results.saveResults();
		
		//Write millenniumRecordsNotInOverDrive
		try {
			File millenniumRecordsNotInOverDriveFile = new File(localWebDir + "/millenniumRecordsNotInOverDriveFile.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(millenniumRecordsNotInOverDriveFile));
			writer.writeNext(new String[]{"OverDrive ID", "Millennium Record Id", "Title", "Author"});
			for (String overDriveId : millenniumRecordsNotInOverDrive.keySet()){
				MarcRecordDetails curDetails = millenniumRecordsNotInOverDrive.get(overDriveId);
				writer.writeNext(new String[]{overDriveId, curDetails.getId(), curDetails.getTitle(), curDetails.getAuthor()});
			}
			writer.close();
			results.addNote("Report of records that existing in Millennium, but not OverDrive <a href='" + vufindUrl + "/millenniumRecordsNotInOverDriveFile.csv'>millenniumRecordsNotInOverDriveFile.csv</a>");
		} catch (IOException e) {
			results.addNote("Error saving millenniumRecordsNotInOverDriveFile " + e.toString());
			results.incErrors();
			logger.error("Error saving millenniumRecordsNotInOverDriveFile ", e);
		}
		
		//Write duplicateOverDriveRecordsInMillennium
		try {
			File duplicateOverDriveRecordsInMillenniumFile = new File(localWebDir + "/duplicateOverDriveRecordsInMillennium.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(duplicateOverDriveRecordsInMillenniumFile));
			writer.writeNext(new String[]{"OverDrive ID", "Related Records"});
			for (String overDriveId : duplicateOverDriveRecordsInMillennium.keySet()){
				ArrayList<String> relatedRecords = duplicateOverDriveRecordsInMillennium.get(overDriveId);
				StringBuffer relatedRecordsStr = new StringBuffer();
				for (String curRecord: relatedRecords){
					if (relatedRecordsStr.length() > 0){
						relatedRecordsStr.append(";");
					}
					relatedRecordsStr.append(curRecord);
				}
				writer.writeNext(new String[]{overDriveId, relatedRecordsStr.toString()});
			}
			writer.close();
			results.addNote("Report of OverDrive Ids that are linked to by more than one record in Millennium <a href='" + vufindUrl + "/duplicateOverDriveRecordsInMillennium.csv'>duplicateOverDriveRecordsInMillennium.csv</a>");
		} catch (IOException e) {
			results.addNote("Error saving duplicateOverDriveRecordsInMillenniumFile " + e.toString());
			results.incErrors();
			logger.error("Error saving duplicateOverDriveRecordsInMillenniumFile ", e);
		}
		
		//Write report of overdrive ids we don't have MARC record for
		try {
			File overDriveRecordsWithoutMarcsFile = new File(localWebDir + "/OverDriveRecordsWithoutMarcs.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(overDriveRecordsWithoutMarcsFile));
			writer.writeNext(new String[]{"OverDrive ID", "Title", "Author", "Media Type", "Publisher"});
			for (String overDriveId : overDriveTitles.keySet()){
				OverDriveRecordInfo overDriveTitle = overDriveTitles.get(overDriveId);
				writer.writeNext(new String[]{overDriveId, overDriveTitle.getTitle(), overDriveTitle.getAuthor(), overDriveTitle.getMediaType(), overDriveTitle.getPublisher()});
			}
			writer.close();
			results.addNote("Report of OverDrive Titles that we do not have MARC records for <a href='" + vufindUrl + "/OverDriveRecordsWithoutMarcs.csv'>OverDriveRecordsWithoutMarcs.csv</a>");
		} catch (IOException e) {
			results.addNote("Error saving overDriveRecordsWithoutMarcsFile " + e.toString());
			results.incErrors();
			logger.error("Error saving overDriveRecordsWithoutMarcsFile ", e);
		}
		
		//Write a report of marc records that are tagged as overdrive records but do not have an overdrive id in the url
		try {
			File marcsWithoutOverDriveIdFile = new File(localWebDir + "/MarcsWithoutOverDriveId.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(marcsWithoutOverDriveIdFile));
			writer.writeNext(new String[]{"Bib Record"});
			for (String bibId : recordsWithoutOverDriveId){
				writer.writeNext(new String[]{bibId});
			}
			writer.close();
			results.addNote("Report of MARC records that do not have an OverDrive ID <a href='" + vufindUrl + "/MarcsWithoutOverDriveId.csv'>MarcsWithoutOverDriveId.csv</a>");
		} catch (IOException e) {
			results.addNote("Error saving marcsWithoutOverDriveIdFile " + e.toString());
			results.incErrors();
			logger.error("Error saving marcsWithoutOverDriveIdFile ", e);
		}
		
		
		results.addNote("Finished eContent extraction");
		results.saveResults();
	}
	
	@Override
	public ProcessorResults getResults() {
		return results;
	}

	public String getVufindUrl() {
		return vufindUrl;
	}

	public String getDateAdded(Long publishDate) {
			try {
				Date dateAdded = new Date(publishDate);
				SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
				return formatter2.format(dateAdded);
			} catch (Exception ex) {
				// Syntax error in the regular expression
				logger.error("Unable to parse date added for OD nonMarc record " + publishDate);
			}
		return null;
	}
	
	public String getRelativeTimeAdded(String date) {
	
		if (date == null) return null;
	
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		try {
			Date publishDate = formatter2.parse(date);
			return getTimeSinceAddedForDate(publishDate);
		} catch (ParseException e) {
		}
		return null;
	}
	
	public String getTimeSinceAddedForDate(Date curDate) {
		long timeDifferenceDays = (new Date().getTime() - curDate.getTime()) / (1000 * 60 * 60 * 24);
		if (timeDifferenceDays <= 1) {
			return "Day";
		}
		if (timeDifferenceDays <= 7) {
			return "Week";
		}
		if (timeDifferenceDays <= 30) {
			return "Month";
		}
		if (timeDifferenceDays <= 60) {
			return "2 Months";
		}
		if (timeDifferenceDays <= 90) {
			return "Quarter";
		}
		if (timeDifferenceDays <= 180) {
			return "Six Months";
		}
		if (timeDifferenceDays <= 365) {
			return "Year";
		}
		if (timeDifferenceDays > 365) {
			int years = (int)(timeDifferenceDays/365);
			return (years + " Years" );
		}
		return null;
	}
}