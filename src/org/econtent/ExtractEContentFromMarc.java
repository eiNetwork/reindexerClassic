package org.econtent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
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
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.econtent.GutenbergItemInfo;
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

import au.com.bytecode.opencsv.CSVReader;
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
	
	private HashMap<String, EcontentRecordInfo> existingEcontentIlsIds = new HashMap<String, EcontentRecordInfo>();
	private HashMap<String, EcontentRecordInfo> overDriveTitlesWithoutIlsId = new HashMap<String, EcontentRecordInfo>();
	private HashMap<String, EcontentRecordInfo>overDriveIdsFromEContent = new HashMap<String, EcontentRecordInfo>();
	
	private PreparedStatement createEContentRecord;
	private PreparedStatement updateEContentRecord;
	private PreparedStatement createEContentRecordForOverDrive;
	private PreparedStatement updateEContentRecordForOverDrive;
	private PreparedStatement deleteEContentItem;
	private PreparedStatement deleteEContentRecord;
	
	private PreparedStatement existingEContentRecordLinks;
	private PreparedStatement addSourceUrl;
	private PreparedStatement updateSourceUrl;
	
	private PreparedStatement doesOverDriveItemExist;
	private PreparedStatement addOverDriveItem;
	private PreparedStatement updateOverDriveItem;
	
	private PreparedStatement getEContentRecordStmt;
	private PreparedStatement getItemsForEContentRecordStmt;
	private PreparedStatement getAvailabilityForEContentRecordStmt;
	
	private PreparedStatement doesOverDriveAvailabilityExist;
	private PreparedStatement addOverDriveAvailability;
	private PreparedStatement updateOverDriveAvailability;

	private PreparedStatement getAllOverDriveTitles;
	private PreparedStatement getExternalData;
	private PreparedStatement getIndexedMetaData;
	private PreparedStatement getExternalFormats;
	private PreparedStatement getMarcMap;
	private PreparedStatement updateExternalDataIDs;
	
	// BA++ Setup additional statements to delete records from db not in OverDrive
	private PreparedStatement					deleteEContentRecordnotinOverdrive;
	
	public ProcessorResults results;
	
	private HashMap<String, OverDriveRecordInfo> overDriveTitles = new HashMap<String, OverDriveRecordInfo>();
	
	private HashMap<String, String> processedOverDriveRecords = new HashMap<String, String>();
	private HashMap<String, ArrayList<String>> duplicateOverDriveRecordsInMillennium = new HashMap<String, ArrayList<String>>();
	private HashMap<String, MarcRecordDetails> millenniumRecordsNotInOverDrive = new HashMap<String, MarcRecordDetails>();
	private HashSet<String> recordsWithoutOverDriveId = new HashSet<String>(); 

	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Connection econtentConn, Logger logger) {
		return init(configIni, serverName, reindexLogId, vufindConn, econtentConn, null, logger);
	}
	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Connection econtentConn, Connection reindexerConn, Logger logger) {
		this.logger = logger;
		//Import a marc record into the eContent core. 
		if (!loadConfig(configIni, logger)){
			return false;
		}
		results = new ProcessorResults("Extract eContent from ILS", reindexLogId, vufindConn, logger);
		solrPort = configIni.get("Reindex", "solrPort");
		
		localWebDir = configIni.get("Site", "local");
		
		//Initialize the updateServer
		try {
			updateServer = new ConcurrentUpdateSolrServer("http://localhost:" + solrPort + "/solr/econtent2", 500, 10);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
			URLPostResponse response = Util.postToURL("http://localhost:" + solrPort + "/solr/econtent2/update/?commit=true", "<delete><query>recordtype:econtentRecord</query></delete>", logger);
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
			//Connect to the vufind database
			//BA+++ Author initial load not sort type
			//author removed from update - will be updated to sort author from OverDrive when non-MARC records are processed
			//still done in create
			createEContentRecord = econtentConn.prepareStatement("INSERT INTO econtent_record (ilsId, cover, source, title, subTitle, author, author2, description, contents, subject, language, publisher, publishLocation, physicalDescription, edition, isbn, issn, upc, lccn, topic, genre, region, era, target_audience, sourceUrl, purchaseUrl, publishDate, marcControlField, accessType, date_added, marcRecord, externalId, itemLevelOwnership, series) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
			updateEContentRecord = econtentConn.prepareStatement("UPDATE econtent_record SET ilsId = ?, cover = ?, source = ?, title = ?, subTitle = ?, author2 = ?, description = ?, contents = ?, subject = ?, language = ?, publisher = ?, publishLocation = ?, physicalDescription = ?, edition = ?, isbn = ?, issn = ?, upc = ?, lccn = ?, topic = ?, genre = ?, region = ?, era = ?, target_audience = ?, sourceUrl = ?, purchaseUrl = ?, publishDate = ?, marcControlField = ?, accessType = ?, date_updated = ?, marcRecord = ?, externalId = ?, itemLevelOwnership = ?, series = ?, status = 'active' WHERE id = ?");
			
			createEContentRecordForOverDrive = econtentConn.prepareStatement("INSERT INTO econtent_record (ilsId, cover, source, title, author, author2, description, subject, language, publisher, edition, isbn, publishDate, accessType, date_added, externalId, itemLevelOwnership, series) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
			updateEContentRecordForOverDrive = econtentConn.prepareStatement("UPDATE econtent_record SET ilsId = NULL, cover = ?, source = ?, title = ?, author = ?, author2 = ?, description = ?, subject = ?, language = ?, publisher = ?, edition = ?, isbn = ?, publishDate = ?, accessType = ?, date_updated = ?, externalId = ?, itemLevelOwnership = ?, series = ?, status = 'active' WHERE id = ?");
			
			deleteEContentRecord = econtentConn.prepareStatement("UPDATE econtent_record set status = 'deleted' where id = ?");
			deleteEContentItem = econtentConn.prepareStatement("DELETE FROM econtent_item where id = ?");
			
			existingEContentRecordLinks = econtentConn.prepareStatement("SELECT id, link, libraryId, item_type from econtent_item WHERE recordId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			addSourceUrl = econtentConn.prepareStatement("INSERT INTO econtent_item (recordId, item_type, notes, link, date_added, addedBy, date_updated, libraryId) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
			updateSourceUrl = econtentConn.prepareStatement("UPDATE econtent_item SET link = ?, date_updated = ?, item_type = ?, notes = ? WHERE id = ?");
			
			doesOverDriveItemExist =  econtentConn.prepareStatement("SELECT id from econtent_item WHERE recordId = ? AND externalFormatId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			addOverDriveItem = econtentConn.prepareStatement("INSERT INTO econtent_item (recordId, item_type, externalFormat, externalFormatId, externalFormatNumeric, identifier, sampleName_1, sampleUrl_1, sampleName_2, sampleUrl_2, date_added, addedBy, date_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			updateOverDriveItem = econtentConn.prepareStatement("UPDATE econtent_item SET externalFormat = ?, externalFormatId = ?, externalFormatNumeric = ?, identifier = ?, sampleName_1 = ?, sampleUrl_1 = ?, sampleName_2 = ?, sampleUrl_2 = ?, date_updated =? WHERE id = ?");
			
			doesOverDriveAvailabilityExist = econtentConn.prepareStatement("SELECT id from econtent_availability where recordId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			addOverDriveAvailability = econtentConn.prepareStatement("INSERT INTO econtent_availability (recordId, copiesOwned, availableCopies, numberOfHolds, libraryId) VALUES (?, ?, ?, ?, ?)");
			updateOverDriveAvailability = econtentConn.prepareStatement("UPDATE econtent_availability SET copiesOwned = ?, availableCopies = ?, numberOfHolds = ? WHERE id = ?");
			
			getEContentRecordStmt = econtentConn.prepareStatement("SELECT * FROM econtent_record WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getItemsForEContentRecordStmt = econtentConn.prepareStatement("SELECT * FROM econtent_item WHERE recordId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getAvailabilityForEContentRecordStmt= econtentConn.prepareStatement("SELECT * FROM econtent_availability WHERE recordId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			getAllOverDriveTitles = reindexerConn.prepareStatement("SELECT * FROM externalData", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getExternalData = reindexerConn.prepareStatement("SELECT * FROM externalData WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getIndexedMetaData = reindexerConn.prepareStatement("SELECT * FROM indexedMetaData WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getExternalFormats = reindexerConn.prepareStatement("SELECT externalFormatId, externalDataId, formatId, formatLink, dateAdded, dateUpdated, sourceId, externalFormatId, externalFormatName, externalFormatNumber, displayFormat FROM externalFormats JOIN format ON (externalFormats.formatId=format.id) WHERE externalFormats.externalDataId = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getMarcMap = reindexerConn.prepareStatement("SELECT * FROM marcMap WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			updateExternalDataIDs = reindexerConn.prepareStatement("UPDATE externalData SET recordid = ?, ilsid = ? WHERE id = ?");
			
			//BA++set up Prepared Statements to delete records from db not in OverDrive
			deleteEContentRecordnotinOverdrive = econtentConn.prepareStatement("DELETE FROM econtent_record where externalid = ?");
			
			PreparedStatement existingEcontentIlsIdsStmt = econtentConn.prepareStatement("SELECT econtent_record.id, ilsId, status, count(econtent_item.id) as numItems from econtent_item RIGHT join econtent_record on econtent_record.id = recordId GROUP by ilsId", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet existingEcontentIlsIdsRS = existingEcontentIlsIdsStmt.executeQuery();
			while (existingEcontentIlsIdsRS.next()){
				EcontentRecordInfo recordInfo = new EcontentRecordInfo();
				recordInfo.setRecordId(existingEcontentIlsIdsRS.getLong(1));
				recordInfo.setIlsId(existingEcontentIlsIdsRS.getString(2));
				recordInfo.setStatus(existingEcontentIlsIdsRS.getString(3));
				recordInfo.setNumItems(existingEcontentIlsIdsRS.getInt(4));
				existingEcontentIlsIds.put(recordInfo.getIlsId(), recordInfo);
			}
			existingEcontentIlsIdsRS.close();
			
			PreparedStatement overDriveTitlesWithoutIlsIdStmt = econtentConn.prepareStatement("SELECT econtent_record.id, externalId, status, count(econtent_item.id) as numItems from econtent_item RIGHT join econtent_record on econtent_record.id = recordId WHERE externalId is NOT NULL AND ilsId IS NULL GROUP by externalId", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet overDriveTitlesWithoutIlsIdRS = overDriveTitlesWithoutIlsIdStmt.executeQuery();
			while (overDriveTitlesWithoutIlsIdRS.next()){
				EcontentRecordInfo recordInfo = new EcontentRecordInfo();
				recordInfo.setRecordId(overDriveTitlesWithoutIlsIdRS.getLong(1));
				recordInfo.setExternalId(overDriveTitlesWithoutIlsIdRS.getString(2));
				recordInfo.setStatus(overDriveTitlesWithoutIlsIdRS.getString(3));
				recordInfo.setNumItems(overDriveTitlesWithoutIlsIdRS.getInt(4));
				overDriveTitlesWithoutIlsId.put(recordInfo.getExternalId(), recordInfo);
			}
			overDriveTitlesWithoutIlsIdRS.close();
			logger.debug("Found " + overDriveTitlesWithoutIlsId.size() + " records without ilsids in the database.");
			results.addNote("Found " + overDriveTitlesWithoutIlsId.size() + " records without ilsids in the database.");
						
			//BA++ getOverdriveEContentRecordExId
			int ctr = 0;
			PreparedStatement getOverdriveEContentRecordExId = econtentConn.prepareStatement("SELECT econtent_record.id, externalid FROM econtent_record WHERE externalid is not null", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet OverdriveEContentRecordExId = getOverdriveEContentRecordExId.executeQuery();
			while (OverdriveEContentRecordExId.next()){
				EcontentRecordInfo recordInfo = new EcontentRecordInfo();
				recordInfo.setRecordId(OverdriveEContentRecordExId.getLong(1));
				recordInfo.setExternalId(OverdriveEContentRecordExId.getString(2));
				overDriveIdsFromEContent.put(recordInfo.getExternalId(), recordInfo);
				ctr++;
			}
			OverdriveEContentRecordExId.close();
			
			
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
				/*****curRecord.setMediaType(curProduct.getString("mediaType"));/*****/
				curRecord.setSeries(metaData.getString("series"));
				//BA+++Author ok for Marc?
				curRecord.setAuthor(metaData.getString("author"));
				
				while ( formats.next() ){
					curRecord.getFormats().add(formats.getString("formatId"));
				}
				curRecord.setCoverImage(metaData.getString("thumbnail"));
				metaData.close();
				formats.close();
				/*****curRecord.getCollections().add(getLibraryIdForOverDriveAccount(libraryName));/*****/
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

	private void loadOverDriveAvailability(OverDriveRecordInfo overDriveInfo) {
		//logger.debug("Loading availability of " + overDriveInfo.getId() );
		
		try {
			OverDriveAvailabilityInfo availabilityInfo = new OverDriveAvailabilityInfo();

			getExternalData.setInt(1, overDriveInfo.getDatabaseId());
			ResultSet data = getExternalData.executeQuery();
			data.next();
			
			availabilityInfo.setCopiesOwned(data.getInt("totalCopies"));
			availabilityInfo.setAvailableCopies(data.getInt("availableCopies"));
			availabilityInfo.setNumHolds(data.getInt("numberOfHolds"));
			overDriveInfo.setAvailabilityInfo(availabilityInfo);
			data.close();
		} catch (SQLException e) {
			logger.error("Error loading availability for title " + overDriveInfo.getId() + " " + e.toString());
			results.addNote("Error loading availability for title " + overDriveInfo.getId() + " " + e.toString());
			results.incErrors();
		}
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
			//overDriveInfo.setPublishDate(data.getLong("publishDate"));
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
				if (existingEcontentIlsIds.containsKey(recordInfo.getId())){
					//Delete the existing record
					EcontentRecordInfo econtentInfo = existingEcontentIlsIds.get(recordInfo.getId());
					if (econtentInfo.getStatus().equals("active")){
						logger.debug("Record " + recordInfo.getId() + " is no longer eContent, removing");
						deleteEContentRecord.setLong(1, econtentInfo.getRecordId());
						deleteEContentRecord.executeUpdate();
						deleteRecord(econtentInfo.getRecordId(), logger);
						results.incDeleted();
					}else{
						results.incSkipped();
					}
					existingEcontentIlsIds.remove(recordInfo.getId());
				}else{
					//logger.debug("Skipping record, it is not eContent");
					results.incSkipped();
				}
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
				//Make sure that overdrive titles are updated if we need to check availability
				if (source.matches("(?i)^overdrive.*") && checkOverDrive){
					//Overdrive record, force processing to make sure we get updated availability
					//logger.debug("Record is overdrive, forcing reindex to check overdrive availability");
				}else if (recordStatus == MarcProcessor.RECORD_UNCHANGED || recordStatus == MarcProcessor.RECORD_CHANGED_SECONDARY){
					if (extractEContentFromUnchangedRecords){
						//logger.debug("Record is unchanged, but reindex unchanged records is on");
					}else{
						//Check to see if we have items for the record
						if (!existingEcontentIlsIds.containsKey(recordInfo.getId())){
							//logger.debug("Record is unchanged, but the record does not exist in the eContent database.");
						}else{
							EcontentRecordInfo existingRecordInfo = existingEcontentIlsIds.get(recordInfo.getId());
							if (checkOverDrive && existingRecordInfo.getNumItems() == 0){
								existingEcontentIlsIds.remove(recordInfo.getId());
								/**/
								logger.debug("Skipping because the record has no items");
								results.incSkipped();
								/**
								logger.debug("Deleting the record because it has no items");
								removeEContentRecordFromDb(recordInfo.getExternalId());
								results.incDeleted();
								/**/
								return false;
								//logger.debug("Record is unchanged, but there are no items so indexing to try to get items.");
							}else if (!existingRecordInfo.getStatus().equalsIgnoreCase("active")){
								//logger.debug("Record is unchanged, is not active indexing to correct the status.");
							}else{
								existingEcontentIlsIds.remove(recordInfo.getId());
								//logger.debug("Skipping because the record is not changed");
								results.incSkipped();
								return false;
							}
						}
					}
				}
				
				
				//Check to see if the record already exists
				String ilsId = recordInfo.getId();
				boolean importRecordIntoDatabase = true;
				long eContentRecordId = -1;
				if (ilsId.length() == 0){
					logger.warn("ILS Id could not be found in the marc record, importing.  Running this file multiple times could result in duplicate records in the catalog.");
				}else{
					if (existingEcontentIlsIds.containsKey(ilsId)){
						EcontentRecordInfo eContentRecordInfo = existingEcontentIlsIds.get(ilsId);
						//The record already exists, check if it needs to be updated?
						importRecordIntoDatabase = false;
						eContentRecordId = eContentRecordInfo.getRecordId();
						existingEcontentIlsIds.remove(recordInfo.getId());
					}else{
						//Add to database
						importRecordIntoDatabase = true;
					}
				}
				
				boolean recordAdded = false;
				String overDriveId = recordInfo.getExternalId();
				String cover = "";
				if (overDriveId != null){
					OverDriveRecordInfo overDriveInfo = overDriveTitles.get(overDriveId);
					if (overDriveInfo != null){
						cover = overDriveInfo.getCoverImage();
						
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
					eContentRecordId = addEContentRecordToDb(recordInfo, cover, logger, source, accessType, ilsId, eContentRecordId);
					recordAdded = (eContentRecordId != -1);
				}else{
					//Update the record
					recordAdded = updateEContentRecordInDb(recordInfo, cover, logger, source, accessType, ilsId, eContentRecordId, recordAdded);
				}
				
				//logger.debug("Finished initial insertion/update recordAdded = " + recordAdded);
				
				if (recordAdded){
					addItemsToEContentRecord(recordInfo, logger, source, detectionSettings, eContentRecordId);
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

	private boolean updateEContentRecordInDb(MarcRecordDetails recordInfo, String cover, Logger logger, String source, String accessType, String ilsId, long eContentRecordId,
			boolean recordUpdated) throws SQLException, IOException {
		int curField = 1;
		//BA+++ Author from primary Creator
		updateEContentRecord.setString(curField++, recordInfo.getId());
		updateEContentRecord.setString(curField++, cover);
		updateEContentRecord.setString(curField++, source);
		updateEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("title_short")));
		updateEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("title_sub")));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("author2")));
		updateEContentRecord.setString(curField++, recordInfo.getDescription());
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("contents")));
		HashMap<String, String> subjects = recordInfo.getBrowseSubjects(false);
		//logger.debug("Found " + subjects.size() + " subjects");
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(subjects.values()));
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("language"));
		updateEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("publisher")));
		updateEContentRecord.setString(curField++, Util.trimTo(100, recordInfo.getPublicationLocation()));
		updateEContentRecord.setString(curField++, Util.trimTo(100, recordInfo.getEContentPhysicalDescription()));
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("edition"));
		updateEContentRecord.setString(curField++, Util.trimTo(500, Util.getCRSeparatedString(recordInfo.getMappedField("isbn"))));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("issn")));
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("upc"));
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("lccn"));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("topic_facet")));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("genre")));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("geographic")));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("era")));
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("target_audience")));
		String sourceUrl = "";
		if (recordInfo.getSourceUrls().size() == 1){
			sourceUrl = recordInfo.getSourceUrls().get(0).getUrl();
		}
		updateEContentRecord.setString(curField++, sourceUrl);
		updateEContentRecord.setString(curField++, recordInfo.getPurchaseUrl());
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("publishDate"));
		updateEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("ctrlnum"));
		updateEContentRecord.setString(curField++, accessType);
		updateEContentRecord.setLong(curField++, new Date().getTime() / 1000);
		updateEContentRecord.setString(curField++, recordInfo.toString());
		updateEContentRecord.setString(curField++, recordInfo.getExternalId());
		updateEContentRecord.setInt(curField++, recordInfo.hasItemLevelOwnership());
		updateEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("series")));
		updateEContentRecord.setLong(curField++, eContentRecordId);
		//BA++ 1 = successful update   else = no update/failed
	
		int rowUpdated = updateEContentRecord.executeUpdate();
		if (rowUpdated != 0){
			logger.debug("Record updated " + eContentRecordId + " for id " + ilsId + " in the database.");
			recordUpdated = true;
			results.incUpdated();
			results.addNote("Record updated " + eContentRecordId + " for id " + ilsId);
		}else{
			logger.debug("Could not update record " + eContentRecordId + " for id " + ilsId);
			recordUpdated = false;
			results.incErrors();
			results.addNote("Could not update record " + eContentRecordId + " for id " + ilsId);
		}		
		return recordUpdated;
	}

	private long addEContentRecordToDb(MarcRecordDetails recordInfo, String cover, Logger logger, String source, String accessType, String ilsId, long eContentRecordId)
			throws SQLException, IOException {
		logger.debug("Adding ils id " + ilsId + " to the econtent database.");
		int curField = 1;
		createEContentRecord.setString(curField++, recordInfo.getId());
		createEContentRecord.setString(curField++, cover);
		createEContentRecord.setString(curField++, source);
		createEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("title_short")));
		createEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("title_sub")));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("author"));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("author2")));
		createEContentRecord.setString(curField++, recordInfo.getDescription());
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("contents")));
		HashMap<String, String> subjects = recordInfo.getBrowseSubjects(false);
		//logger.debug("Found " + subjects.size() + " subjects");
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(subjects.values()));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("language"));
		createEContentRecord.setString(curField++, Util.trimTo(255, recordInfo.getFirstFieldValueInSet("publisher")));
		createEContentRecord.setString(curField++, Util.trimTo(100, recordInfo.getPublicationLocation()));
		createEContentRecord.setString(curField++, Util.trimTo(100, recordInfo.getEContentPhysicalDescription()));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("edition"));
		createEContentRecord.setString(curField++, Util.trimTo(500, Util.getCRSeparatedString(recordInfo.getMappedField("isbn"))));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("issn")));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("language"));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("lccn"));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("topic_facet")));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("genre")));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("geographic")));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("era")));
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("target_audience")));
		String sourceUrl = "";
		if (recordInfo.getSourceUrls().size() == 1){
			sourceUrl = recordInfo.getSourceUrls().get(0).getUrl();
		}
		createEContentRecord.setString(curField++, sourceUrl);
		createEContentRecord.setString(curField++, recordInfo.getPurchaseUrl());
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("publishDate"));
		createEContentRecord.setString(curField++, recordInfo.getFirstFieldValueInSet("ctrlnum"));
		createEContentRecord.setString(curField++, accessType);
		createEContentRecord.setLong(curField++, new Date().getTime() / 1000);
		createEContentRecord.setString(curField++, recordInfo.toString());
		createEContentRecord.setString(curField++, recordInfo.getExternalId());
		createEContentRecord.setInt(curField++, recordInfo.hasItemLevelOwnership());
		createEContentRecord.setString(curField++, Util.getCRSeparatedString(recordInfo.getMappedField("series")));
		int rowsInserted = createEContentRecord.executeUpdate();
		if (rowsInserted != 1){
			logger.error("Could not insert row into the database, rowsInserted was " + rowsInserted);
			results.incErrors();
			results.addNote("Error inserting econtent record for id " + ilsId + " number of rows updated was not 1");
		}else{
			ResultSet generatedKeys = createEContentRecord.getGeneratedKeys();
			if (generatedKeys.next()){
				eContentRecordId = generatedKeys.getLong(1);
				results.incAdded();
			}
			generatedKeys.close();
		}
		return eContentRecordId;
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
	
	private void addExternalLink(LinkInfo existingLinkInfo, LibrarySpecificLink linkInfo, long eContentRecordId, DetectionSettings detectionSettings, Logger logger) {
		//Check to see if the link already exists
		try {
			if (existingLinkInfo != null){
				//logger.debug("Updating link " + linkInfo.getUrl() + " libraryId = " + linkInfo.getLibrarySystemId());
				String existingUrlValue = existingLinkInfo.getLink();
				Long existingItemId = existingLinkInfo.getItemId();
				//if (detectionSettings.getSource() == "OneClick") {newItemType = "external_eAudio";}
				String newItemType = detectionSettings.getItem_type();
				if (existingUrlValue == null || !existingUrlValue.equals(linkInfo.getUrl()) || !newItemType.equals(existingLinkInfo.getItemType())){
					//Url does not match, add it to the record. 
					updateSourceUrl.setString(1, linkInfo.getUrl());
					updateSourceUrl.setLong(2, new Date().getTime());
					updateSourceUrl.setString(3, newItemType);
					updateSourceUrl.setString(4, linkInfo.getNotes());
					updateSourceUrl.setLong(5, existingItemId);
					updateSourceUrl.executeUpdate();
				}
			}else{
				//logger.debug("Adding link " + linkInfo.getUrl() + " libraryId = " + linkInfo.getLibrarySystemId());
				//the url does not exist, insert it
				String newItemType = detectionSettings.getItem_type();
				addSourceUrl.setLong(1, eContentRecordId);
				addSourceUrl.setString(2, newItemType);
				addSourceUrl.setString(3, linkInfo.getNotes());
				addSourceUrl.setString(4, linkInfo.getUrl());
				addSourceUrl.setLong(5, new Date().getTime());
				addSourceUrl.setLong(6, -1);
				addSourceUrl.setLong(7, new Date().getTime());
				addSourceUrl.setLong(8, linkInfo.getLibrarySystemId());
				addSourceUrl.executeUpdate();
			}
		} catch (SQLException e) {
			logger.error("Error adding link to record " + eContentRecordId + " " + linkInfo.getUrl(), e);
			results.addNote("Error adding link to record " + eContentRecordId + " " + linkInfo.getUrl() + " " + e.toString());
			results.incErrors();
		}
		
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
		//logger.debug("Found " + sourceUrls.size() + " urls for overdrive id " + recordInfo.getId());
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
					processedOverDriveRecords.put(overDriveId, overDriveId);
					overDriveTitles.remove(overDriveId);
					addOverdriveItemsAndAvailability(overDriveInfo, eContentRecordId);
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
	
	private void addOverdriveItemsAndAvailability(OverDriveRecordInfo overDriveInfo, long eContentRecordId) {
		//Add items
		//logger.debug("Adding overdrive items and availability");
		loadOverDriveMetaData(overDriveInfo);
		logger.debug("loaded meta data, found " + overDriveInfo.getItems().size() + " items.");
		
		// get the list of current formats in case we need to clear any of them
		HashSet<Long> idsToRemove = new HashSet<Long>();
		try {
			existingEContentRecordLinks.setLong(1, eContentRecordId);
			ResultSet rs = existingEContentRecordLinks.executeQuery();			
			while( rs.next() ) {
				idsToRemove.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			logger.debug("SQLException loading current formats: " + e.getMessage());
		}
		
		for (OverDriveItem curItem : overDriveInfo.getItems().values()){
			try {
				//BA+++ Update OverDrive Record - add Author from loadMetaData
				doesOverDriveItemExist.setLong(1, eContentRecordId);
				doesOverDriveItemExist.setString(2, curItem.getFormatId());
				ResultSet existingOverDriveId = doesOverDriveItemExist.executeQuery();
				if (existingOverDriveId.next()){
					Long existingItemId = existingOverDriveId.getLong("id");
					//Url does not match, add it to the record. 
					updateOverDriveItem.setString(1, curItem.getFormat());
					updateOverDriveItem.setString(2, curItem.getFormatId());
					updateOverDriveItem.setLong(3, curItem.getFormatNumeric());
					updateOverDriveItem.setString(4, curItem.getIdentifier());
					updateOverDriveItem.setString(5, curItem.getSampleName_1());
					updateOverDriveItem.setString(6, curItem.getSampleUrl_1());
					updateOverDriveItem.setString(7, curItem.getSampleName_2());
					updateOverDriveItem.setString(8, curItem.getSampleUrl_2());
					updateOverDriveItem.setLong(9, new Date().getTime());
					updateOverDriveItem.setLong(10, existingItemId);
					updateOverDriveItem.executeUpdate();
					
					idsToRemove.remove(existingItemId);
				}else{
					//the url does not exist, insert it
					addOverDriveItem.setLong(1, eContentRecordId);
					addOverDriveItem.setString(2, "overdrive");
					addOverDriveItem.setString(3, curItem.getFormat());
					addOverDriveItem.setString(4, curItem.getFormatId());
					addOverDriveItem.setLong(5, curItem.getFormatNumeric());
					addOverDriveItem.setString(6, curItem.getIdentifier());
					addOverDriveItem.setString(7, curItem.getSampleName_1());
					addOverDriveItem.setString(8, curItem.getSampleUrl_1());
					addOverDriveItem.setString(9, curItem.getSampleName_2());
					addOverDriveItem.setString(10, curItem.getSampleUrl_2());
					addOverDriveItem.setLong(11, new Date().getTime());
					addOverDriveItem.setLong(12, -1);
					addOverDriveItem.setLong(13, new Date().getTime());
					addOverDriveItem.executeUpdate();
				}
				existingOverDriveId.close();
			} catch (SQLException e) {
				logger.error("Error adding item to overdrive record " + eContentRecordId + " " + overDriveInfo.getId(), e);
				results.addNote("Error adding item to overdrive record " + eContentRecordId + " " + overDriveInfo.getId() + " " + e.toString());
				results.incErrors();
			}
		}
		
		// remove formats no longer available on overdrive
		try {
			if( !overDriveInfo.getItems().isEmpty() ) {
				for( Long removeID : idsToRemove) {
					deleteEContentItem.setLong(1, removeID);
					deleteEContentItem.executeUpdate();
				}
			}
		} catch (SQLException e) {
			logger.debug("SQLException removing stale formats: " + e.getMessage());
		}
		
		//BA+++ Add availability
		if (ReindexProcess.isOverDriveAvailabilityAPI()) {
			loadOverDriveAvailability(overDriveInfo);
			OverDriveAvailabilityInfo availabilityInfo = overDriveInfo.getAvailabilityInfo();
			try {
				doesOverDriveAvailabilityExist.setLong(1, eContentRecordId);
				ResultSet availabilityRS = doesOverDriveAvailabilityExist.executeQuery();
				if (availabilityRS.next()){
					long availabilityId = availabilityRS.getLong(1);
					updateOverDriveAvailability.setLong(1, availabilityInfo.getCopiesOwned());
					updateOverDriveAvailability.setLong(2, availabilityInfo.getAvailableCopies());
					updateOverDriveAvailability.setLong(3, availabilityInfo.getNumHolds());
					updateOverDriveAvailability.setLong(4, availabilityId);
					updateOverDriveAvailability.executeUpdate();
				}else{
					addOverDriveAvailability.setLong(1, eContentRecordId);
					addOverDriveAvailability.setLong(2, availabilityInfo.getCopiesOwned());
					addOverDriveAvailability.setLong(3, availabilityInfo.getAvailableCopies());
					addOverDriveAvailability.setLong(4, availabilityInfo.getNumHolds());
					addOverDriveAvailability.setLong(5, availabilityInfo.getLibraryId());
					addOverDriveAvailability.executeUpdate();
				}
				availabilityRS.close();
			} catch (SQLException e) {
				logger.error("Error adding availability to record " + eContentRecordId + " " + overDriveInfo.getId(), e);
				results.addNote("Error adding availability to record " + eContentRecordId + " " + overDriveInfo.getId() + " " + e.toString());
				results.incErrors();
			}
			overDriveInfo.setAvailabilityInfo(availabilityInfo);
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
	//BA++++ Setting --- TAKE OUT COUNTER for test with OD on and marc sample file so only 200 records added for - 
	private void addOverDriveTitlesWithoutMarcToIndex(){
		results.addNote("Adding OverDrive titles without marc records to index");
		//int ctr = 0;
		for (String overDriveId : overDriveTitles.keySet()){
			//if ( ctr < 200 )
			//{
			OverDriveRecordInfo recordInfo = overDriveTitles.get(overDriveId);
			logger.debug("Adding OverDrive record without MARC to index " + recordInfo.getId());
			loadOverDriveMetaData(recordInfo);
			try {
				long econtentRecordId = -1;
				if (overDriveTitlesWithoutIlsId.containsKey(overDriveId)){
					EcontentRecordInfo econtentInfo = overDriveTitlesWithoutIlsId.get(overDriveId);
					econtentRecordId = econtentInfo.getRecordId();
					//We have already added this title before
					updateOverDriveRecordWithoutMarcRecordInDb(recordInfo, econtentRecordId);
					//BA++ setting to only update if add record
					if (econtentRecordId != -1  && ! ReindexProcess.isCallAPIAddOnly()){
						addOverdriveItemsAndAvailability(recordInfo, econtentRecordId);
					}
				} else {
					//New title
					econtentRecordId = addOverDriveRecordWithoutMarcRecordToDb(recordInfo);
					if (econtentRecordId != -1){

						addOverdriveItemsAndAvailability(recordInfo, econtentRecordId);
					}
				}
						
				//Reindex the record
				SolrInputDocument doc = createSolrDocForOverDriveRecord(recordInfo, econtentRecordId);
				updateServer.add(doc);
				
			} catch (Exception e) {
				logger.error("Error processing eContent record " + overDriveId , e);
				results.incErrors();
				results.addNote("Error processing eContent record " + overDriveId + " " + e.toString());
			}
			//ctr++;
			//}
		}
	}
	
	private SolrInputDocument createSolrDocForOverDriveRecord(OverDriveRecordInfo recordInfo, long econtentRecordId) {
		logger.info("add Solr info for OD record " + econtentRecordId);
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", "econtentRecord" + econtentRecordId);
		doc.addField("id_sort", "econtentRecord" + econtentRecordId);
		
		doc.addField("collection", "Allegheny County Catalog");
		int numHoldings = 0;
		OverDriveAvailabilityInfo curAvailability = recordInfo.getAvailabilityInfo();
		numHoldings += (curAvailability == null) ? 0 : curAvailability.getCopiesOwned();
		doc.addField("institution", "Digital Collection");
		doc.addField("building", "Digital Collection");
		if (curAvailability != null && curAvailability.isAvailable()){
			doc.addField("available_at", "Digital Collection");
		}
		doc.addField("collection_group", "Electronic Access");
		if (recordInfo.getLanguages().size() == 0){
			doc.addField("language_boost", "0");
			doc.addField("language_boost_es", "0");
		}else{
			for (String curLanguage : recordInfo.getLanguages()){
				doc.addField("language", curLanguage);
				/***
				if (curLanguage.equalsIgnoreCase("English")){
					doc.addField("language_boost", "300");
					doc.addField("language_boost_es", "0");
				}else if (curLanguage.equalsIgnoreCase("Spanish")){
					doc.addField("language_boost", "0");
					doc.addField("language_boost_es", "300");
				}else{
					doc.addField("language_boost", "0");
					doc.addField("language_boost_es", "0");
				}
				/***/
				if (curLanguage.equalsIgnoreCase("English")){
					doc.addField("language_boost", "300");
				}else if (curLanguage.equalsIgnoreCase("Spanish")){
					doc.addField("language_boost_es", "300");
				}
				/***/
			}
		}
		
		String firstFormat = null;
		LexileData lexileData = null;
		Set<String> econtentDevices = new HashSet<String>();
		for (OverDriveItem curItem : recordInfo.getItems().values()){
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
			/***
			String devicesForFormat = marcProcessor.findMap("device_compatibility_map").get(curItem.getFormat().replace(" ", "_"));
			if (devicesForFormat != null){
				String[] devices = devicesForFormat.split("\\|");
				for (String device : devices){
					econtentDevices.add(device);
				}
			}
			***/
		}
		/***
		if (firstFormat != null){
			doc.addField("format_boost", marcProcessor.findMap("format_boost_map").get(firstFormat));
			doc.addField("format_category", marcProcessor.findMap("format_category_map").get(firstFormat));
		}
		***/
		doc.addField("author", recordInfo.getAuthor());
		for (String curContributor : recordInfo.getContributors()){
			doc.addField("author2", curContributor);
		}
		//BA++  leave ok will return sortTitle
		doc.addField("title", recordInfo.getTitle());
		doc.addField("title_full", recordInfo.getTitle());
		doc.addField("title_sort", recordInfo.getSortTitle());
		for (String curSubject : recordInfo.getSubjects()){
			doc.addField("subject_facet", curSubject);
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
		doc.addField("econtent_source", "OverDrive");
		doc.addField("econtent_protection_type", "external");
		doc.addField("recordtype", "econtentRecord");
		Float rating = null; //***marcProcessor.getEcontentRatings().get(econtentRecordId);
		if (rating == null) {
			rating = -2.5f;
		}
		doc.addField("rating", Float.toString(rating));
		/***
		Set<String> ratingFacets = marcProcessor.getGetRatingFacet(rating);
		for (String ratingFacet : ratingFacets){
			doc.addField("rating_facet", ratingFacet);
		}
		***/
		
		Collection<String> allFieldNames = doc.getFieldNames();
		StringBuffer fieldValues = new StringBuffer();
		for (String fieldName : allFieldNames){
			if (fieldValues.length() > 0) fieldValues.append(" ");
			fieldValues.append(doc.getFieldValue(fieldName));
		}
		doc.addField("allfields", fieldValues.toString());
		doc.addField("keywords", fieldValues.toString());
		
		return doc;
	}

	private void updateOverDriveRecordWithoutMarcRecordInDb(OverDriveRecordInfo recordInfo, long eContentRecordId) throws SQLException, IOException {
		logger.debug("Updating record in db eContentRecordId " + eContentRecordId);
		updateEContentRecordForOverDrive.setString(1, recordInfo.getCoverImage());
		updateEContentRecordForOverDrive.setString(2, "OverDrive");
		updateEContentRecordForOverDrive.setString(3, recordInfo.getTitle());
		updateEContentRecordForOverDrive.setString(4, recordInfo.getAuthor());
		updateEContentRecordForOverDrive.setString(5, Util.getCRSeparatedString(recordInfo.getContributors()));
		updateEContentRecordForOverDrive.setString(6, recordInfo.getDescription());
		updateEContentRecordForOverDrive.setString(7, Util.getCRSeparatedString(recordInfo.getSubjects()));
		updateEContentRecordForOverDrive.setString(8, recordInfo.getLanguages().size() >= 1 ? recordInfo.getLanguages().iterator().next() : "");
		updateEContentRecordForOverDrive.setString(9, Util.trimTo(255, recordInfo.getPublisher()));
		updateEContentRecordForOverDrive.setString(10, recordInfo.getEdition());
		StringBuffer identifiers = new StringBuffer();
		for (OverDriveItem curItem : recordInfo.getItems().values()){
			if (identifiers.length() > 0) identifiers.append("\r\n");
			identifiers.append(curItem.getIdentifier());
		}
		updateEContentRecordForOverDrive.setString(11, identifiers.toString());
		
		//BA+++ publishDate	
		//logger.error("Update Publish Date " + recordInfo.getPublishDate());
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
		String publishDate = (recordInfo.getPublishDate() == null) ? null : formatter2.format(recordInfo.getPublishDate());
		if ( publishDate != null && publishDate.length() > 5 ) {
			publishDate = publishDate.substring(0,4);
		}
		//logger.error("Update Publish Date String " + publishDate);
		updateEContentRecordForOverDrive.setString(12,publishDate);
		//updateEContentRecordForOverDrive.setString(12, recordInfo.getPublishDate());
		
		updateEContentRecordForOverDrive.setString(13, "external");
		updateEContentRecordForOverDrive.setLong(14, new Date().getTime() / 1000);
		updateEContentRecordForOverDrive.setString(15, recordInfo.getId());
		updateEContentRecordForOverDrive.setInt(16, 0);
		updateEContentRecordForOverDrive.setString(17, recordInfo.getSeries());
		updateEContentRecordForOverDrive.setLong(18, eContentRecordId);
		int rowsUpdated = updateEContentRecordForOverDrive.executeUpdate();

		if (rowsUpdated != 1){
			logger.error("Could not update overdrive record " + eContentRecordId + " for id " + recordInfo.getId() + " in the database, number of rows updated was " + rowsUpdated);
			results.incErrors();
			results.addNote("Error updating overdrive econtent record " + eContentRecordId + " for id " + recordInfo.getId() + " number of rows updated was " + rowsUpdated);
		}else{
			results.incUpdated();
		}
	}

	private long addOverDriveRecordWithoutMarcRecordToDb(OverDriveRecordInfo recordInfo) throws SQLException, IOException {
		long eContentRecordId= -1;
		logger.debug("Adding OverDriveRecordWithoutMarcRecord to the database.");
		createEContentRecordForOverDrive.setString(1, recordInfo.getCoverImage());
		createEContentRecordForOverDrive.setString(2, "OverDrive");
		createEContentRecordForOverDrive.setString(3, Util.trimTo(255, recordInfo.getTitle()));
		createEContentRecordForOverDrive.setString(4, recordInfo.getAuthor());
		createEContentRecordForOverDrive.setString(5, Util.getCRSeparatedString(recordInfo.getContributors()));
		createEContentRecordForOverDrive.setString(6, recordInfo.getDescription());
		createEContentRecordForOverDrive.setString(7, Util.getCRSeparatedString(recordInfo.getSubjects()));
		createEContentRecordForOverDrive.setString(8, recordInfo.getLanguages().size() >= 1 ? recordInfo.getLanguages().iterator().next() : "");
		createEContentRecordForOverDrive.setString(9, Util.trimTo(255, recordInfo.getPublisher()));
		createEContentRecordForOverDrive.setString(10, recordInfo.getEdition());
		StringBuffer identifiers = new StringBuffer();
		for (OverDriveItem curItem : recordInfo.getItems().values()){
			if (identifiers.length() > 0) identifiers.append("\r\n");
			identifiers.append(curItem.getIdentifier());
		}
		createEContentRecordForOverDrive.setString(11, identifiers.toString());
		//BA+++ publishDate
		//logger.error("Create Publish Date " + recordInfo.getPublishDate());
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
		String publishDate = (recordInfo.getPublishDate() == null) ? null : formatter2.format(recordInfo.getPublishDate());
		if ( publishDate != null && publishDate.length() > 5 ) {
			publishDate = publishDate.substring(0,4);
		}
		//logger.error("Create Publish Date String " + publishDate);
		createEContentRecordForOverDrive.setString(12,publishDate);
		//createEContentRecordForOverDrive.setString(12, recordInfo.getPublishDate());
		createEContentRecordForOverDrive.setString(13, "external");
		createEContentRecordForOverDrive.setLong(14, new Date().getTime() / 1000);
		createEContentRecordForOverDrive.setString(15, recordInfo.getId());
		createEContentRecordForOverDrive.setInt(16, 0);
		createEContentRecordForOverDrive.setString(17, recordInfo.getSeries());
		int rowsInserted = createEContentRecordForOverDrive.executeUpdate();
		if (rowsInserted != 1){
			logger.error("Could not insert row into the database, rowsInserted was " + rowsInserted);
			results.incErrors();
			results.addNote("Error inserting econtent record for overdrive id " + recordInfo.getId() + " number of rows updated was not 1");
		}else{
			ResultSet generatedKeys = createEContentRecordForOverDrive.getGeneratedKeys();
			if (generatedKeys.next()){
				eContentRecordId = generatedKeys.getLong(1);
				results.incAdded();
			}
		}
		return eContentRecordId;
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

			URLPostResponse response = Util.postToURL("http://localhost:" + solrPort + "/solr/econtent2/update/", "<commit />", logger);
			if (!response.isSuccess()){
				results.incErrors();
				results.addNote("Error committing changes " + response.getMessage());
			}

			results.addNote("optimizing econtent2 index");
			logger.info("optimizing econtent2 index");
			try {
				URLPostResponse optimizeResponse = Util.postToURL("http://localhost:" + solrPort + "/solr/econtent2/update/", "<optimize />", logger);
				if (!optimizeResponse.isSuccess()){
					results.addNote("Error optimizing econtent2 index " + optimizeResponse.getMessage());
				}
			} catch (Exception e) {	
				results.addNote("Error optimizing econtent2 index");
			}
						
			if (checkMarcImport()){
				results.addNote("index passed checks, swapping cores so new index is active.");
				logger.info("index passed checks, swapping cores so new index is active.");
				URLPostResponse postResponse = Util.getURL("http://localhost:" + solrPort + "/solr/admin/cores?action=SWAP&core=econtent2&other=econtent", logger);
				if (!postResponse.isSuccess()){
					results.addNote("Error swapping cores " + postResponse.getMessage());
					logger.info("Error swapping cores " + postResponse.getMessage());					
				}else{
					results.addNote("Result of swapping cores " + postResponse.getMessage());
					logger.info("Result of swapping cores " + postResponse.getMessage());					
				}
			}else{
				results.incErrors();
				results.addNote("index did not pass check, not swapping");
				logger.info("index did not pass check, not swapping");
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
	
	private boolean checkMarcImport() {
		//Do not pass the import if more than 1% of the records have errors 
		if (results.getNumErrors() > results.getRecordsProcessed() * .01){
			return false;
		}else{
			return true;
		}
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