package org.vufind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.ini4j.Ini;

public class UpdateResourceInformation implements IMarcRecordProcessor, IEContentProcessor, IRecordProcessor{
	private Logger logger;
	private HashMap<String, Long> existingResourceIds = new HashMap<String, Long>();
	private HashMap<String, Long> existingResourceChecksums = new HashMap<String, Long>();
	
	private boolean updateUnchangedResources = false;
	private boolean removeTitlesNotInMarcExport = false;
	
	private PreparedStatement resourceUpdateStmt = null;
	private PreparedStatement resourceInsertStmt = null;
	private PreparedStatement deleteResourceStmt = null;
	
	//Setup prepared statements that we will use
	//private PreparedStatement existingResourceStmt;
	private PreparedStatement addResourceStmt;
	private PreparedStatement updateResourceStmt;
	
	
	//Code related to call numbers
	private HashMap<String, Long> locations;
	private PreparedStatement getLocationsStmt = null;
	private ProcessorResults results;

	//private PreparedStatement	getDistinctRecordIdsStmt;
	private PreparedStatement	getDuplicateResourceIdsStmt;
	private PreparedStatement	getRelatedRecordsStmt;
	
	private PreparedStatement	deleteResoucePermanentStmt;
	private PreparedStatement	deleteResouceCallNumberPermanentStmt;
	private PreparedStatement	deleteResouceSubjectPermanentStmt;

	private PreparedStatement	transferCommentsStmt;
	private PreparedStatement	transferTagsStmt;
	private PreparedStatement	transferRatingsStmt;
	private PreparedStatement	transferReadingHistoryStmt;
	private PreparedStatement	transferUserResourceStmt;

	private PreparedStatement	existingEContentResourceStmt;
	
	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Logger logger) {
		this.logger = logger;
		results = new ProcessorResults("Update Resources", reindexLogId, vufindConn, logger);
		// Load configuration
		String databaseConnectionInfo = Util.cleanIniValue(configIni.get("Database", "database_vufind_jdbc"));
		if (databaseConnectionInfo == null || databaseConnectionInfo.length() == 0) {
			logger.error("Database connection information not found in General Settings.  Please specify connection information in a database key.");
			return false;
		}
		
		String updateUnchangedResourcesVal = configIni.get("Reindex", "updateUnchangedResources");
		if (updateUnchangedResourcesVal != null && updateUnchangedResourcesVal.length() > 0){
			updateUnchangedResources = Boolean.parseBoolean(updateUnchangedResourcesVal);
		}
		results.addNote("Update Unchanged Resources = " + updateUnchangedResources);
		
		String removeTitlesNotInMarcExportVal = configIni.get("Reindex", "removeTitlesNotInMarcExport");
		if (removeTitlesNotInMarcExportVal != null && removeTitlesNotInMarcExportVal.length() > 0){
			removeTitlesNotInMarcExport = Boolean.parseBoolean(removeTitlesNotInMarcExportVal);
		}
		results.addNote("Remove Titles Not In Marc Export = " + removeTitlesNotInMarcExport);
		
		
		try {
			// Setup prepared statements
			resourceUpdateStmt = vufindConn.prepareStatement("UPDATE resource SET title = ?, title_sort = ?, author = ?, isbn = ?, upc = ?, format = ?, format_category = ?, marc_checksum=?, marc = ?, shortId = ?, date_updated=?, deleted=0 WHERE id = ?");
			resourceInsertStmt = vufindConn.prepareStatement("INSERT INTO resource (title, title_sort, author, isbn, upc, format, format_category, record_id, shortId, marc_checksum, marc, source, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)", PreparedStatement.RETURN_GENERATED_KEYS);
			deleteResourceStmt = vufindConn.prepareStatement("UPDATE resource SET deleted = 1 WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
			deleteResoucePermanentStmt = vufindConn.prepareStatement("DELETE from resource where id = ?");
			deleteResouceCallNumberPermanentStmt = vufindConn.prepareStatement("DELETE from resource_callnumber where resourceId = ?");
			deleteResouceSubjectPermanentStmt = vufindConn.prepareStatement("DELETE from resource_subject where resourceId = ?");
			
			getLocationsStmt = vufindConn.prepareStatement("SELECT locationId, code FROM location", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet locationsRS = getLocationsStmt.executeQuery();
			locations = new HashMap<String, Long>();
			while (locationsRS.next()){
				locations.put(locationsRS.getString("code").toLowerCase(),locationsRS.getLong("locationId") );
			}
			locationsRS.close();
			
			//Setup prepared statements that we will use
			//existingResourceStmt = vufindConn.prepareStatement("SELECT id, date_updated, marc_checksum, deleted from resource where record_id = ? and source = 'VuFind'", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			existingEContentResourceStmt = vufindConn.prepareStatement("SELECT id, date_updated, marc_checksum, deleted from resource where record_id = ? and source = 'eContent'", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			addResourceStmt = vufindConn.prepareStatement("INSERT INTO resource (record_id, title, source, author, title_sort, isbn, upc, format, format_category, marc_checksum, date_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			updateResourceStmt = vufindConn.prepareStatement("UPDATE resource SET record_id = ?, title = ?, source = ?, author = ?, title_sort = ?, isbn = ?, upc = ?, format = ?, format_category = ?, marc_checksum = ?, date_updated = ? WHERE id = ?");
			
			//Cleanup duplicate resources
			getDuplicateResourceIdsStmt = vufindConn.prepareStatement("SELECT record_id, count(id) numResources FROM resource group by record_id, source having count(id) > 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			getRelatedRecordsStmt = vufindConn.prepareStatement("SELECT id, deleted FROM resource where record_id = ? and source = 'VuFind'", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			transferCommentsStmt = vufindConn.prepareStatement("UPDATE comments set resource_id = ? where resource_id = ?");
			transferTagsStmt = vufindConn.prepareStatement("UPDATE resource_tags set resource_id = ? where resource_id = ?");
			transferRatingsStmt = vufindConn.prepareStatement("UPDATE user_rating set resourceid = ? where resourceid = ?");
			transferReadingHistoryStmt = vufindConn.prepareStatement("UPDATE user_reading_history set resourceId = ? where resourceId = ?");
			transferUserResourceStmt = vufindConn.prepareStatement("UPDATE user_resource set resource_id = ? where resource_id = ?");
			cleanupDulicateResources();
			
			//Cleanup duplicated print and eContent resources
			
			//Get a list of resources that have already been installed. 
			logger.debug("Loading existing resources");
			results.addNote("Loading existing resources");
			results.saveResults();
			PreparedStatement existingResourceStmt = vufindConn.prepareStatement("SELECT record_id, id, marc_checksum FROM resource where source = 'VuFind'", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet existingResourceRS = existingResourceStmt.executeQuery();
			int numResourcesRead = 0;
			while (existingResourceRS.next()){
				String ilsId = existingResourceRS.getString("record_id");
				existingResourceIds.put(ilsId, existingResourceRS.getLong("id"));
				existingResourceChecksums.put(ilsId, existingResourceRS.getLong("marc_checksum"));
				if (++numResourcesRead % 100000 == 0){
					ReindexProcess.updateLastUpdateTime();
					results.addNote("Read " + numResourcesRead + " resources");
					results.saveResults();
				}
			}
			existingResourceRS.close();

			logger.debug("Finished loading existing resources");
			results.addNote("Finished loading existing resources");
			results.saveResults();
			
		} catch (SQLException ex) {
			// handle any errors
			logger.error("Unable to setup prepared statements", ex);
			return false;
		}
		return true;
		
	}
	
	private void cleanupDulicateResources() {
		try {
			logger.debug("Cleaning up duplicate resources");
			results.addNote("Cleaning up duplicate resources");
			results.saveResults();
			
			//Get a list of the total number of resources 
			ResultSet distinctIdRS = getDuplicateResourceIdsStmt.executeQuery();
			int resourcesProcessed = 0;
			while (distinctIdRS.next()){
				String ilsId = distinctIdRS.getString("record_id");
				getRelatedRecordsStmt.setString(1, ilsId);
				ResultSet relatedRecordsRS = getRelatedRecordsStmt.executeQuery();
				HashMap<Long, Boolean> relatedRecords = new HashMap<Long, Boolean>();
				while (relatedRecordsRS.next()){
					relatedRecords.put(relatedRecordsRS.getLong("id"), relatedRecordsRS.getBoolean("deleted"));
				}
				relatedRecordsRS.close();
				if (relatedRecords.size() > 1){
					//Need to get rid of some records since there should only be one resource for a given ILS Id
					Long firstActiveRecordId = null;
					for (Long curRecordId : relatedRecords.keySet()){
						if (relatedRecords.get(curRecordId) == false){
							firstActiveRecordId = curRecordId;
							break;
						}
					}
					if (firstActiveRecordId == null){
						//All records were deleted, can delete all but the first
						int curIndex = 0;
						for (Long curRecordId : relatedRecords.keySet()){
							if (curIndex != 0){
								//System.out.println("Deleting all resources (except first) for record id " + ilsId);
								logger.debug("Deleting all resources (except first) for record id " + ilsId);
								deleteResourcePermanently(curRecordId);
								
							}
							curIndex++;
						}
					}else{
						//We have an active record
						for (Long curRecordId : relatedRecords.keySet()){
							if (curRecordId != firstActiveRecordId){
								//System.out.println("Transferring user info for " + curRecordId + " to " + firstActiveRecordId + " because it is redundant");
								logger.debug("Transferring user info for " + curRecordId + " to " + firstActiveRecordId + " because it is redundant");
								transferUserInfo(curRecordId, firstActiveRecordId);
								deleteResourcePermanently(curRecordId);
							}
						}
					}
				}
				
				//Check to see if the record is eContent.  If so, make sure there is a resource for the eContent record and delete the record
				//for VuFind
				//TODO: Move records to eContent as needed 
				
				if (++resourcesProcessed % 100000 == 0){
					logger.debug("Processed " + resourcesProcessed + " resources");
					results.addNote("Processed " + resourcesProcessed + " resources");
					results.saveResults();
				}
			}
			distinctIdRS.close();
			
			//Get a list of distinct ids
		} catch (Exception e) {
			logger.error("Error cleaning up duplicate resources", e);
			results.addNote("Cleaning up duplicate resources");
			results.incErrors();
			results.saveResults();
		}
		logger.debug("Done cleaning up duplicate resources");
	}

	private void deleteResourcePermanently(Long curRecordId) {
		try {
			deleteResoucePermanentStmt.setLong(1, curRecordId);
			deleteResoucePermanentStmt.executeUpdate();
			deleteResouceCallNumberPermanentStmt.setLong(1, curRecordId);
			deleteResouceCallNumberPermanentStmt.executeUpdate();
			deleteResouceSubjectPermanentStmt.setLong(1, curRecordId);
			deleteResouceSubjectPermanentStmt.executeUpdate();
			results.incDeleted();
		} catch (SQLException e) {
			logger.error("Error deleting resource permanently " + curRecordId, e);
		}
	}

	private void transferUserInfo(Long idToTransferFrom, Long idToTransferTo) {
		try {
			//Transfer comments
			transferCommentsStmt.setLong(1, idToTransferTo);
			transferCommentsStmt.setLong(2, idToTransferFrom);
			int numCommentsMoved = transferCommentsStmt.executeUpdate();
			if (numCommentsMoved > 0) System.out.println("Moved " + numCommentsMoved + " comments");
			//Transfer tags
			transferTagsStmt.setLong(1, idToTransferTo);
			transferTagsStmt.setLong(2, idToTransferFrom);
			int numTagsMoved = transferTagsStmt.executeUpdate();
			if (numTagsMoved > 0) System.out.println("Moved " + numTagsMoved + " tags");
			//Transfer ratings
			transferRatingsStmt.setLong(1, idToTransferTo);
			transferRatingsStmt.setLong(2, idToTransferFrom);
			int numRatingsMoved = transferRatingsStmt.executeUpdate();
			if (numRatingsMoved > 0) System.out.println("Moved " + numRatingsMoved + " ratings");
			//Transfer reading history
			transferReadingHistoryStmt.setLong(1, idToTransferTo);
			transferReadingHistoryStmt.setLong(2, idToTransferFrom);
			int numReadingHistoryMoved = transferReadingHistoryStmt.executeUpdate();
			if (numReadingHistoryMoved > 0) System.out.println("Moved " + numReadingHistoryMoved + " reading history entries");
			//Transfer User Resource Information 
			transferUserResourceStmt.setLong(1, idToTransferTo);
			transferUserResourceStmt.setLong(2, idToTransferFrom);
			int numUserResourceMoved = transferUserResourceStmt.executeUpdate();
			if (numUserResourceMoved > 0) System.out.println("Moved " + numUserResourceMoved + " user resource (list) entries");
			
			results.incUpdated();
		} catch (SQLException e) {
			logger.error("Error transferring resource info for user from " + idToTransferFrom + " to " + idToTransferTo, e);
		}
	}

	@Override
	public synchronized boolean processMarcRecord(MarcProcessor processor, MarcRecordDetails recordInfo, int recordStatus, Logger logger) {
		Long resourceId = -1L;
		
		//boolean updateSubjectAndCallNumber = true;
		results.incRecordsProcessed();
		try {
			//Get the existing resource if any
			//logger.debug("Checking to see if we have an existing resource for the record.");
			Long existingResourceId = existingResourceIds.get(recordInfo.getId());
			
			if (recordInfo.isEContent()){
				results.incSkipped();
				//logger.debug("Skipping updating resource for record because it is eContent");
				if (existingResourceId != null ){
					//logger.debug("Removing print resource for eContent record" );
					existingResourceIds.remove(recordInfo.getId());
				}
				return true;
			}
			
			Long existingChecksum = existingResourceChecksums.get(recordInfo.getId());
			if ((recordStatus == MarcProcessor.RECORD_UNCHANGED || recordStatus == MarcProcessor.RECORD_CHANGED_SECONDARY) && !updateUnchangedResources){
				boolean updateResource = false; 
				//BasicResourceInfo basicResourceInfo = existingResources.get(recordInfo.getId());
				if (existingChecksum != null && existingChecksum == -1){
					//logger.debug("Forcing resource update because checksum is -1");
					updateResource = true;
				}
				if (!updateResource){
					//logger.debug("Skipping record because it hasn't changed");
					results.incSkipped();
					return true;
				}
			}
		
			//Check to see if we have an existing resource
			//BasicResourceInfo basicResourceInfo = existingResources.get(recordInfo.getId());
			if (existingResourceId != null){
				resourceId = existingResourceId;
				//Remove the resource from the existingResourcesList so 
				//We can determine which resources no longer exist
				existingResourceIds.remove(recordInfo.getId());
				existingResourceChecksums.remove(recordInfo.getId());
				if (updateUnchangedResources || existingChecksum == null || existingChecksum == -1 || (existingChecksum != recordInfo.getChecksum())){
					updateResourceInDb(recordInfo, logger, existingResourceId);
				}else{
					//updateSubjectAndCallNumber = false;
					results.incSkipped();
				}
				

			} else {
				//logger.debug("This is a brand new record, adding to resources table");
				resourceId = addResourceToDb(recordInfo, logger, resourceId);
			}
		} catch (SQLException ex) {
			// handle any errors
			logger.error("Error updating resource for record " + recordInfo.getId() + " " + recordInfo.getTitle(), ex);
			System.out.println("Error updating resource for record " + recordInfo.getId() + " " + recordInfo.getTitle() + " " + ex.toString());
			results.addNote("Error updating resource for record " + recordInfo.getId() + " " + recordInfo.getTitle() + " " + ex.toString());
			results.incErrors();
		}finally{
			if (results.getRecordsProcessed() % 100 == 0){
				results.saveResults();
			}
		}
		return true;
	}

	private Long addResourceToDb(MarcRecordDetails recordInfo, Logger logger, Long resourceId) throws SQLException {
		String author = recordInfo.getAuthor();
		/***** NO_DB
		// Update resource SQL
		resourceInsertStmt.setString(1, Util.trimTo(200, recordInfo.getTitle()));
		resourceInsertStmt.setString(2, Util.trimTo(200, recordInfo.getSortTitle()));
		resourceInsertStmt.setString(3, Util.trimTo(255, author));
		resourceInsertStmt.setString(4, Util.trimTo(13, recordInfo.getIsbn()));
		resourceInsertStmt.setString(5, Util.trimTo(13, recordInfo.getFirstFieldValueInSet("upc")));
		resourceInsertStmt.setString(6, Util.trimTo(50, recordInfo.getFirstFieldValueInSet("format")));
		resourceInsertStmt.setString(7, Util.trimTo(50, recordInfo.getFirstFieldValueInSet("format_category")));
		resourceInsertStmt.setString(8, recordInfo.getId());
		resourceInsertStmt.setString(9, recordInfo.getShortId());
		resourceInsertStmt.setLong(10, recordInfo.getChecksum());
		resourceInsertStmt.setString(11, recordInfo.getRawRecord());
		resourceInsertStmt.setString(12, "VuFind");

		int rowsUpdated = resourceInsertStmt.executeUpdate();
		if (rowsUpdated == 0) {
			logger.debug("Unable to insert record " + recordInfo.getId());
			results.addNote("Unable to insert record " + recordInfo.getId());
			results.incErrors();
		} else {
			results.incAdded();
			//Get the resourceId
			ResultSet insertedResourceIds = resourceInsertStmt.getGeneratedKeys();
			if (insertedResourceIds.next()){
				resourceId = insertedResourceIds.getLong(1);
			}
			insertedResourceIds.close();
		}
		*****/
		results.incAdded();
		return resourceId;
	}

	private void updateResourceInDb(MarcRecordDetails recordInfo, Logger logger, Long resourceId) throws SQLException {
		// Update the existing record
		String title = recordInfo.getTitle();
		String author = recordInfo.getAuthor();
		// Update resource SQL
		resourceUpdateStmt.setString(1, Util.trimTo(200, title));
		resourceUpdateStmt.setString(2, Util.trimTo(200, recordInfo.getSortTitle()));
		resourceUpdateStmt.setString(3, Util.trimTo(255, author));
		resourceUpdateStmt.setString(4, Util.trimTo(13, recordInfo.getIsbn()));
		resourceUpdateStmt.setString(5, Util.trimTo(13, recordInfo.getFirstFieldValueInSet("upc")));
		resourceUpdateStmt.setString(6, Util.trimTo(50, recordInfo.getFirstFieldValueInSet("format")));
		resourceUpdateStmt.setString(7, Util.trimTo(50, recordInfo.getFirstFieldValueInSet("format_category")));
		resourceUpdateStmt.setLong(8, recordInfo.getChecksum());
		resourceUpdateStmt.setString(9, recordInfo.getRawRecord());
		resourceUpdateStmt.setString(10, recordInfo.getShortId());
		resourceUpdateStmt.setLong(11, new Date().getTime() / 1000);
		resourceUpdateStmt.setLong(12, resourceId);
		int rowsUpdated = resourceUpdateStmt.executeUpdate();
		if (rowsUpdated == 0) {
			logger.debug("Unable to update resource for record " + recordInfo.getId() + " " + resourceId);
			results.addNote("Unable to update resource for record " + recordInfo.getId() + " " + resourceId);
			results.incErrors();
		}else{
			results.incUpdated();
		}
	}

	@Override
	public void finish() {
		if (removeTitlesNotInMarcExport){
			if (existingResourceIds.size() > 10000){
				results.addNote("There are " + existingResourceIds.size() + " resources to be deleted, not deleting because something may have gone wrong.");
				results.saveResults();
				return;
			}
			results.addNote("Deleting resources that no longer exist from resources table, there are " + existingResourceIds.size() + " resources to be deleted.");
			results.saveResults();
			
			//Mark any resources that no longer exist as deleted.
			logger.info("Deleting resources that no longer from resources table, there are " + existingResourceIds.size() + " resources to be deleted.");
			int maxResourcesToDelete = 100;
			int numResourcesAdded = 0;
			for (Long resourceId : existingResourceIds.values()){
				try {
					deleteResourceStmt.setLong(++numResourcesAdded, resourceId);
					if (numResourcesAdded == maxResourcesToDelete){
						deleteResourceStmt.executeUpdate();
						numResourcesAdded = 0;
					}
				} catch (SQLException e) {
					logger.error("Unable to delete resources", e);
					break;
				}
				results.incDeleted();
				if (results.getNumDeleted() % 100 == 0){
					results.saveResults();
				}
			}
			if (numResourcesAdded > 0 && numResourcesAdded == maxResourcesToDelete){
				try {
					for (int i = numResourcesAdded + 1; i < maxResourcesToDelete; i++){
						deleteResourceStmt.setLong(i, -1);
					}
					if (numResourcesAdded == maxResourcesToDelete){
						deleteResourceStmt.executeUpdate();
						numResourcesAdded = 0;
					}
				} catch (SQLException e) {
					logger.error("Unable to delete final resources", e);
				}
			}
			results.addNote("Finished deleting resources");
			results.saveResults();
		}
	}

	@Override
	public ProcessorResults getResults() {
		return results;
	}

	@Override
	public boolean processEContentRecord(ResultSet allEContent, long recordStatus) {
		try {
			results.incEContentRecordsProcessed();
			if (recordStatus == MarcProcessor.RECORD_UNCHANGED && !updateUnchangedResources){
				boolean updateResource = false; 
				//BasicResourceInfo basicResourceInfo = existingResources.get(recordInfo.getId());
				if (!updateResource){
					//logger.debug("Skipping record because it hasn't changed");
					results.incSkipped();
					return true;
				}
			}
			String econtentId = allEContent.getString("id");
			
			//Load title information so we have access regardless of 
			String title = allEContent.getString("title");
			String subTitle = allEContent.getString("subTitle");
			if (subTitle.length() > 0){
				title += ": " + subTitle;
			}
			String sortTitle = title.toLowerCase().replaceAll("^(the|an|a|el|la)\\s", "");
			String isbn = allEContent.getString("isbn");
			if (isbn != null){
				if (isbn.indexOf(' ') > 0){
					isbn = isbn.substring(0, isbn.indexOf(' '));
				}
				if (isbn.indexOf("\r") > 0){
					isbn = isbn.substring(0,isbn.indexOf("\r"));
				}
				if (isbn.indexOf("\n") > 0){
					isbn = isbn.substring(0,isbn.indexOf("\n"));
				}
			}
			String upc = allEContent.getString("upc");
			if (upc != null){
				if (upc.indexOf(' ') > 0){
					upc = upc.substring(0, upc.indexOf(' '));
				}
				if (upc.indexOf("\r") > 0){
					upc = upc.substring(0,upc.indexOf("\r"));
				}
				if (upc.indexOf("\n") > 0){
					upc = upc.substring(0,upc.indexOf("\n"));
				}
			}
			//System.out.println("UPC: " + upc);
			
			//Check to see if we have an existing resource
			existingEContentResourceStmt.setString(1, econtentId);
			ResultSet existingResource = existingEContentResourceStmt.executeQuery();
			if (existingResource.next()){
				//Check the date resource was updated and update if it was updated before the record was changed last
				boolean updateResource = false;
				long resourceUpdateTime = existingResource.getLong("date_updated");
				long econtentUpdateTime = allEContent.getLong("date_updated");

				updateResource = true;
				if (updateResource){
					logger.debug("Updating Resource for eContentRecord " + econtentId);
					updateResourceStmt.setString(1, econtentId);
					updateResourceStmt.setString(2, Util.trimTo(255, title));
					updateResourceStmt.setString(3, "eContent");
					updateResourceStmt.setString(4, Util.trimTo(255, allEContent.getString("author")));
					updateResourceStmt.setString(5, Util.trimTo(255, sortTitle));
					updateResourceStmt.setString(6, Util.trimTo(13, isbn));
					updateResourceStmt.setString(7, Util.trimTo(13, upc));
					updateResourceStmt.setString(8, "");
					updateResourceStmt.setString(9, "emedia");
					updateResourceStmt.setLong(10, 0);
					updateResourceStmt.setLong(11, new Date().getTime() / 1000);
					updateResourceStmt.setLong(12, existingResource.getLong("id"));
					
					int numUpdated = updateResourceStmt.executeUpdate();
					if (numUpdated != 1){
						logger.error("Resource not updated for econtent record " + econtentId);
						results.incErrors();
						results.addNote("Resource not updated for econtent record " + econtentId);
					}else{
						results.incUpdated();
					}
				}else{
					logger.debug("Not updating resource for eContentRecord " + econtentId + ", it is already up to date");
					results.incSkipped();
				}
			}else{
				//Insert a new resource
				
				/***** NO_DB 
				addResourceStmt.setString(1, econtentId);
				addResourceStmt.setString(2, Util.trimTo(255, title));
				addResourceStmt.setString(3, "eContent");
				addResourceStmt.setString(4, Util.trimTo(255, allEContent.getString("author")));
				addResourceStmt.setString(5, Util.trimTo(255, sortTitle));
				addResourceStmt.setString(6, Util.trimTo(13, isbn));
				addResourceStmt.setString(7, Util.trimTo(13, upc));
				addResourceStmt.setString(8, "");
				addResourceStmt.setString(9, "emedia");
				addResourceStmt.setLong(10, 0);
				addResourceStmt.setLong(11, new Date().getTime() / 1000);
				int numAdded = addResourceStmt.executeUpdate();
				if (numAdded != 1){
					logger.error("Resource not added for econtent record " + econtentId);
					results.incErrors();
					results.addNote("Resource not added for econtent record");
				}else{
					//BA+ added
					System.out.println("Adding resource for eContentRecord " + econtentId);
					results.incAdded();
				}
				*****/
				System.out.println("Skipping resource for eContentRecord " + econtentId);
				results.incAdded();
			}
			existingResource.close();
			return true;
		} catch (SQLException e) {
			logger.error("Error updating resources for eContent", e);
			results.incErrors();
			results.addNote("Error updating resources for eContent " + e.toString());
			return false;
		}finally{
			if (results.getEContentRecordsProcessed() % 100 == 0){
				results.saveResults();
			}
		}
	}

	@Override
	public boolean processEContentRecord(ResultSet eContentRecord) {
		// TODO Auto-generated method stub
		return false;
	}
}
