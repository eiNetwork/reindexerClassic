package org.vufind;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.sql.Connection;
import org.apache.log4j.Logger;

public class DeleteDependentRecords {
	
	PreparedStatement deleteResourceRecords;
	PreparedStatement deleteMarcImportRecords;
	PreparedStatement deleteUserResourceRecords; 
	
	Logger logger;
	Connection vufindConn;
	
	public DeleteDependentRecords(Logger logger, Connection vufindConn) {
		try {
			this.logger = logger;
			this.vufindConn = vufindConn;
			
			deleteResourceRecords = vufindConn.prepareStatement("delete from resource where shortid is null and record_id not in (select concat('econtentRecord', id) from reindexer.externalData)");
			deleteMarcImportRecords = vufindConn.prepareStatement("delete from marc_import where id not in (select record_id from resource)");
			deleteUserResourceRecords = vufindConn.prepareStatement("delete from user_resource where resource_id not in (select id from resource)");
			
			
		} catch (Exception e) {
			logger.error("Error processing delete econtent record " , e);
			
		}
	}
	
	public void ExecuteDeletes() {
		int resourceRecordsDeleted = 0;
		int marcImportRecordsDeleted = 0;
		int userResourceRecordsDeleted = 0;
		
		try {
		
			resourceRecordsDeleted = deleteResourceRecords.executeUpdate();
			logger.info("Delete dependent  resource records " + resourceRecordsDeleted);
			marcImportRecordsDeleted = deleteMarcImportRecords.executeUpdate();
			logger.info("Delete dependent marc import records " + marcImportRecordsDeleted);
			userResourceRecordsDeleted = deleteUserResourceRecords.executeUpdate();
			logger.info("Delete dependent  user resource records " + userResourceRecordsDeleted);
			
		} catch (Exception e) {
			logger.error("Error processing delete dependent econtent records " , e);			
		}
		
		
	}
	
	
	};
	