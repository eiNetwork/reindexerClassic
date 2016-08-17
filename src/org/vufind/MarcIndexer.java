package org.vufind;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.ini4j.Ini;

public class MarcIndexer implements IMarcRecordProcessor, IRecordProcessor {
	private String solrPort;
	private Logger logger;
	private boolean reindexUnchangedRecords;
	private ProcessorResults results;
	private ConcurrentUpdateSolrServer updateServer;
	@Override
	public boolean init(Ini configIni, String serverName, long reindexLogId, Connection vufindConn, Logger logger) {
		this.logger = logger;
		results = new ProcessorResults("Update Solr", reindexLogId, vufindConn, logger);
		solrPort = configIni.get("Reindex", "solrPort");
		
		//Initialize the updateServer
		//try {
			updateServer = new ConcurrentUpdateSolrServer("http://localhost:" + solrPort + "/solr/biblio2", 5000, 10);
		//} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
		
		//Check to see if we should clear the existing index
		String clearMarcRecordsAtStartOfIndexVal = configIni.get("Reindex", "clearMarcRecordsAtStartOfIndex");
		boolean clearMarcRecordsAtStartOfIndex;
		if (clearMarcRecordsAtStartOfIndexVal == null){
			clearMarcRecordsAtStartOfIndex = false;
		}else{
			clearMarcRecordsAtStartOfIndex = Boolean.parseBoolean(clearMarcRecordsAtStartOfIndexVal);
		}
		results.addNote("clearMarcRecordsAtStartOfIndex = " + clearMarcRecordsAtStartOfIndex);
		if (clearMarcRecordsAtStartOfIndex){
			logger.info("Clearing existing marc records from index");
			results.addNote("clearing existing marc records");
			URLPostResponse response = Util.postToURL("http://localhost:" + solrPort + "/solr/biblio2/update/?commit=true", "<delete><query>recordtype:marc</query></delete>", logger);
			if (!response.isSuccess()){
				results.addNote("Error clearing existing marc records " + response.getMessage());
			}
		}
		
		String reindexUnchangedRecordsVal = configIni.get("Reindex", "reindexUnchangedRecords");
		if (reindexUnchangedRecordsVal == null){
			reindexUnchangedRecords = true;
		}else{
			reindexUnchangedRecords = Boolean.parseBoolean(reindexUnchangedRecordsVal);
		}
		results.addNote("reindexUnchangedRecords = " + reindexUnchangedRecords);
		//Make sure that we don't skip unchanged records if we are clearing at the beginning
		if (clearMarcRecordsAtStartOfIndex) reindexUnchangedRecords = true;
		results.saveResults();
		return true;
	}

	@Override
	public void finish() {
		URLPostResponse response;
		results.addNote("optimizing index");
		try {
			response = Util.postToURL("http://localhost:" + solrPort + "/solr/biblio2/update/", "<optimize />", logger);	
			if (!response.isSuccess()){
				results.addNote("Error optimizing biblio2 index " + response.getMessage());
			}
		} catch (Exception e) {	
			results.addNote("Error optimizing biblio2 index");
		}
		
		results.saveResults();
	}

	@Override
	public boolean processMarcRecord(MarcProcessor processor, MarcRecordDetails recordInfo, int recordStatus, Logger logger) {
		try {
			results.incRecordsProcessed();
			if (recordStatus == MarcProcessor.RECORD_UNCHANGED && !reindexUnchangedRecords){
				//logger.info("Skipping record because it hasn't changed");
				results.incSkipped();
				return true;
			}
			
			
			if (!recordInfo.isEContent() && (recordInfo.getExternalId() == null)){
					//Create the XML document for the record
				try {
					//String xmlDoc = recordInfo.createXmlDoc();
					SolrInputDocument doc = recordInfo.getSolrDocument();
					if (doc != null){
						//Post to the Solr instance
						updateServer.add(doc);
						results.incAdded();
						return true;
					}else{
						results.incErrors();
						return false;
					}
				} catch (Exception e) {
					results.addNote("Error creating xml doc for record " + recordInfo.getId() + " " + e.toString());
					logger.error("Error creating xml doc for record " + recordInfo.getId() + " " + e.toString());
					e.printStackTrace();
					return false;
				}
			}else{
				logger.info("Skipping record because it is eContent");
				results.incSkipped();
				return false;
			}
		} catch (Exception ex) {
			// handle any errors
			logger.error("Error indexing marc record " + recordInfo.getId() + " " + ex.toString());
			results.addNote("Error indexing marc record " + recordInfo.getId() + " " + ex.toString());
			results.incErrors();
			return false;
		}finally{
			if (results.getRecordsProcessed() % 100 == 0){
				results.saveResults();
			}
		}
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
}
