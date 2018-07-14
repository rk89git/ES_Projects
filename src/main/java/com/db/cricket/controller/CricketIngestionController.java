package com.db.cricket.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.model.ResponseMessage;
import com.db.common.services.DataIngestionService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.cricket.model.CricketQuery;
import com.db.cricket.predictnwin.services.PredictNWinIngestionService;
import com.db.cricket.services.CricketIngestionService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Controller
@RequestMapping("/cricket/ingestion")
public class CricketIngestionController {

	@Autowired
	private DataIngestionService dataIngestionService;

	private static Logger log = LogManager.getLogger(CricketIngestionController.class);

	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private ObjectMapper mapper = new ObjectMapper();

	private boolean productionMode = true;

	private static ExecutorService matchRestartService = null;

	public CricketIngestionController() {
		if (StringUtils.isNotBlank(DBConfig.getInstance().getString("production.environment"))) {
			productionMode = Boolean.valueOf(DBConfig.getInstance().getString("production.environment"));
		}
	}

	@Autowired
	private CricketIngestionService cricketIngestionService;

	@RequestMapping(method = RequestMethod.POST, value = "/data/{indexName}/{fileName}")
	@ResponseBody
	public ResponseEntity<Object> ingestData(@PathVariable(value = "indexName") String indexName,
			@PathVariable(value = "fileName") String fileName, @RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			if (!productionMode) {
				writeToFile(fileName, jsonData);
			}

			log.info("Inserting/updating Data for Index [" + indexName + "] and fileName [" + fileName
					+ "] , Input Data: ");
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
			cricketIngestionService.ingestData(indexName, fileName, record);

			responseMessage.setMessage("Records successfully stored.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored for index: " + indexName, e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(
					"ERROR: Record could not be stored for index: " + indexName + ". Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/restartMatch")
	@ResponseBody
	public ResponseEntity<Object> restartMatch(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();

		try {
			final CricketQuery query = gson.fromJson(jsonData, CricketQuery.class);

			if (matchRestartService != null) {
				matchRestartService.shutdownNow();
			}

			log.info("Restarting Match....");
			matchRestartService = Executors.newSingleThreadExecutor();
			matchRestartService.submit(new Runnable() {

				@Override
				public void run() {
					cricketIngestionService.reingestDataFromSystem(query.getCricketDirectory(),
							Long.parseLong(query.getCricketRestartInterval()));
				}
			});

			responseMessage.setMessage("Match Successfully Restarted");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("Match couldn't be started", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Match couldn't be started. " + "Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/stopMatch")
	@ResponseBody
	public ResponseEntity<Object> stopMatch() {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();

		try {
			if (matchRestartService != null) {

				log.info("Stopping Previously restarted match.");
				matchRestartService.shutdownNow();
			}

			responseMessage.setMessage("Match Successfully Stopped");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("Failed to stop running match", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage
					.setMessage("ERROR: Failed to stop currently running match. " + "Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/updateMatchDetails")
	public ResponseEntity<Object> updateMatchDetails(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();

		log.info("Received request for updating match details: " + jsonData);

		try {
			cricketIngestionService.updateMatchDetails(jsonData);

			responseMessage.setMessage("Match details updated successfully.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("Could not update match details.", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Could not update match details. " + "Caused By: " + e.getMessage());

			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/matchControl")
	public ResponseEntity<Object> updateMatchControls(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();

		log.info("Received request for updating match controls: " + jsonData);
		try {
			cricketIngestionService.updateMatchFlags(jsonData);

			responseMessage.setMessage("Match flags updated successfully.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("Could not update match flags.", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Could not update match flags. " + "Caused By: " + e.getMessage());

			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/predictAndWinBidsData")
	@ResponseBody
	public Object ingestPredictAndWinBidsDataToKafka(@RequestBody String jsonData) {
		// Map<String, Object> map = new HashMap<String, Object>();
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});
			log.info("Record successfully produced for predict and win bids." + recordList);
			return dataIngestionService.ingestPredictAndWinBidsData(recordList);

		} catch (Exception e) {
			log.error("ERROR: Record could not be produced for predict and win bids: " + recordList, e);
		}
		return "ERROR: Record could not be produced for predict and win bids.";
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/disabledMatchIds")
	@ResponseBody
	public Object ingestDisabledMatchIds(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		ResponseMessage responseMessage = new ResponseMessage();
		HttpStatus status = HttpStatus.OK;

		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			int records = dataIngestionService.ingestDisabledMatchIds(recordList);

			responseMessage.setMessage("Successfully Updated "+records+" records");

		} catch (Exception e) {
			log.error("ERROR: while updating Match Ids using DISABLED-API" + recordList, e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Could not update match team & playerIDs. " + "Caused By: " + e.getMessage());
		}
		return new ResponseEntity<Object>(responseMessage, status);
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/calcMatchStat")
	@ResponseBody
	public Object calculateMatchStats(@RequestBody String jsonData) {
		ResponseMessage responseMessage = new ResponseMessage();
		HttpStatus status = HttpStatus.OK;

		try {
//			Map<String, Object> matchScoreCard =  mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>(){
//			});
			
			CricketQuery query = gson.fromJson(jsonData, CricketQuery.class);		
			
			PredictNWinIngestionService.calcMatchStatsForSeries(query);	
			responseMessage.setMessage("Successfully Updated Stats");

		} catch (Exception e) {
			log.error("ERROR: while updating Updated Stats", e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Could not update Stats of palyers" + "Caused By: " + e.getMessage());
		}
		return new ResponseEntity<Object>(responseMessage, status);
	}

	/**
	 * Writes incoming data file to the file system. Writes occur only on staging.
	 * 
	 * @param fileName
	 * @param jsonData
	 */
	private void writeToFile(String fileName, String jsonData) {
		String directory = "/opt/cric-data/" + DateUtil.getCurrentDateTime();
		File theDir = new File(directory);
		try {
			if (!theDir.exists()) {
				theDir.mkdirs();
			}
			File file = new File(directory + "/" + fileName);
			FileUtils.writeStringToFile(file, jsonData);

		} catch (Exception e) {
			log.error("Error in writing cricket file.", e);
		}

	}

	public static void shutdown() {

		if (matchRestartService != null) {
			log.info("Shutting Down Match Restart Service.");
			matchRestartService.shutdownNow();
		}
	}

	// public static void main(String[] args) {
	//
	// String filename = "wiin08272016181777_commentary_all_2";
	//
	// System.out.println(filename.split("_")[0].substring(filename.split("_")[0].length()-6,
	// filename.split("_")[0].length()));
	// HashMap<String, Object> main = new HashMap<>();
	// main.put("hello", "World");
	//
	// HashMap<String, Object> sub = new HashMap<>();
	//
	// sub.put("Hi", "freedom");
	//
	// main.putAll(sub);
	//
	// System.out.println(main);
	// }
}
