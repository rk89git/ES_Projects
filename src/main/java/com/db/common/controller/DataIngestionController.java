package com.db.common.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
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

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.model.ResponseMessage;
import com.db.common.services.DataIngestionService;
import com.db.common.utils.GenericUtils;

@Controller
@RequestMapping("/ingestion")
public class DataIngestionController {

	@Autowired
	private DataIngestionService dataIngestionService;

	private static Logger log = LogManager.getLogger(DataIngestionController.class);

	private ObjectMapper mapper = new ObjectMapper();

	@RequestMapping(method = RequestMethod.POST, value = "/realtimedata")
	@ResponseBody
	public Object ingestDataToKafka(@RequestBody String jsonData) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			map = mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>() {
			});
			dataIngestionService.ingestRealTimeData(map);
			log.info("Record successfully produced.");
			return "Record successfully produced.";
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced: " + map, e);
		}
		return "ERROR: Record could not be produced.";
	}

	@RequestMapping(method = RequestMethod.POST, value = "/readLaterData")
	@ResponseBody
	public Object ingestReadLaterData(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		try {
			log.info("Record Recieved marked as read later: " + jsonData);
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});
			for (Map<String, Object> map : recordList) {
				map.put(Constants.ROWID, map.get(Constants.SESSION_ID_FIELD) + "_" + map.get(Constants.STORY_ID_FIELD));
			}
			dataIngestionService.indexData(Indexes.USER_READ_LATER_DATA, recordList);
			log.info("Records successfully stored.");
			return "Records successfully stored.";
		} catch (JsonParseException e) {
			log.error("ERROR: Record could not be stored  " + recordList, e);
		} catch (JsonMappingException e) {
			log.error("ERROR: Record could not be stored  " + recordList, e);
		} catch (IOException e) {
			log.error("ERROR: Record could not be stored  " + recordList, e);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored  " + recordList, e);
		}
		return "ERROR: Record could not be stored.";
	}

	@RequestMapping(method = RequestMethod.POST, value = "/explicitPersonalizationData")
	@ResponseBody
	public ResponseEntity<Object> ingestExplicitPersonalizationData(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Inserting/updating ExplicitPersonalizationData: " + jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			dataIngestionService.indexExplicitPersonalizationData(Indexes.EXPLICIT_PERSONALIZATION_DATA, record);
			log.info("Records successfully stored/updated for explicit personalization");
			return new ResponseEntity<Object>("Records successfully stored.", status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored in API ExplicitPersonalizationData. " + record, e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/data/{indexName}")
	@ResponseBody
	public ResponseEntity<Object> ingestData(@PathVariable(value = "indexName") String indexName,
			@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			log.info("Inserting/updating Data for Index [" + indexName + "]");
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			dataIngestionService.index(indexName, record);
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

	@RequestMapping(method = RequestMethod.POST, value = "/records/{indexName}")
	@ResponseBody
	public ResponseEntity<Object> indexListofRecords(@PathVariable(value = "indexName") String indexName,
			@RequestBody String jsonData) {
		List<Map<String, Object>> records = new ArrayList<>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			log.info("Inserting/updating Data for Index [" + indexName + "]");
			records = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			dataIngestionService.indexListofRecords(indexName, records);
			responseMessage.setMessage("Records successfully stored.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Records could not be stored for index: " + indexName, e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(
					"ERROR: Records could not be stored for index: " + indexName + ". Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/responseid/{indexName}")
	@ResponseBody
	public ResponseEntity<Object> ingestDataWithResponseId(@PathVariable(value = "indexName") String indexName,
			@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			log.info("Inserting/updating Data for Index [" + indexName + "], Input Data: " + jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			return new ResponseEntity<Object>(dataIngestionService.indexWithResponse(indexName, record), status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored for index: " + indexName, e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(
					"ERROR: Record could not be stored for index: " + indexName + ". Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/produce/{topicName}")
	@ResponseBody
	public ResponseEntity<Object> produceData(@PathVariable(value = "topicName") String topicName,
			@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			log.info("Producing Data for topic [" + topicName + "]");
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			dataIngestionService.produce(topicName, record);
			responseMessage.setMessage("Records successfully produced.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced for index: " + topicName, e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(
					"ERROR: Record could not be produced for index: " + topicName + ". Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/produce")
	@ResponseBody
	public ResponseEntity<Object> produceData(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			log.info("Producing Data, Input Data: " + jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});

			dataIngestionService.produce(record);
			responseMessage.setMessage("Records successfully produced.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced.", e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Record could not be produced. Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/notifyStatus")
	@ResponseBody
	public ResponseEntity<Object> ingestAppNotificationStatus(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;

		try {
			log.info("Inserting/updating ingestAppNotificationStatus: " + jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
			dataIngestionService.ingestAppNotificationStatus(Indexes.APP_USER_PROFILE, record);
			log.info("Record successfully stored/updated for mobile notification.");
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage("Notification status updated successfully.", status), status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored in API ingestAppNotificationStatus. " + record, e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage("ERROR: Notification status couldn't be updated.", status), status);
		}
	}

	// ROHIT SHARMA NOTIFICATION IMPRESSION
	@RequestMapping(method = RequestMethod.POST, value = "/notifyEvent")
	@ResponseBody
	public ResponseEntity<Object> ingestAppNotificationEvent(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		log.info("Notification event API input "+jsonData);
		Map<String, Object> record = new HashMap<String, Object>();
		try {
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
		dataIngestionService.ingestAppNotificationEvent(record);
		return new ResponseEntity<Object>(
				GenericUtils.getResponseMessage("Notification status updated successfully.", status), status);
		} catch (Exception e) {
			log.error("ERROR: Notification Record could not be UPDATE in API notifyEvent. ", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage("ERROR: Notification event couldn't be updated by notifyEvent.", status), status);
		}
	}
	// ROHIT SHARMA NOTIFICATION IMPRESSION

	@RequestMapping(method = RequestMethod.POST, value = "/addEvent")
	@ResponseBody
	public ResponseEntity<Object> ingestEventStatus(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Inserting/updating ingestAppEventStatus: " + jsonData);
			dataIngestionService.ingestEventStatus(jsonData);
			log.info("Record successfully stored/updated for Event.");
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage("Event status updated successfully.", status), status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored in API ingestEventStatus. " + record, e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage("ERROR: Event status couldn't be updated.", status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/identification")
	@ResponseBody
	public ResponseEntity<Object> identification(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		try {
			// log.info("Producing identification data, Input Data: " +
			// jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
			dataIngestionService.identification(record);
			log.info("Record successfully produced for identification.");
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage("Records successfully produced.", status),
					status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"ERROR: Record could not be produced. Caused By: " + e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/recommendation")
	@ResponseBody
	public ResponseEntity<Object> recommendation(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		try {
			// log.info("Producing recommendation data, Input Data: " +
			// jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
			dataIngestionService.recommendation(record);
			log.info("Record successfully produced for recommendation.");
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage("Records successfully produced.", status),
					status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced for recommendation.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"ERROR: Record could not be produced for recommendation. Caused By: " + e.getMessage(), status),
					status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/lotame")
	@ResponseBody
	public ResponseEntity<Object> lotameUserData(@RequestBody String jsonData) {
		Map<String, Object> record = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		String indexName = Indexes.LOTAME_USER_DATA;
		try {
			log.info("Inserting/updating Data for lotame ,index name :  [" + indexName + "], Input Data: " + jsonData);
			record = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
			});
			int num = dataIngestionService.lotaMeIndexWithResponse(indexName, record);
			if (num > 0) {
				responseMessage.setMessage("Records successfully produced for lotame.");
				return new ResponseEntity<Object>(responseMessage, status);
			} else {
				status = HttpStatus.BAD_REQUEST;
				responseMessage.setStatus(status.value());
				responseMessage.setMessage("Records could not successfully produced for lotame.");
				return new ResponseEntity<Object>(responseMessage, status);
			}
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored for lotame for index: " + indexName, e);

			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Record could not be stored for lotame for index: " + indexName
					+ ". Caused By: " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsights")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsData(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			dataIngestionService.facebookInsightsData(recordList);
			String message = recordList.size() + " records of facebook insights data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the facebook insights data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the facebook insights data. Caused By: " + e.getMessage(), status),
					status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/facebookComments")
	@ResponseBody
	public ResponseEntity<Object> facebookComments(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			dataIngestionService.facebookComments(recordList);
			String message = recordList.size() + " records of facebookComments data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the facebookComments data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the facebookComments data. Caused By: " + e.getMessage(), status),
					status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/adMetricsDFP")
	@ResponseBody
	public ResponseEntity<Object> adMetricsDFP(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			int persisted = dataIngestionService.adMetricsDFP(recordList);
			String message = persisted + " records of adMetricsDFP data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the adMetricsDFP data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the adMetricsDFP data. Caused By: " + e.getMessage(), status),
					status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/adMetricsGA")
	@ResponseBody
	public ResponseEntity<Object> adMetricsGA(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			int persisted = dataIngestionService.adMetricsGA(recordList);
			String message = persisted + " records of adMetricsGA data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the adMetricsGA data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the adMetricsGA data. Caused By: " + e.getMessage(), status),
					status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/fbPageData")
	@ResponseBody
	public ResponseEntity<Object> fbPageData(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			int persisted = dataIngestionService.fbPageData(recordList);
			String message = persisted + " records of fbPageData data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the fbPageData data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the fbPageData data. Caused By: " + e.getMessage(), status),
					status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/pollCreationData")
	@ResponseBody
	public ResponseEntity<Object> pollCreationData(@RequestBody String jsonData) {
		Map<String, Object> map = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		try {
			map = mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>() {
			});
			int size = dataIngestionService.pollCreationData(map);
			String message = size + " record of poll creation data has been stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store poll data.", e);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"ERROR: Failed to store poll creation data caused By: " + e.getMessage(), status), status);
		}

	}

	@RequestMapping(method = RequestMethod.POST, value = "/pollPlayData")
	@ResponseBody
	public Object ingestPollPlayDataToKafka(@RequestBody String jsonData) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			map = mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>() {
			});
			dataIngestionService.ingestPollPlayData(map);
			log.info("Poll play record successfully produced.");
			return "Poll play record successfully produced.";
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced for poll play: " + map, e);
		}
		return "ERROR: Record could not be produced for poll play.";
	}

}
