package com.db.wisdom.product.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.controller.DataIngestionController;
import com.db.common.utils.GenericUtils;
import com.db.wisdom.product.services.WisdomProductDataIngestionService;
@Controller
@RequestMapping("/wisdomProduct/ingestion")
public class WisdomProductDataIngestionController {

	private WisdomProductDataIngestionService wisdomProductDataIngestionService = new WisdomProductDataIngestionService();

	private static Logger log = LogManager.getLogger(DataIngestionController.class);
	
	private ObjectMapper mapper = new ObjectMapper();
	
	@RequestMapping(method = RequestMethod.POST, value = "/wisdomRealtimeData")
	@ResponseBody
	public Object ingestWisdomDataToKafka(@RequestBody String jsonData) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			map = mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>() {
			});
			wisdomProductDataIngestionService.ingestWisdomRealTimeData(map);
			log.info("Wisdom Record successfully produced.");
			return "Wisdom Record successfully produced.";
		} catch (Exception e) {
			log.error("ERROR: Wisdom Record could not be produced: " + map, e);
		}
		return "ERROR: Wisdom Record could not be produced.";
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsights")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsData(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		try {
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			wisdomProductDataIngestionService.facebookInsightsData(recordList);
			String message = recordList.size() + " records of wisdom product facebook insights data stored.";
			log.info(message);
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(message, status), status);
		} catch (Exception e) {
			log.error("Failed to store the wisdom facebook insights data.", e);
			return new ResponseEntity<Object>(
					GenericUtils.getResponseMessage(
							"ERROR: Failed to store the wisdom facebook insights data. Caused By: " + e.getMessage(), status),
					status);
		}

	}
	
	
}
