package com.db.comment.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.comment.services.CommentIngestionService;
import com.db.common.constants.Indexes;
import com.db.common.model.ResponseMessage;

@Controller
@RequestMapping("/comment/ingestion")
public class CommentIngestionController {

	@Autowired
	private CommentIngestionService commentIngestionService;

	private static Logger log = LogManager.getLogger(CommentIngestionController.class);

	private ObjectMapper mapper = new ObjectMapper();

	@RequestMapping(method = RequestMethod.POST, value = "/userData")
	@ResponseBody
	public Object ingestCommentingUserData(@RequestBody String jsonData) {
		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {

			log.info("Record Recieved in ingest commenting  User Data: " + jsonData);
			recordList = mapper.readValue(jsonData, new TypeReference<ArrayList<Map<String, Object>>>() {
			});

			commentIngestionService.ingestCommentingUserData(Indexes.DB_USER, recordList);
			log.info("Records successfully stored for user data.");

			responseMessage.setMessage("Records successfully stored for user data.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be stored  " + recordList, e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("Record could not be stored for userData  " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/data")
	@ResponseBody
	public Object ingestCommentDataToKafka(@RequestBody String jsonData) {
		Map<String, Object> map = new HashMap<String, Object>();
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			map = mapper.readValue(jsonData, new TypeReference<HashMap<String, Object>>() {
			});
			String id= commentIngestionService.ingestUsersCommentData(map);
			log.info("Record successfully produced for comment data.");
			responseMessage.setId(id);
			responseMessage.setMessage("Record successfully produced for comment data. ");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("ERROR: Record could not be produced for comment data: " + map, e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Record could not be produced for comment data. " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}
}
