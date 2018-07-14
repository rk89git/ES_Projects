package com.db.elastic.controller;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.utils.GenericUtils;
import com.db.elastic.services.ESExecutorService;

@Controller
@RequestMapping("/elasticrest")
public class ESController {

	@Autowired
	private ESExecutorService esExecutorService;

	private static Logger log = LogManager.getLogger(ESController.class);

	@RequestMapping(method = RequestMethod.POST, value = "/{indexName}/{type}/_search")
	@ResponseBody
	public ResponseEntity<Object> postSearchRequest(@PathVariable(value = "indexName") String indexName,
			@PathVariable(value = "type") String type, @RequestBody String jsonData) {
		log.info("Received query for POST _search request: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(esExecutorService.postSearchRequest(indexName, type, jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in postSearchRequest.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/{indexName}/_search")
	@ResponseBody
	public ResponseEntity<Object> postSearchWithoutTypeRequest(@PathVariable(value = "indexName") String indexName,
			@RequestBody String jsonData) {
		log.info("Received query for POST _search without type request: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(esExecutorService.postSearchWithoutTypeRequest(indexName,jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in postSearchWithoutTypeRequest.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	} 

	@RequestMapping(method = RequestMethod.GET, value = "/{indexName}/{request}")
	@ResponseBody
	public ResponseEntity<Object> getSearchWithoutTypeRequest(@PathVariable(value = "indexName") String indexName,@PathVariable(value = "request") String request) {
		log.info("Received query for GET _search without type request.");
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(esExecutorService.getRequestWithoutType(indexName,request), status);
		} catch (Exception e) {
			log.error("Error occurred in getRequestWithoutType.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/{indexName}/{type}/{request}")
	@ResponseBody
	public ResponseEntity<Object> getRequest(@PathVariable(value = "indexName") String indexName, @PathVariable(value = "type") String type, @PathVariable(value = "request") String request) {
		log.info("Received query for GET _search request.");
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(esExecutorService.getRequest(indexName, type,request), status);
		} catch (Exception e) {
			log.error("Error occurred in getRequest.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.DELETE, value = "/{indexName}/{type}/{id}")
	@ResponseBody
	public ResponseEntity<Object> deleteRequest(@PathVariable(value = "indexName") String indexName, @PathVariable(value = "type") String type, @PathVariable(value = "id") String id) {
		log.info("Received query for DELETE request.");
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(esExecutorService.deleteRequest(indexName, type,id), status);
		} catch (Exception e) {
			log.error("Error occurred in deleteRequest.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

}
