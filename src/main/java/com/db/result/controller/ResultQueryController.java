package com.db.result.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.result.service.ResultExecutorService;

@Controller
@RequestMapping("/result/query")
public class ResultQueryController {

	@Autowired
	private ResultExecutorService resultExecutorService;
	
	private static Logger log = LogManager.getLogger(ResultQueryController.class);
	
	@RequestMapping(method = RequestMethod.POST, value = "/getResult")
	@ResponseBody
	public ResponseEntity<Object> getResult(@RequestBody String jsonData) {
		log.info("Received query for getMaxPVSstories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<>(resultExecutorService.getResult(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving result.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<>(e.getMessage(), status);
		}
	}
}
