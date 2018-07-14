package com.db.recommendation.controller;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import com.db.recommendation.services.RecommendationQueryHandler;

@Controller
@RequestMapping("/rec/query")
public class RecommendationQueryController {

	@Autowired
	private RecommendationQueryHandler recommendationQueryHandler;
	
	private static Logger log = LogManager.getLogger(RecommendationQueryController.class);
	
	@RequestMapping(method = RequestMethod.POST, value = "/getArticleRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getArticleRecommendation(@RequestBody String jsonData) {
		log.info("Received query for getArticleRecommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(recommendationQueryHandler.getArticleStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving article recommendation stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getUserRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getUserRecommendation(@RequestBody String jsonData) {
		log.info("Received query for getUserRecommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(recommendationQueryHandler.getUserStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving user recommendation stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getCollaborativeArticles ")
	@ResponseBody
	public ResponseEntity<Object> getCollaborativeRecommendation(@RequestBody String jsonData) {
		log.info("Received query for getUserRecommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(recommendationQueryHandler.getUserStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving user recommendation stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	/**
	 * This API is used for keyword recommendation 
	 * 
	 * @param jsonData
	 * @return recommended stories 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getKeywordRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getKeywordRecommendation(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		log.info("Received query for keyword recommendation: " + jsonData);
		try {
			return new ResponseEntity<Object>(recommendationQueryHandler.getKeywordRecommendation(jsonData), status);
		} catch (Exception e) {
			log.error("Error in getKeywordRecommendation API :" +jsonData);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<>("Faled to get recommended stories.",status);
		}
	}
}
