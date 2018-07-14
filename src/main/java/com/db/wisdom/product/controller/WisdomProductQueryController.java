package com.db.wisdom.product.controller;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.utils.GenericUtils;
import com.db.wisdom.product.services.WisdomProductQueryHandlerService;
import com.db.wisdom.services.WisdomQueryHandlerService;

/**
 * The Class WisdomQueryController.
 */
@Controller
@RequestMapping("/wisdomProduct/query")
public class WisdomProductQueryController {

	/** The wisdom query handler service. */
	private WisdomProductQueryHandlerService wisdomProductQueryHandlerService = new WisdomProductQueryHandlerService();

	/** The log. */
	private static Logger log = LogManager.getLogger(WisdomProductQueryController.class);

	
	/**
	 * Top stories listing.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/topStoriesListing")
	@ResponseBody
	public ResponseEntity<Object> topStoriesListing(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for topStoryListing" + jsonData);

			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getTopStoryListing(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * authors listing.
	 *
	 * @param jsonData the json data
	 * @return the authors list
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getAuthorsList")
	@ResponseBody
	public ResponseEntity<Object> getAuthorsList(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuthorsList" + jsonData);

			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getAuthorsList(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * top authors listing.
	 *
	 * @param jsonData the json data
	 * @return the top authors list
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getTopAuthorsList")
	@ResponseBody
	public ResponseEntity<Object> getTopAuthorsList(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuthorsList" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getTopAuthorsList(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * authors monthly data.
	 *
	 * @param jsonData the json data
	 * @return authors monthly data 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getAuthorsMonthlyData")
	@ResponseBody
	public ResponseEntity<Object> getAuthorsMonthlyData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuthorsMonthlyData" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getAuthorsMonthlyData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * detail of a story.
	 *
	 * @param jsonData the json data
	 * @return story detail 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getStoryDetail")
	@ResponseBody
	public ResponseEntity<Object> getStoryDetail(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getStoryDetail" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getStoryDetail(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * version wise performance of a story.
	 *
	 * @param jsonData the json data
	 * @return ResponseEntity 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getVersionwisePerformance")
	@ResponseBody
	public ResponseEntity<Object> getVersionwisePerformance(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getVersionwisePerformance" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getVersionwisePerformance(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * performance of a story.
	 *
	 * @param jsonData the json data
	 * @return story performance 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/performanceGraph")
	@ResponseBody
	public ResponseEntity<Object> getStoryPerformance(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getStoryPerformance" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getStoryPerformance(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * tracker wise performance of a story.
	 *
	 * @param jsonData the json data
	 * @return story performance tracker wise
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/trackerwisePerformanceGraph")
	@ResponseBody
	public ResponseEntity<Object> getTrackerwisePerformanceGraph(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTrackerwisePerformanceGraph" + jsonData);
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getTrackerwisePerformanceGraph(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * Slide wise pvs.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/slideWisePvs")
	@ResponseBody
	public ResponseEntity<Object> slideWisePvs(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/slideWisePvs" + jsonData);

			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getSlideWisePvs(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	/**
	 * Story  timeline.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/storyTimeline")
	@ResponseBody
	public ResponseEntity<Object> storyTimeline(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/storyTimeline" + jsonData);			
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getStoryTimeline(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	/**
	 * Facebook insights.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsights")
	@ResponseBody
	public ResponseEntity<Object> facebookInsights(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {			
			log.info("Received query for facebookInsights" + jsonData);			
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getFacebookInsights(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsForDate")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsForDate(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFacebookInsightsForDate" + jsonData);				
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getFacebookInsightsForDate(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsByInterval")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsByInterval(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for facebookInsightsByInterval" + jsonData);			
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getFacebookInsightsByInterval(jsonData), status);
			
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsByIntervalAndCategory")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsByIntervalAndCategory(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for facebookInsightsByIntervalAndCategory" + jsonData);			
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getFacebookInsightsByIntervalAndCategory(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsByCategory")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsByCategory(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for facebookInsightsByCategory" + jsonData);			
			return new ResponseEntity<Object>(wisdomProductQueryHandlerService.getFacebookInsightsByCategory(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
}
