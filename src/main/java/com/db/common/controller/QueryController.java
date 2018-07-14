package com.db.common.controller;

import com.db.common.model.GenericQuery;
import com.db.common.services.QueryHandlerService;
import com.db.common.utils.GenericUtils;
import com.db.nlp.services.NLPParser;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/query")
public class QueryController {

	@Autowired
	private QueryHandlerService queryHandlerService;// = new QueryHandlerService();
	
	NLPParser nlpParser = null; //new NLPParser();

	private static Logger log = LogManager.getLogger(QueryController.class);

	@RequestMapping(method = RequestMethod.GET, value = "/userHistory/{sessionId:.+}")
	@ResponseBody
	public ResponseEntity<Object> getHistoryOfUser(@PathVariable(value = "sessionId") String sessionId) {
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getHistoryOfUser(sessionId), status);
		} catch (Exception e) {
			log.error("Error while retriving history of user.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/userLastVisit/{sessionId:.+}")
	@ResponseBody
	public ResponseEntity<Object> getLastVisitOfUser(@PathVariable(value = "sessionId") String sessionId) {
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getLastVisitOfUser(sessionId), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@Deprecated
	@RequestMapping(method = RequestMethod.GET, value = "/getNotVisitedUsers")
	@ResponseBody
	public ResponseEntity<Object> getNotVisitedUsers(
			@RequestParam(value = "dayCount", required = false) Integer count) {
		System.out.println("Count: " + count);

		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getAbsentVisitors(count), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@Deprecated
	@RequestMapping(method = RequestMethod.POST, value = "/getConditionalUsers")
	@ResponseBody
	public ResponseEntity<Object> getUserAggsCount(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getUserAggsCount(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getUserPersonalizationStories")
	@ResponseBody
	public ResponseEntity<Object> getUserPersonalizationStories(@RequestBody String jsonData) {
		log.info("Received query for getUserPersonalizationStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getUserPersonalizationStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving user personalization stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getWebUserPersonalizationStories")
	@ResponseBody
	public ResponseEntity<Object> getWebUserPersonalizationStories(@RequestBody String jsonData) {
		log.info("Received query for getWebUserPersonalizationStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getWebUserPersonalizationStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving user personalization stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getMostViewedStories")
	@ResponseBody
	public ResponseEntity<Object> getMostViewedArticles(@RequestBody String jsonData) {
		log.info("Received query for getMostViewedStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getMostViewedArticles(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving most viewed articles for given parameters.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/pushNotification")
	@ResponseBody
	public ResponseEntity<Object> pushNotification(@RequestBody String jsonData) {
		log.info("Input data in pushNotification: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getSources")
	@ResponseBody
	public ResponseEntity<Object> getSources(@RequestBody GenericQuery genericQuery) {
		log.info("Input data in getSources: " + genericQuery);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getSources(genericQuery), status);
		} catch (Exception e) {
			log.error("Error occurred while getting list of sources.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getVideoPartners")
	@ResponseBody
	public ResponseEntity<Object> getVideoPartners(@RequestBody GenericQuery genericQuery) {
		log.info("Input data in getVideoPartners: " + genericQuery);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getVideoPartners(genericQuery), status);
		} catch (Exception e) {
			log.error("Error occurred while getting list of sources.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getFollowTags")
	@ResponseBody
	public ResponseEntity<Object> getFollowTags(@RequestBody GenericQuery genericQuery) {
		log.info("Input data in getFollowTags: " + genericQuery);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getFollowTags(genericQuery), status);
		} catch (Exception e) {
			log.error("Error occurred while getting list of tags.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/testPushNotification")
	@ResponseBody
	public ResponseEntity<Object> testPushNotification(@RequestBody String jsonData) {
		log.info("Input data in testPushNotification: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.testPushNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while testing push notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/testBrowserPushNotification")
	@ResponseBody
	public ResponseEntity<Object> testBrowserPushNotification(@RequestBody String jsonData) {
		log.info("Input data in testPushNotification: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.testBrowserPushNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while testing push notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/notificationHistory")
	@ResponseBody
	public ResponseEntity<Object> searchNotificationHistory(@RequestBody String jsonData) {
		log.info("Query request for searchNotificationHistory: " + jsonData);

		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getNotificationHistory(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while searching notifications logs.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = {RequestMethod.POST,RequestMethod.GET}, value = "/getNotificationSubscribers")
	@ResponseBody
	public ResponseEntity<Object> getNotificationSubscribers(@RequestBody String jsonData) {
		log.info("Query request for getNotificationSubscribers: " + jsonData);

		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getNotificationSubscribers(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while getNotificationSubscribers", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/notificationHistory")
	@ResponseBody
	public ResponseEntity<Object> getNotificationHistory() {
		log.info("Query request for getNotificationHistory.");

		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getNotificationHistory("{}"), status);
		} catch (Exception e) {
			log.error("Error occurred while getting notifications logs.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/getCohortGroup")
	@ResponseBody
	public ResponseEntity<Object> getCohortTrackingGroup(@RequestParam(value = "session_id") String sessionId) {
		log.info("Query request for getCohortTrackingGroup: " + sessionId);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getCohortGroup(sessionId), status);
		} catch (Exception e) {
			log.error("Error occurred in getCohortTrackingGroup", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/getWisdomUvsPvs")
	@ResponseBody
	public ResponseEntity<Object> getWisdomUvsPvsStats() {
		log.info("Query request for getWisdomUvsPvs");
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getWisdomUvsPvs(), status);
		} catch (Exception e) {
			log.error("Error occurred in getCohortTrackingGroup", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/userStoryIdHistory/{sessionId:.+}")
	@ResponseBody
	public ResponseEntity<Object> getStoryIdHistoryOfUser(@PathVariable(value = "sessionId") String sessionId,
			@RequestParam(value = "count", required = false) Integer count) {
		HttpStatus status = HttpStatus.OK;
		log.info("Query request for getStoryIdHistoryOfUser. SessionId=" + sessionId + ", count=" + count);
		try {
			return new ResponseEntity<Object>(queryHandlerService.getStoryIdHistoryOfUser(sessionId, count), status);
		} catch (Exception e) {
			log.error("Error while retriving story-id history of user.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/fetch/{indexName}/{type}/{key:.+}")
	@ResponseBody
	public ResponseEntity<Object> getExplicitPersonalizationData(@PathVariable(value = "indexName") String indexName,
			@PathVariable(value = "type") String type, @PathVariable(value = "key") String key) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Getting record from index [" + indexName + "] type [" + type + "] for key " + key);
			return new ResponseEntity<Object>(queryHandlerService.getRecord(indexName, type, key), status);
		} catch (Exception e) {
			log.error("ERROR: Record occured while getting record. ", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@Deprecated
	@RequestMapping(method = RequestMethod.POST, value = "/getMostTrendingStories")
	@ResponseBody
	public ResponseEntity<Object> getMostTrendingStories(@RequestBody String jsonData) {
		log.info("Received query for getMostTrendingStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getMostTrendingStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving most trending stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getTopUCBStories")
	@ResponseBody
	public ResponseEntity<Object> getTopUcbStories(@RequestBody String jsonData) {
		log.info("Received query for getTopUcbStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getTopUCBStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving Top Ucb Stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"ERROR: Failed to retrieve UCB articles. Caused By: " + e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getMicromaxFeedStories")
	@ResponseBody
	public ResponseEntity<Object> getMicromaxFeedStories(@RequestBody String jsonData) {
		log.info("Received query for getMicromaxFeedStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getMicromaxFeedStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving micromax feed stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getVideoRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getVideoRecommendation(@RequestBody String jsonData) {
		log.info("Received query for Video Recommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getVideoRecommendation(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in getVideoRecommendation", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getTimeSpent")
	@ResponseBody
	public ResponseEntity<Object> getTimeSpent(@RequestBody String jsonData) {
		log.info("Received query for getTimeSpent: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getAvgTimeSpent(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in getTimeSpent", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}	
	
	@RequestMapping(method = RequestMethod.POST, value = "/NLPParser")
	@ResponseBody
	public ResponseEntity<Object> parseNLP(@RequestBody String sentence) {
		log.info("Received query for NLPParser: " + sentence);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(nlpParser.parseNLP(sentence), status);
		} catch (Exception e) {
			log.error("Error occurred in getEventList: ", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getAppRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getAppRecommendationArticles(@RequestBody String jsonData) {
		log.info("Received query for getAppRecommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getAppRecommendationArticles(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving app recommendation stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getRealtimePersonalizedRecommendation")
	@ResponseBody
	public ResponseEntity<Object> getRealtimePersonalizedRecommendation(@RequestBody String jsonData) {
		log.info("Received query for HP getRealtimePersonalizedRecommendation: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getRealtimePersonalizedRecommendation(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving HP realtime personalization stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getCityWiseStories")
	@ResponseBody
	public ResponseEntity<Object> getCityWiseStories(@RequestBody String jsonData) {
		log.info("Received query for getCityWiseStories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getCityWiseStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving city wise stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFacebookTrendingArticles")
	@ResponseBody
	public ResponseEntity<Object> getFacebookTrendingArticles(@RequestBody String jsonData) {
		log.info("Received query for getFacebookTrendingArticles: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getFacebookTrendingArticles(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving facebook trending articles.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getTopEODStories")
	@ResponseBody
	public ResponseEntity<Object> getTopEODStories(@RequestBody String jsonData) {
		log.info("Received query to get EOD Stories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getTopEODStories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving Top EOD articles.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/recordsCountByHost")
	@ResponseBody
	public ResponseEntity<Object> getRecordsCountByHost(@RequestBody String jsonData) {
		log.info("Received query for recordsCountByHost: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getRecordsCountByHost(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving recordsCountByHost.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getMaxPVSstories")
	@ResponseBody
	public ResponseEntity<Object> getMaxPVSstories(@RequestBody String jsonData) {
		log.info("Received query for getMaxPVSstories: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(queryHandlerService.getMaxPVSstories(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving getMaxPVSstories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
//	@RequestMapping(method = RequestMethod.GET, value = "/reindexData")
//	@ResponseBody
//	public ResponseEntity<Object> getAppRecommendationArticles() {
//		log.info("Received query for reindexData: " );
//		HttpStatus status = HttpStatus.OK;
//		try {
//			ESReindexService duvi = new ESReindexService();
//			
//			duvi.reindex("rec_story_detail", "recommendation_story_detail");
//			return new ResponseEntity<Object>("SUCCESS", status);
//		} catch (Exception e) {
//			log.error("Error occurred in retriving app recommendation stories.", e);
//			status = HttpStatus.BAD_REQUEST;
//			return new ResponseEntity<Object>(e.getMessage(), status);
//		}
//	}


	@RequestMapping(method = RequestMethod.GET, value = "/getHealth")
	@ResponseBody
	public ResponseEntity<Object> getHealth() {
		log.info("getHealth API call");
		HttpStatus status = HttpStatus.OK;
		try {
			Map<String,Object> response= new HashMap<>();
			response.put("status",true);
			return new ResponseEntity<Object>(response, status);
		} catch (Exception e) {
			log.error("Error occurred in getHealth.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
}
