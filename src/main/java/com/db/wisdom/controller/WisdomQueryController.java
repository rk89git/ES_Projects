package com.db.wisdom.controller;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.utils.GenericUtils;
import com.db.wisdom.services.WisdomQueryHandlerService;

/**
 * The Class WisdomQueryController.
 */
@Controller
@RequestMapping("/wisdom/query")
public class WisdomQueryController {

	/** The wisdom query handler service. */
	private WisdomQueryHandlerService wisdomQueryHandlerService = new WisdomQueryHandlerService();

	/** The log. */
	private static Logger log = LogManager.getLogger(WisdomQueryController.class);

	/**
	 * Gets the top authors list.
	 *
	 * @param jsonData the json data
	 * @return the top authors list
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/topAuthors")
	@ResponseBody
	public ResponseEntity<Object> getTopAuthorsList(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTopAuthors" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTopAuthorsList(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Authors monthly data.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/authorsMonthlyData")
	@ResponseBody
	public ResponseEntity<Object> authorsMonthlyData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuthorsMonthlyData" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getAuthorsMonthlyData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Gets the author stories list.
	 *
	 * @param jsonData the json data
	 * @return the author stories list
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/authorStoriesList")
	@ResponseBody
	public ResponseEntity<Object> getAuthorStoriesList(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTopAuthorStories" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getAuthorStoriesList(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}	


	/**
	 * Editor dashboard top stories.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	/*@RequestMapping(method = RequestMethod.POST, value = "/topStories")
	@ResponseBody
	public ResponseEntity<Object> getTopStories(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTopStories" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTopStories(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/editorDashboardTopStories")
	@ResponseBody
	public ResponseEntity<Object> editorDashboardTopStories(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {

			log.info("Received query for editorDashboardTopStories" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.editorDashboardTopStories(jsonData), status);
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
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsights(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

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

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTopStoryListing(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Story detail.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail")
	@ResponseBody
	public ResponseEntity<Object> storyDetail(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getStoryDetail(jsonData), status);
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

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getSlideWisePvs(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Story performance graph.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/performanceGraph")
	@ResponseBody
	public ResponseEntity<Object> storyPerformanceGraph(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/performanceGraph" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getStoryPerformance(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Facebook story performance graph.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/facebookPerformanceGraph")
	@ResponseBody
	public ResponseEntity<Object> facebookStoryPerformanceGraph(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/facebookPerformanceGraph" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookStoryPerformance(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Ucb story performance graph.
	 *
	 * @param jsonData the json data
	 * @return the response entity
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/trackerwisePerformanceGraph")
	@ResponseBody
	public ResponseEntity<Object> trackerwisePerformanceGraph(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/trackerwisePerformanceGraph" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTrackerwisePerformanceGraph(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/versionwisePerformance")
	@ResponseBody
	public ResponseEntity<Object> versionwisePerformance(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/versionwisePerformance" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getVersionwisePerformance(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	@RequestMapping(method = RequestMethod.POST, value = "/storyDetail/storyTimeline")
	@ResponseBody
	public ResponseEntity<Object> storyTimeline(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storyDetail/storyTimeline" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getStoryTimeline(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/trendingEntities")
	@ResponseBody
	public ResponseEntity<Object> trendingEntities(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for trendingEntities" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTrendingEntities(jsonData), status);
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
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsByInterval(jsonData), status);
			
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
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsByIntervalAndCategory(jsonData), status);
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
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsByCategory(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsByCategoryAndDay")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsByCategoryAndDay(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for facebookInsightsByCategoryAndDay" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsByCategoryAndDay(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookInsightsForAutomation")
	@ResponseBody
	public ResponseEntity<Object> facebookInsightsForAutomation(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for facebookInsightsForAutomation" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsForAutomation(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/trendingEntitiesForSocialDecode")
	@ResponseBody
	public ResponseEntity<Object> trendingEntitiesForSocialDecode(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for trendingEntitiesForSocialDecode" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTrendingEntitiesForSocialDecode(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/EODFlickerData")
	@ResponseBody
	public ResponseEntity<Object> eodFlickerData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for EODFlickerData" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getEODFlickerData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/AdMetricsData")
	@ResponseBody
	public ResponseEntity<Object> adMetricsData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for AdMetricsData" + jsonData);			
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getAdMetricsData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * Gets the user session details.
	 *
	 * @param jsonData the json data
	 * @return the user session details
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/frequency/getUserFrequency")
	@ResponseBody
	public ResponseEntity<Object> getUserSessionDetails(@RequestBody String jsonData) {
		log.info("Received query for getUserFrequency: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getUserSessionDetails(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retriving Top Ucb Stories.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"ERROR: Failed to retrieve UCB articles. Caused By: " + e.getMessage(), status), status);
		}
	}

	/**
	 * Gets the session buckets.
	 *
	 * @param jsonData the json data
	 * @return the session buckets
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/frequency/getSessionBuckets")
	@ResponseBody
	public ResponseEntity<Object> getSessionBuckets(@RequestBody String jsonData) {
		log.info("Received query for getSessionBuckets: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getUserSessionBuckets(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Gets the session buckets.
	 *
	 * @param jsonData the json data
	 * @return the session buckets
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/frequency/getSessionBucketsWithDetails")
	@ResponseBody
	public ResponseEntity<Object> getSessionBucketsWithDetails(@RequestBody String jsonData) {
		log.info("Received query for getSessionBucketsWithDetails: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getUserSessionBucketsWithDetails(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}

	/**
	 * Gets the session count for user.
	 *
	 * @param sessionId the session id
	 * @return the session count for user
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/frequency/getSessionCount/{sessionId:.+}")
	@ResponseBody
	public ResponseEntity<Object> getSessionCountForUser(@PathVariable(value = "sessionId") String sessionId) {
		HttpStatus status = HttpStatus.OK;
		log.info("Received query for getSessionCount: " + sessionId);
		try {
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getSessionCountForUser(sessionId), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/getArticleDiscovery")
	@ResponseBody
	public ResponseEntity<Object> getArticleDiscovery(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getArticleDiscovery" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getArticleDiscovery(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getWidgetArticleDiscovery")
	@ResponseBody
	public ResponseEntity<Object> getWidgetArticleDiscovery(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getWidgetArticleDiscovery" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getWidgetArticleDiscovery(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/getArticleFeedback")
	@ResponseBody
	public ResponseEntity<Object> getArticleFeedback(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getArticleFeedback" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getArticleFeedback(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
		
	@RequestMapping(method = RequestMethod.POST, value = "/getUserFrequencybyStory")
	@ResponseBody
	public ResponseEntity<Object> getUserFrequencybyStory(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getUserFrequencybyStory" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getUserFrequencybyStory(jsonData), status);
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
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookInsightsForDate(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFacebookUcbFlag")
	@ResponseBody
	public ResponseEntity<Object> getFacebookUcbFlag(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFacebookUcbFlag" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookUcbFlag(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/facebookPageInsightsByInterval")
	@ResponseBody
	public ResponseEntity<Object> getFacebookPageInsightsByInterval(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFacebookPageInsightsByInterval" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFacebookPageInsightsByInterval(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getTrackerwiseStoryDetail")
	@ResponseBody
	public ResponseEntity<Object> getTrackerwiseStoryDetail(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTrackerwiseStoryDetail" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTrackerwiseStoryDetail(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFbCompetitorsWithInterval")
	@ResponseBody
	public ResponseEntity<Object> getFbCompetitorsWithInterval(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFbCompetitorsWithInterval" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFbCompetitorsWithInterval(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFbCompetitors")
	@ResponseBody
	public ResponseEntity<Object> getFbCompetitors(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFbCompetitors" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFbCompetitors(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFbCompetitorsStories")
	@ResponseBody
	public ResponseEntity<Object> getFbCompetitorsStories(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFbCompetitorsStories" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFbCompetitorsStories(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getFbVelocity")
	@ResponseBody
	public ResponseEntity<Object> getFbVelocity(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getFbVelocity" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getFbVelocity(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/get0HourFbVelocity")
	@ResponseBody
	public ResponseEntity<Object> get0HourFbVelocity(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for get0HourFbVelocity" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.get0HourFbVelocity(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getSimilarStories")
	@ResponseBody
	public ResponseEntity<Object> getSimilarStories(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getSimilarStories" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getSimilarStories(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/get0HourFbVelocityGraph")
	@ResponseBody
	public ResponseEntity<Object> get0HourFbVelocityGraph(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for get0HourFbVelocityGraph" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.get0HourFbVelocityGraph(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getStoriesForCommentDashboard")
	@ResponseBody
	public ResponseEntity<Object> getStoriesForCommentDashboard(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getStoriesForCommentDashboard" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getStoriesForCommentDashboard(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getComments")
	@ResponseBody
	public ResponseEntity<Object> getComments(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getComments" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getComments(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getVideosCtr")
	@ResponseBody
	public ResponseEntity<Object> getVideosCtr(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getVideosCtr" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getVideosCtr(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getWidgetWiseVideosCtr")
	@ResponseBody
	public ResponseEntity<Object> getWidgetWiseVideosCtr(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getWidgetWiseVideosCtr" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getWidgetWiseVideosCtr(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getSharability")
	@ResponseBody
	public ResponseEntity<Object> getSharability(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getSharability" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getSharability(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getKraReport")
	@ResponseBody
	public ResponseEntity<Object> getKraReport(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getKraReport" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getKraReport(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/storiesForFlickerAutomation")
	@ResponseBody
	public ResponseEntity<Object> getStoriesForFlickerAutomation(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for storiesForFlickerAutomation" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getStoriesForFlickerAutomation(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/AuditSiteData")
	@ResponseBody
	public ResponseEntity<Object> getAuditSiteData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuditSiteData" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getAuditSiteData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/mis_report")
	@ResponseBody
	public ResponseEntity<Object> getMISReports(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received request for MIS Report" + jsonData);				
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getMISReport(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * Gets the  authors list for newsletter.
	 *
	 * @param jsonData the json data
	 * @return the top authors list with stories for newsletter
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/AuthorsListForNewsLetter")
	@ResponseBody
	public ResponseEntity<Object> getAuthorsListForNewsLetter(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getAuthorsListForNewsLetter" + jsonData);

			return new ResponseEntity<Object>(wisdomQueryHandlerService.getAuthorsListForNewsLetter(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * Gets PVS, UVS and sessions.
	 *
	 * @param jsonData the json data
	 * @return the PVS, UVS and sessions
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/GAtoWisdomDailyDataVerification")
	@ResponseBody
	public ResponseEntity<Object> getDailyData(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for GAtoWisdomDailyDataVerification" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getDailyData(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	
	/**
	 * Gets year till date data.
	 *
	 * @param jsonData the json data
	 * @return the ytdReport
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/ytdReoprt")
	@ResponseBody
	public ResponseEntity<Object> getYTDReport(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getYTDReport" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getYTDReport(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/keywordBasedSimilarStories")
	@ResponseBody
	public ResponseEntity<Object> getKeywordBasedSimilarStories(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getKeywordBasedSimilarStories" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getKeywordBasedSimilarStories(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/keywordBasedSimilarStoriesCount")
	@ResponseBody
	public ResponseEntity<Object> getKeywordBasedSimilarStoriesCount(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getKeywordBasedSimilarStoriesCount" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getKeywordBasedSimilarStoriesCount(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/keywordBasedSimilarStoriesCountAlphabetical")
	@ResponseBody
	public ResponseEntity<Object> getKeywordBasedSimilarStoriesCountAlphabetical(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getKeywordBasedSimilarStoriesCountAlphabetical" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getKeywordBasedSimilarStoriesCountAlphabetical(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/keywordBasedSimilarStoriesCountByInterval")
	@ResponseBody
	public ResponseEntity<Object> getKeywordBasedSimilarStoriesCountByInterval(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getKeywordBasedSimilarStoriesCountByInterval" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getKeywordBasedSimilarStoriesCountByInterval(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/totalEngagementByInterval")
	@ResponseBody
	public ResponseEntity<Object> getTotalEngagementByInterval(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getTotalEngagementByInterval" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getTotalEngagementByInterval(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
			
	@RequestMapping(method = RequestMethod.POST, value = "/videoReport")
	@ResponseBody
	public ResponseEntity<Object> getVideoReport(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getVideoReport" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getVideoReport(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/notificationSessions")
	@ResponseBody
	public ResponseEntity<Object> getNotificationSessions(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getNotificationSessions" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getNotificationSessions(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	/**
	 * Gets year till date data.
	 *
	 * @param jsonData the json data
	 * @return the social data from wisdom
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/DataForSocial")
	@ResponseBody
	public ResponseEntity<Object> getDataForSocialTotal(@RequestParam String startDate) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getDataForSocialTotal" + startDate);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getDataForSocialTotal(startDate), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/completeStoryDetail")
	@ResponseBody
	public ResponseEntity<Object> getCompleteStoryDetail(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		try {
			log.info("Received query for getCompleteStoryDetail" + jsonData);
			return new ResponseEntity<Object>(wisdomQueryHandlerService.getCompleteStoryDetail(jsonData), status);
		} catch (Exception e) {
			log.error(e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		}
	}
}