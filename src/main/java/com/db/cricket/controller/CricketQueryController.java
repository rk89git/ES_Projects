package com.db.cricket.controller;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.ResponseMessage;
import com.db.common.utils.GenericUtils;
import com.db.cricket.services.CricketQueryHandlerService;

@Controller
@RequestMapping("/cricket/query")
public class CricketQueryController {

	@Autowired
	private CricketQueryHandlerService cricketQueryService;

	private static final Logger LOG = LogManager.getLogger(CricketQueryController.class);

	@RequestMapping(method = RequestMethod.POST, value = "/getScoreCard")
	@ResponseBody
	public ResponseEntity<Object> getScoreCard(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getScoreCard: " + query);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getScoreCard(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retreiving scorecard", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retreiving scorecard", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getWidgetDetail")
	@ResponseBody
	public ResponseEntity<Object> getWidgetDetail(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getWidgetDetail: " + query);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getWidgetDetails(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in widget details.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retreiving Widget details", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/schedule")
	@ResponseBody
	public ResponseEntity<Object> getMatchSchedule(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getMatchSchedule: " + query);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getScheduledOrLiveMatches(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/commentary")
	@ResponseBody
	public ResponseEntity<Object> getMatchCommentary(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getMatchCommentary: " + query);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getMatchCommentary(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving match commentary.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving match commentary.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query " + query + ". Please look at the logs for more details.",
					status), status);
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/getActiveMatches")
	@ResponseBody
	public ResponseEntity<Object> getActiveMatches() {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getActiveMatches.");
		try {
			return new ResponseEntity<Object>(cricketQueryService.getActiveMatches(), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/matchSquads")
	@ResponseBody
	public ResponseEntity<Object> getMatchSquads(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getMatchSquads. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getMatchSquads(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving match squads.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/inningDetails")
	@ResponseBody
	public ResponseEntity<Object> getInningsDetails(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getInningsDetails. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getInningsDetails(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in retreiving inning details.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/getTopWinnersList")
	@ResponseBody
	public ResponseEntity<Object> getTopWinnersList(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getTopWinnersList. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getTopWinnersList(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in fetching top winners list for predict and win .", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while fetching top winners list for predict and win. Please look at the logs for more details.", status),
					status);
		}
	}
	

	
	
	@RequestMapping(method = RequestMethod.POST, value = "/getBidDetailsSummary")
	@ResponseBody
	public ResponseEntity<Object> getBidDetailsSummary(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getBidDetailsSummary. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getBidDetailsSummary(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in fetching bids details summary for predict and win .", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while fetching bids details summary for predict and win. Please look at the logs for more details.", status),
					status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/updatedPointsPostAPI")
	@ResponseBody
	public ResponseEntity<Object> updatedPointsPostAPI(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for updatedPointsPostAPI. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.updatedPointsPostAPI(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in gettting updated points for predict and win .", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured in gettting updated point for predict and win. Please look at the logs for more details.", status),
					status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/matchWinnersPostAPI")
	@ResponseBody
	public ResponseEntity<Object> matchWinnersPostAPI(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for matchWinnersPostAPI. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.matchWinnersPOSTAPI(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in match winners post API for predict and win .", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured in match winners post API for predict and win. Please look at the logs for more details.", status),
					status);
		}
	}
	
	
	
	
	
	@RequestMapping(method = RequestMethod.GET, value = "/widgetConfig")
	@ResponseBody
	public ResponseEntity<Object> getWidgetConfig() {
		HttpStatus status = HttpStatus.OK;

		try {
			return new ResponseEntity<Object>(cricketQueryService.getWidgetConfig(), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving widget  config.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving widget config.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getCricketTeamStanding")
	@ResponseBody
	public ResponseEntity<Object> getCricketTeamStanding(@RequestBody Map<String, Object> requestBody) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getCricketTeamStanding. Input Request: "+requestBody);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getCricketTeamStanding(requestBody), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving cricket team standing.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving cricket team standing.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/calcUserBidRank/{matchId}")
	@ResponseBody
	public ResponseEntity<Object> calculateUsersBidRank(@PathVariable(value = "matchId") String matchId) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		LOG.info("Received query for calculateUsersBidRank. Input Match Id: "+matchId);
		try {
			cricketQueryService.calculateUsersBidRank(matchId);
			responseMessage.setMessage("Ranks successfully Calculated.");
			responseMessage.setStatus(status.value());
			
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error while calculating users bid rank.", e);
			
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(e.getMessage());
			
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			LOG.error("Error while calculating users bid rank.", e);
		
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(e.getMessage());
			
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/getLastActiveMatch")
	@ResponseBody
	public ResponseEntity<Object> lastActiveMatch() {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getActiveMatches.");
		try {
			return new ResponseEntity<Object>(cricketQueryService.lastActiveMatch(), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving match schedule.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getScoreCardPlayersStats")
	@ResponseBody
	public ResponseEntity<Object> getScoreCardWithPlayerStats(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getScoreCardPlayersStats: " + query);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getScoreCardWithPlayersStats(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving getScoreCardPlayersStats", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving getScoreCardPlayersStats", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/getPlayersStats")
	@ResponseBody
	public ResponseEntity<Object> getPalyersStats(@RequestBody Map<String, Object> query) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for getPlayersStats: " + query);
		
		String seriesId = (String)query.get(IngestionConstants.SERIES_ID);
		try {
			if(query.containsKey(IngestionConstants.TEAM_ID)){
				return new ResponseEntity<Object>(cricketQueryService.getPalyersStats(seriesId, 
						(String)query.get(IngestionConstants.TEAM_ID)), status);
			}else{
				return new ResponseEntity<Object>(cricketQueryService.getPalyersStats(seriesId, null), status);
			}
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retrieving getPlayersStats", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving getPlayersStats", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/matchDisabledIds/{matchId}")
	@ResponseBody
	public ResponseEntity<Object> matchDisabledIds(@PathVariable(value = "matchId") String matchId) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();
		try {
			
			return new ResponseEntity<Object>(cricketQueryService.matchDisabledIds(matchId), status);
		} catch (Exception e) {
			LOG.error("Error while retrieving disabled Match Ids.", e);
			
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage(e.getMessage());
			
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/squads")
	@ResponseBody
	public ResponseEntity<Object> getSquads(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;
		LOG.info("Received query for /squads. Input Request: "+queryJson);
		try {
			return new ResponseEntity<Object>(cricketQueryService.getSquads(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in retrieving squads.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing request. Please look at the logs for more details.", status),
					status);
		}
	}
}
