package com.db.cricket.predictnwin.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.ResponseMessage;
import com.db.common.utils.GenericUtils;
import com.db.cricket.predictnwin.services.PredictNWinQueryHandlerService;

@Controller
@RequestMapping("/predict/query")
public class PredictNWinQueryController {

	@Autowired
	private PredictNWinQueryHandlerService handler;

	private static final Logger LOG = LogManager.getLogger(PredictNWinQueryController.class);

	@RequestMapping(method = RequestMethod.POST, value = "/history")
	@ResponseBody
	public ResponseEntity<Object> getScoreCard(@RequestBody String query) {
		HttpStatus status = HttpStatus.OK;

		try {
			return new ResponseEntity<Object>(handler.getTransactionHistory(query), status);
		} catch (DBAnalyticsException e) {
			LOG.error("Error occured in retreiving transaction history.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(e.getMessage(), status), status);
		} catch (Exception e) {
			LOG.error("Error occured in retreiving transaction history.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while processing query" + query + ". Please look at the logs for more details.",
					status), status);
		}
	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/calculateTopWinnersForPredictAndWin")
	public ResponseEntity<Object> calculateTopWinnersForPredictAndWin(@RequestBody String jsonData) {
		HttpStatus status = HttpStatus.OK;
		ResponseMessage responseMessage = new ResponseMessage();

		LOG.info("Received request for calculating top winners For predict and win : " + jsonData);
		try {
			handler.topWinnersForPredictAndWin(jsonData);
			//cricketIngestionService.topWinnersForPredictAndWin(jsonData);

			responseMessage.setMessage("calculated top winners successfully.");
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			LOG.error("Could not process top winners .", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("ERROR: Could not process match winners . " + "Caused By: " + e.getMessage());

			return new ResponseEntity<Object>(responseMessage, status);
		}
	}
	
//	@RequestMapping(method = RequestMethod.POST, value = "/getTopWinnersList")
//	@ResponseBody
//	public ResponseEntity<Object> getTopWinnersList(@RequestBody String queryJson) {
//		HttpStatus status = HttpStatus.OK;
//
//		try {
//			return new ResponseEntity<Object>(handler.getTopWinnersList(queryJson), status);
//		} catch (Exception e) {
//			LOG.error("Error occured in fetching top winners list for predict and win .", e);
//			status = HttpStatus.BAD_REQUEST;
//			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
//					"Error occured while fetching top winners list for predict and win. Please look at the logs for more details.", status),
//					status);
//		}
//	}
	
	
	@RequestMapping(method = RequestMethod.POST, value = "/getBidDetailsSummary")
	@ResponseBody
	public ResponseEntity<Object> getBidDetailsSummary(@RequestBody String queryJson) {
		HttpStatus status = HttpStatus.OK;

		try {
			return new ResponseEntity<Object>(handler.getBidDetailsSummary(queryJson), status);
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

		try {
			return new ResponseEntity<Object>(handler.updatedPointsPostAPI(queryJson), status);
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

		try {
			return new ResponseEntity<Object>(handler.matchWinnersPOSTAPI(queryJson), status);
		} catch (Exception e) {
			LOG.error("Error occured in match winners post API for predict and win .", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured in match winners post API for predict and win. Please look at the logs for more details.", status),
					status);
		}
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/notifyLazyUsers")
	@ResponseBody
	public ResponseEntity<Object> notifyLazyUsers(@RequestParam(value = "dryRun", required=false) String dryRun) {
		HttpStatus status = HttpStatus.OK;

		try {
			handler.notifyLazyUsers(dryRun);
			return new ResponseEntity<Object>("Notification Sent successfully to lazy users", status);
		} catch (Exception e) {
			LOG.error("Error occured while sending notifications to lazy users", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(GenericUtils.getResponseMessage(
					"Error occured while sending notifications to lazy users.", status),
					status);
		}
	}


}
