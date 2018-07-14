package com.db.comment.controller;

import java.util.Map;

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

import com.db.comment.services.CommentQueryHandler;
import com.db.common.model.ResponseMessage;
import com.db.notification.v1.model.NotificationQuery;

/**
 * The Class CommentQueryController.
 */

@Controller
@RequestMapping("/comment/query")
public class CommentQueryController {

	/** The log. */
	private static Logger log = LogManager.getLogger(CommentQueryController.class);
	
	/** The comment query handler service. */	
	@Autowired
	private CommentQueryHandler commentQueryHandler;
	
	/**
	 * Gets all comments.
	 *
	 * @param jsonData the json data
	 * @return all the comments 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getComments")
	@ResponseBody
	public ResponseEntity<Object> getComments(@RequestBody String jsonData) {
		log.info("Received query for getComments: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getComments(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getComments.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	/**
	 * Comments daywise report.
	 *
	 * @return comments daywise count report
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/dayWiseReport")
	@ResponseBody
	public ResponseEntity<Object> getDayWiseReport() {
		log.info("Received query for getdayWiseReport.");
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getDayWiseReport("{}"), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getComments.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	/**
	 * Gets the most engaged comment.
	 *
	 * @param jsonData the json data
	 * @return most engaged comment
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getMostEngagedComment")
	@ResponseBody
	public ResponseEntity<Object> getMostEngagedComment(@RequestBody String jsonData) {
		log.info("Received query for getMostEngagedComment: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getMostEngagedComment(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getMostEngagedComment.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	/**
	 * Gets most engaged Article.
	 *
	 * @param jsonData the json data
	 * @return the most engaged Article 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getMostEngagedArticle")
	@ResponseBody
	public ResponseEntity<Object> getMostEngagedArticle(@RequestBody String jsonData) {
		log.info("Received query for getMostEngagedArticle: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getMostEngagedArticle(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getMostEngagedArticle.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}



	@RequestMapping(method = RequestMethod.POST, value = "/topReviews")
	@ResponseBody
	public ResponseEntity<Object> getTopReviews(@RequestBody NotificationQuery query) {
		log.info("Received query for getTopReviews: " + query);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(commentQueryHandler.getTopReviews(query), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getTopReviews.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}



	@RequestMapping(method = RequestMethod.POST, value = "/recentReviews")
	@ResponseBody
	public ResponseEntity<Object> getRecentReviews(@RequestBody NotificationQuery query) {
		log.info("Received query for getRecentReviews: " + query);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(commentQueryHandler.getRecentReviews(query), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getRecentReviews.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}



	
	/**
	 * Gets the comments based on passed id.
	 *
	 * @param jsonData the json data
	 * @return first comment based on passed id following all the comments 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getCommentsById")
	@ResponseBody
	public ResponseEntity<Object> getCommentsByNotificationClicks(@RequestBody String jsonData) {
		log.info("Received query for getCommentsByNotificationClicks: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getCommentsById(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getCommentsByNotificationClicks.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	/**
	 * Gets the comments based on decision agree or disagree by user.
	 *
	 * @param jsonData the json data
	 * @return first comment based on passed id following all the comments 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/getAgreeOrDisagreeComments")
	@ResponseBody
	public ResponseEntity<Object> getAgreeOrDisagreeComments(@RequestBody String jsonData) {
		log.info("Received query for getAgreeOrDisagreeComments: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {                                                     
			return new ResponseEntity<Object>(commentQueryHandler.getAgreeOrDisagreeComments(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred in retrieving getCommentsByNotificationClicks.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}
	
	/**
	 *	Push spam words for comments
	 *
	 * @param jsonData the json data
	 * @return 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/pushSpamWords")
	@ResponseBody
	public ResponseEntity<Object> pushSpamWords(@RequestBody Map<String, Object> jsonData) {
		log.info("Received spam words for comments: " + jsonData);
	
		ResponseMessage responseMessage = new ResponseMessage();
		HttpStatus status = HttpStatus.OK;
		try {
			responseMessage.setMessage(commentQueryHandler.pushSpamWords(jsonData));
			responseMessage.setStatus(status.value());
			
			return new ResponseEntity<Object>(responseMessage, status);
		} catch (Exception e) {
			log.error("Error while pushing spam words for comments.", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("Spam words could not be stored  " + e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}
	
	
	/**
	 *	Delete comments
	 *
	 * @param jsonData the json data
	 * @return 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/deleteComment")
	@ResponseBody
	public ResponseEntity<Object> deleteComments(@RequestBody Map<String, Object> jsonData) {
		log.info("Received comment to delete: " + jsonData);
	
		ResponseMessage responseMessage = new ResponseMessage();
		HttpStatus status = HttpStatus.OK;
		try {
			responseMessage.setMessage(commentQueryHandler.deleteComment(jsonData));
			responseMessage.setStatus(status.value());
			
			return new ResponseEntity<>(responseMessage, status);
		} catch (Exception e) {
			log.error("Error while deleting comment.", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("comment not deleted " + e.getMessage());
			return new ResponseEntity<>(responseMessage, status);
		}
	}
	
	/**
	 *	Delete comments
	 *
	 * @param jsonData the json data
	 * @return 
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/deleteUser")
	@ResponseBody
	public ResponseEntity<Object> deleteUser(@RequestBody Map<String, Object> jsonData) {
		log.info("Received comment to delete: " + jsonData);
	
		ResponseMessage responseMessage = new ResponseMessage();
		HttpStatus status = HttpStatus.OK;
		try {
			responseMessage.setMessage(commentQueryHandler.deleteUser(jsonData));
			responseMessage.setStatus(status.value());
			
			return new ResponseEntity<>(responseMessage, status);
		} catch (Exception e) {
			log.error("Error while deleting comment.", e);
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("user not deleted " + e.getMessage());
			return new ResponseEntity<>(responseMessage, status);
		}
	}
}
