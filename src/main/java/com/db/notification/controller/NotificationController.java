package com.db.notification.controller;

import com.db.common.model.ResponseMessage;
import com.db.common.services.QueryHandlerService;
import com.db.notification.v1.services.PushNotificationService;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/notify")
public class NotificationController {
	
	private static Logger log = LogManager.getLogger(NotificationController.class);
	
	@Autowired
	private PushNotificationService pushNotificationService;

	@Autowired
	private QueryHandlerService queryHandlerService=new QueryHandlerService();
	
	@RequestMapping(method = RequestMethod.POST, value = "/pushBrowserNotification")
	@ResponseBody
	public ResponseEntity<Object> browserNotificationPush(@RequestBody String jsonData) {
		log.info("Input data in browserNotificationPush: " + jsonData);

		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushBrowserNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing broswer notification.", e);
			ResponseMessage responseMessage = new ResponseMessage();
			status = HttpStatus.BAD_REQUEST;
			responseMessage.setStatus(status.value());
			responseMessage.setMessage("Error occurred while pushing notification."+e.getMessage());
			return new ResponseEntity<Object>(responseMessage, status);
		}
	}


	@RequestMapping(method = RequestMethod.POST, value = "/pushAutomatedAppNotification")
	@ResponseBody
	public ResponseEntity<Object> pushAutomatedAppNotification(@RequestBody String jsonData) {
		log.info("Input data in pushAutomatedAppNotification: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushAutomatedAppNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/pushAutomatedBrowserNotification")
	@ResponseBody
	public ResponseEntity<Object> pushAutomatedBrowserNotification(@RequestBody String jsonData) {
		log.info("Input data in pushAutomatedBrowserNotification: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushAutomatedBrowserNotification(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/pushAutomatedBrowserNotificationNew")
	@ResponseBody
	public ResponseEntity<Object> pushAutomatedBrowserNotificationNew(@RequestBody String jsonData) {
		log.info("Input data in pushAutomatedBrowserNotificationNew: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushAutomatedBrowserNotificationNew(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/getNotificationList")
	@ResponseBody
	public ResponseEntity<Object> getNotificationList(@RequestBody String jsonData) {
		log.info("Input data in getNotificationList: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.getNotificationList(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while getting notification list.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}


	@RequestMapping(method = RequestMethod.POST, value = "/pushAutomatedAppNotificationNew")
	@ResponseBody
	public ResponseEntity<Object> pushAutomatedAppNotificationNew(@RequestBody String jsonData) {
		log.info("Input data in pushAutomatedAppNotificationNew: " + jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			return new ResponseEntity<Object>(queryHandlerService.pushAutomatedAppNotificationNew(jsonData), status);
		} catch (Exception e) {
			log.error("Error occurred while pushing notification.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<Object>(e.getMessage(), status);
		}
	}


//	DIVYA AUTOMATED NOTIFICATION BY ROHIT SHARMA
	@RequestMapping(method = RequestMethod.POST, value = "/pushAutomatedDivyaNotification")
	@ResponseBody
	public ResponseEntity<Object> pushAutomatedDivyaNotification(@RequestBody String jsonData) {
		log.info("AUTOMATEDPUSH Input data in pushAutomatedAppNotification: "+jsonData);
		HttpStatus status = HttpStatus.OK;
		try {
			JSONObject jsonObject = new JSONObject(jsonData);
	        int logicStatus = (int) jsonObject.opt("logicStatus");
	        int hourDelay = (int) jsonObject.opt("hourDelay");
			return new ResponseEntity<>(queryHandlerService.pushAutomatedDivyaNotification(logicStatus,hourDelay), status);
		} catch (Exception e) {
			log.error("Error in pushAutomatedDivyaNotification API.", e);
			status = HttpStatus.BAD_REQUEST;
			return new ResponseEntity<>(e.getMessage(), status);
		}
	}
}
