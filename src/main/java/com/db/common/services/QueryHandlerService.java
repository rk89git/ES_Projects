package com.db.common.services;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.db.notification.v1.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.ArticleParams;
import com.db.common.model.FetchRecordsCountByHost;
import com.db.common.model.GenericQuery;
import com.db.common.model.UserFrequencyQuery;
import com.db.common.model.UserPersonalizationQuery;
import com.db.notification.v1.enums.NotificationTargetType;
import com.db.notification.v1.services.PushNotificationService;
import com.db.wisdom.model.WisdomQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
@Service
public class QueryHandlerService {

	@Autowired
	private QueryExecutorService queryExecutorService;// = new QueryExecutorService();

	private static Logger log = LogManager.getLogger(QueryHandlerService.class);

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	@Autowired
	private PushNotificationService pushNotificationService = new PushNotificationService();

	public String getHistoryOfUser(String sessionId) {
		String recordList = gson.toJson(queryExecutorService.getHistoryOfUser(sessionId));
		// System.out.println(recordList);
		return recordList;
	}

	public String getLastVisitOfUser(String sessionId) {
		return queryExecutorService.getLastVisitOfUser(sessionId);
	}

	public List<String> getAbsentVisitors(Integer noOfDays) {
		return queryExecutorService.getAbsentVisitors(noOfDays);
	}


	public List<String> getUserAggsCount(String jsonData) {
		UserFrequencyQuery userFrequencyQuery = gson.fromJson(jsonData, UserFrequencyQuery.class);
		if (userFrequencyQuery.getDayCount() > 90) {
			throw new DBAnalyticsException("Invalid days interval. Days must be less than 90 days.");
		}
		return queryExecutorService.getUsersVisitIdConditionalAggs(userFrequencyQuery);
	}

	public List<Integer> getUserPersonalizationStories(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);
		// if (StringUtils.isBlank(query.getSession_id())) {
		// throw new DBAnalyticsException(
		// "Invalid user personalization request. Mandatory attributes:
		// session_id. Optional attributes: hosts, count");
		// }

		if (query.getCount() > 60) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 60.");
		}

		// For APP Recommendation
		if (StringUtils.isNotBlank(query.getChannel()) && query.getChannel().equalsIgnoreCase("521")) {
			query.setHosts("15");
		} else if (StringUtils.isNotBlank(query.getChannel()) && query.getChannel().equalsIgnoreCase("960")) {
			query.setHosts("20");
		}
		return queryExecutorService.getUserPersonalizationStories(query);
	}

	public List<Integer> getAppRecommendationArticles(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);
		if (query.getCount() > 60) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 60.");
		}

		// For APP Recommendation
		if (StringUtils.isNotBlank(query.getChannel()) && query.getChannel().equalsIgnoreCase("521")) {
			query.setHosts("15");
		} else if (StringUtils.isNotBlank(query.getChannel()) && query.getChannel().equalsIgnoreCase("960")) {
			query.setHosts("20");
		}
		return queryExecutorService.getAppRecommendationArticles(query);
	}

	public List<Integer> getMostViewedArticles(String jsonData) {
		ArticleParams query = gson.fromJson(jsonData, ArticleParams.class);
		return queryExecutorService.getMostViewedArticles(query);
	}

	public Map<String, List<Integer>> getCityWiseStories(String jsonData) {
		ArticleParams query = gson.fromJson(jsonData, ArticleParams.class);
		return queryExecutorService.getCityWiseStories(query);
	}

	public String pushNotification(String jsonData) {
		NotificationStory notificationStory = gson.fromJson(jsonData, NotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");

		for (String host : hosts) {
			pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
					NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
		}

		return "Request for pushing notification submitted successfully.";
	}

	public String pushAutomatedAppNotification(String jsonData) {
		AutomatedNotificationStory notificationStory = gson.fromJson(jsonData, AutomatedNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}
		if(notificationStory.getAlgorithm().equalsIgnoreCase("CTR"))
		pushNotificationService.autoSendNotificationsCTR(notificationStory);
		else
			pushNotificationService.autoSendNotifications(notificationStory);

		return "Request for pushing notification submitted successfully.";
	}

	public Map<String,Object> getNotificationList(String jsonData) {
		NotificationQuery notificationQuery = gson.fromJson(jsonData, NotificationQuery.class);
		return queryExecutorService.getNotificationList(notificationQuery);
	}

	public String pushAutomatedBrowserNotification(String jsonData) {
		AutomatedNotificationStory notificationStory = gson.fromJson(jsonData, AutomatedNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}
		pushNotificationService.autoSendNotifications(notificationStory);

		return "Request for pushing notification submitted successfully.";
	}
	public String pushBrowserNotification(String jsonData) {
		BrowserNotificationStory notificationStory = gson.fromJson(jsonData, BrowserNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");

		for (String host : hosts) {
			pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
					NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER);
		}

		return "Request for pushing notification submitted successfully.";
	}

	public String pushAutomatedBrowserNotificationNew(String jsonData) {
		AutomatedNotificationStory notificationStory = gson.fromJson(jsonData, AutomatedNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");

		for (String host : hosts) {
			pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
					NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER_AUTOMATED);
		}

		return "Request for pushing notification submitted successfully.";
	}

	public Set<String> getSources(GenericQuery genericQuery) {
		return queryExecutorService.getSources(genericQuery);
	}


	public Map<String,Long> getVideoPartners(GenericQuery genericQuery) {
		return queryExecutorService.getVideoPartners(genericQuery);
	}

	public Map<String,Long> getFollowTags(GenericQuery genericQuery) {
		return queryExecutorService.getFollowTags(genericQuery);
	}
	/**
	 * This API will be used to send notification to mentioned device tokens
	 * list. API is created only for testing purpose.
	 * 
	 * @param jsonData
	 * @return
	 */
	public String testPushNotification(String jsonData) {
		NotificationStory notificationStory = gson.fromJson(jsonData, NotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");

		String[] deviceTokens = notificationStory.getDeviceTokens().split(",");
		List<String> deviceTokensList = Arrays.asList(deviceTokens);
		Map<String,List<String>> target= new HashMap<>();
		target.put(Constants.DEVICE_TOKEN,deviceTokensList);
		notificationStory.setTarget(target);

		for (String host : hosts) {
			pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
					NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
		}
		return "Test Notification sent successfully.";
	}

	public String testBrowserPushNotification(String jsonData) {
		BrowserNotificationStory notificationStory = gson.fromJson(jsonData, BrowserNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");

		String[] deviceTokens = notificationStory.getDeviceTokens().split(",");
		List<String> deviceTokensList = Arrays.asList(deviceTokens);

		for (String host : hosts) {
			pushNotificationService.testPushNotification(notificationStory, Integer.parseInt(host.trim()),
					deviceTokensList, new HashMap<String, String>());
		}
		return "Test Notification sent successfully.";
	}

	public List<PushNotificationHistory> getNotificationHistory(String jsonData) {
		GenericQuery genericQuery = gson.fromJson(jsonData, GenericQuery.class);

		return queryExecutorService.getNotificationHistory(genericQuery);
	}

	public Map<String, Object> getNotificationSubscribers(String jsonData) {
		GenericQuery genericQuery = gson.fromJson(jsonData, GenericQuery.class);

		return queryExecutorService.getNotificationSubscribers(genericQuery);
	}

	public String getCohortGroup(String sessionId) {
		String group = queryExecutorService.getCohortGroup(sessionId);
		log.info("Cohort group for user [" + sessionId + "] is " + group);
		return group;
	}

	public String getWisdomUvsPvs() {
		return queryExecutorService.getUVsPvsForWisdom();
	}

	public String getStoryIdHistoryOfUser(String sessionId, Integer count) {
		if (count == null || count == 0) {
			count = 50;
		}
		String recordList = gson.toJson(queryExecutorService.getStoryIdHistoryOfUser(sessionId, count));
		return recordList;

	}

	public String getRecord(String indexName, String mappingType, String key) {
		return queryExecutorService.getRecord(indexName, mappingType, key);
	}

	public String getWebUserPersonalizationStories(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);

		if (query.getCount() > 60) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 60.");
		}

		return gson.toJson(queryExecutorService.getWebUserPersonalizationStories(query));
	}

	public String getMostTrendingStories(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);

		if (query.getCount() > 18) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 18.");
		}

		return gson.toJson(queryExecutorService.getMostTrendingStories(query));
	}

	public String getTopUCBStories(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);

		if (query.getCount() > 18) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 18.");
		}

		return gson.toJson(queryExecutorService.getTopUCBStories(query));
	}

	public String getMicromaxFeedStories(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);

		return gson.toJson(queryExecutorService.getMicromaxFeedStories(query));
	}

	public Set<String> getVideoRecommendation(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);

		if (StringUtils.isBlank(query.getSession_id()) || StringUtils.isBlank(query.getChannel())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: session_id, channel");
		}
		Set<String> group = queryExecutorService.getVideoRecommendation(query);
		return group;
	}

	public String getAvgTimeSpent(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		String storyids = query.getStoryid();
		String response="";
		response = gson.toJson(queryExecutorService.getAvgTimeSpent(query));
		/*if(query.getTracker().equals("app")){
			response = gson.toJson(queryExecutorService.getAvgTimeSpent(query,true));
		}
		else {
			response = gson.toJson(queryExecutorService.getAvgTimeSpent(storyids));
		}*/			
		return response;
	}

	public Set<Integer> getRealtimePersonalizedRecommendation(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);
		Set<Integer> result = new HashSet<>();

		if (StringUtils.isBlank(query.getSession_id())) {
			throw new DBAnalyticsException(
					"Invalid realtime recommendation request. Mandatory attributes: session_id, hosts");
		}

		if (query.getCount() > 60) {
			throw new DBAnalyticsException(
					"Invalid story count " + query.getCount() + ", please give value less than or equal to 60.");
		}

		// if (query.getFirstTimeFlag()==1) {
		// result.addAll(queryExecutorService.getUserPersonalizationStories(query));
		// } else {
		// result=queryExecutorService.getRealtimePersonalizedRecommendation(query);
		// }

		result = queryExecutorService.getRealtimePersonalizedRecommendationV2(query);
		return result;
	}

	public List<Integer> getFacebookTrendingArticles(String jsonData) {
		ArticleParams query = gson.fromJson(jsonData, ArticleParams.class);
		if (StringUtils.isBlank(query.getTopic())) {
			throw new DBAnalyticsException(
					"Invalid facebook trending articles request. Mandatory attributes: topic, hosts");
		}

		return queryExecutorService.getFacebookTrendingArticles(query);
	}

	public List<String> getTopEODStories(String jsonData) {
		ArticleParams query = gson.fromJson(jsonData, ArticleParams.class);
		if (StringUtils.isBlank(query.getInputStoriesId())) {
			throw new DBAnalyticsException("Invalid EOD articles request. Mandatory attributes: inputStoriesId");
		}

		return queryExecutorService.getHigestCTRArticles(query);
	}

	public Long getRecordsCountByHost(String jsonData) {
		FetchRecordsCountByHost query = gson.fromJson(jsonData, FetchRecordsCountByHost.class);

		if (StringUtils.isNotBlank(query.getDateValue()) && StringUtils.isNotBlank(query.getTrackerValue())
				&& (!query.getPageNumber().isEmpty())) {
			return queryExecutorService.getRecordsCountByHost(query);
		} else {
			throw new DBAnalyticsException("Invalid data to fetch records.");
		}

	}

	public ArrayList<String> getMaxPVSstories(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		if (query.getChannel_slno() != null) {
			throw new DBAnalyticsException("Invalid getMaxPVSstories request. Mandatory attributes: Channle_slno");
		}
		return (ArrayList<String>) queryExecutorService.getMaxPVSstories(query);
	}

	public String pushAutomatedAppNotificationNew(String jsonData) {
		AutomatedNotificationStory notificationStory = gson.fromJson(jsonData, AutomatedNotificationStory.class);
		if (StringUtils.isBlank(notificationStory.getHosts())) {
			throw new DBAnalyticsException("Invalid push notification request. Please give hosts attribute.");
		}

		String[] hosts = notificationStory.getHosts().split(",");
		for (String host : hosts) {
			pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
					NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP_AUTOMATED);
		}
		return "Request for pushing notification submitted successfully.";
	}
	
	// DIVYA AUTOMATED NOTIFICATION BY ROHIT SHARMA
	public String pushAutomatedDivyaNotification(int logicStatus, int hourDelay) throws ParseException {
		pushNotificationService.sendAutomatedDivyaNotification(logicStatus,hourDelay);
		return "Automated story notification process initiated.";
	}	
}
