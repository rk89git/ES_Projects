package com.db.cricket.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IPLConstants;
import com.db.common.constants.Constants.Host;
import com.db.common.constants.Constants.NotificationConstants;
import com.db.common.constants.Indexes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.CricketGlobalVariableUtils;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.KeyGenerator;
import com.db.cricket.model.CricketNotification;
import com.db.cricket.utils.CricketUtils;
import com.db.notification.v1.enums.NotificationTargetType;
import com.db.notification.v1.model.AutomatedNotificationStory;
import com.db.notification.v1.model.BrowserNotificationStory;
import com.db.notification.v1.model.GenericSLIDRequestBody;
import com.db.notification.v1.model.NotificationStory;
import com.db.notification.v1.services.PushNotificationService;

@Component
public class CricketNotificationService {

	@Autowired
	private PushNotificationService pushNotificationService = new PushNotificationService();

	private static final Logger LOG = LogManager.getLogger(CricketNotificationService.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private DBConfig config = DBConfig.getInstance();

	@SuppressWarnings("unchecked")
	void sendNotification(CricketNotification cricketNotification, Map<?, ?> notificationData) {

		long startTime = System.currentTimeMillis();

		try {
			LOG.info("Notification Meta: " + cricketNotification);

			/*
			 * Validate notification data.
			 */
			if (notificationData == null || notificationData.isEmpty()) {
				LOG.error("Received invalid notification details.");
				return;
			}

			Map<?, ?> target = (Map<?, ?>) notificationData.get(CricketConstants.NOTIFICATION_TARGET_FIELD);

			if (notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD) == null || StringUtils
					.isBlank(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString())) {
				LOG.error("Received Invalid notification data.");
				return;
			}

			/*
			 * Check if the notification contains null as string. If it does, abort
			 * notification.
			 */
			if (cricketNotification.toNotificationString().contains("null")) {
				LOG.error("Received null in Notification.");
				return;
			} else if (cricketNotification.slugIntro().contains("null")) {
				LOG.error("Received null in Notification.");
				return;
			}

			/*
			 * This object provides configuration for sending notifications.
			 */
			AutomatedNotificationStory automatedNotificationStory = new AutomatedNotificationStory();
			automatedNotificationStory.setHosts(String.valueOf(Constants.Host.BHASKAR_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_MOBILE_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_MOBILE_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_APP_ANDROID_HOST));
			automatedNotificationStory.setNotificationTargetType("BROWSER");

			if (StringUtils.isNotBlank(config.getProperty("cricket.notification.hosts"))) {
				automatedNotificationStory.setHosts(config.getProperty("cricket.notification.hosts"));
			}

			Map<String, List<String>> targetMap = null;
			
			/*
			 * Check if target field is set. If it is, set the target of the notification.
			 */
			// TODO: make the target field usable for debugging and testing purposes.
			if (target != null && !target.isEmpty()) {

				LOG.info(target);
				targetMap = new HashMap<>();
				targetMap.put(CricketConstants.NOTIFICATION_FOLLOW_TAG_FIELD,
						(List<String>) target.get(CricketConstants.NOTIFICATION_FOLLOW_TAG_FIELD));
			}

			NotificationStory notification = null;

			/*
			 * Prepare and send notification for each host.
			 */
			for (String host : automatedNotificationStory.getHosts().split(",")) {

				notification = prepareCricketNotification(cricketNotification, host, notificationData);

				if (notification == null) {
					continue;
				}

				if (targetMap != null) {
					notification.setTarget(targetMap);
				}

				// testing for specific id
				targetMap = new HashMap<>();
				// targetMap.put("deviceId",
				// Arrays.asList("40f27f3b-678d-a572-9be0-c0e90a069748",
				// "2c6c96dd-ca7a-cdef-d393-231e26a33ca2",
				// "f383e4ce-7abb-9f68-d4cd-5018ea554f6c"));
				// notification.setTarget(targetMap);
				// notificationStory.setDryRun(true);

				LOG.info("Pushing notification: " + notification + " to host " + host);

				notification.setHosts(host);
				pushNotificationService.setDefaultParameters(notification);

				/*
				 * Check the host and send either a browser notification or an app notification.
				 */
				if (host.equalsIgnoreCase(String.valueOf(Host.DIVYA_WEB_HOST))
						|| host.equalsIgnoreCase(String.valueOf(Host.DIVYA_MOBILE_WEB_HOST))
						|| host.equalsIgnoreCase(String.valueOf(Host.BHASKAR_WEB_HOST))
						|| host.equalsIgnoreCase(String.valueOf(Host.BHASKAR_MOBILE_WEB_HOST))) {
					pushNotificationService.pushNotification(notification, Integer.parseInt(host.trim()),
							NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER);
				} else if (host.equalsIgnoreCase(String.valueOf(Host.DIVYA_APP_ANDROID_HOST))
						&& Boolean.parseBoolean(config.getProperty("cricket.notification.app.enabled"))) {
					pushNotificationService.pushNotification(notification, Integer.parseInt(host.trim()),
							NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
				}
			}

		} catch (Exception e) {
			LOG.error("Some error occured while sending notification.", e);
		}
		long endTime = System.currentTimeMillis();
		LOG.info("Time to process sendNotification Request: " + (endTime - startTime) + " ms.");
	}

	/**
	 * Prepare the notification title
	 *
	 * @param cricketNotification
	 *            the cricket notification object.
	 * @return the notification title or null
	 */
	String createTitleMessage(CricketNotification cricketNotification) {
		if (cricketNotification != null) {
			return cricketNotification.toNotificationString();
		}
		return null;
	}

	String createSlugIntroMessage(CricketNotification cricketNotification) {
		return cricketNotification.slugIntro();
	}

	/**
	 * Prepare the notification title
	 *
	 * @param cricketNotification
	 *            the cricket notification object.
	 * @return the notification title or null
	 */
	String createTitleMessage(CricketNotification cricketNotification, int host) {
		if (cricketNotification != null) {
			return cricketNotification.toNotificationString(host);
		}
		return null;
	}

	/**
	 * Provides message body for the notification as per the host specified
	 * 
	 * @param cricketNotification
	 *            the notification meta
	 * @param host
	 *            the target host for this message.
	 * @return message body string.
	 */
	String createSlugIntroMessage(CricketNotification cricketNotification, int host) {
		return cricketNotification.slugIntro(host);
	}

	/**
	 * Write a notification as a comment.
	 * 
	 * @param notification
	 */
	public void sendComment(CricketNotification notification) {

		/*
		 * If the comment contains any null string, return immediately.
		 */
		if (notification.toCommentDescription().contains("null")) {
			LOG.error("Received null in notification.");
			return;
		}

		String key = KeyGenerator.getKey();
		Map<String, Object> commentMap = new HashMap<>();
		commentMap.put(Constants.POST_ID, postHashCode("/cric-score/" + notification.getMatch_id() + "/"
				+ CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/"));
		commentMap.put("user_id", "db@dainikbhaskar.com");
		commentMap.put("email", "db@dainikbhaskar.com");
		commentMap.put(Constants.DESCRIPTION, notification.toCommentDescription());
		commentMap.put(Constants.MODIFIED_DESCRIPTION, notification.toCommentDescription());
		commentMap.put(Constants.CHANNEL_SLNO, 521);
		commentMap.put("name", CricketConstants.CRICKET_COMMENT_USER_NAME);
		commentMap.put(Constants.IMAGE, "https://i10.dainikbhaskar.com/cricketscoreboard/image/cricket.jpg");
		commentMap.put(Constants.URL, getLandingUrl(notification, Host.BHASKAR_WEB_HOST));
		commentMap.put("isReply", false);
		commentMap.put("isByAnonymousUser", false);
		commentMap.put(Constants.SESSION_ID_FIELD, "s98dcai45asdcd");
		commentMap.put(Constants.STATUS, 1);
		commentMap.put(Constants.HOST, 1);
		commentMap.put(CricketConstants.CRICKET_COMMENT_IS_AUTOMATED, true);
		commentMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

		if (StringUtils.isBlank((String) commentMap.get(Constants.ID))) {
			commentMap.put(Constants.ID, key);
			commentMap.put(Constants.ROWID, key);
		}

		LOG.info("Outgoinng Comment: " + commentMap);

		elasticSearchIndexService.index(Indexes.DB_COMMENT, commentMap);
	}

	/**
	 * Compute the hashcode value to use as post id for the comment.
	 * 
	 * @param url
	 * @return
	 */
	public int postHashCode(String url) {
		int hash = 0;

		if (StringUtils.isNotBlank(url)) {
			for (int i = 0; i < url.length(); i++) {
				hash = ((hash << 5) - hash) + url.codePointAt(i);
				hash = hash & hash;
			}
		}

		return hash;
	}

	/**
	 * Given the notification and target, provide a landing url for when the user
	 * clicks on the notification.
	 * 
	 * @param notification
	 *            the cricket notification to use.
	 * @param target
	 *            the target
	 * @return the landing url.
	 */
	String getLandingUrl(CricketNotification notification, int target) {

		switch (target) {
		case Host.BHASKAR_MOBILE_WEB_HOST:
			return pushNotificationService
					.getBasicURL(IPLConstants.M_BHASKAR_LIVE_SCORE_URL + notification.getMatch_id() + "/"
							+ CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/");
		case Host.DIVYA_MOBILE_WEB_HOST:
			return pushNotificationService
					.getBasicURL(IPLConstants.M_DIVYA_BHASKAR_LIVE_SCORE_URL + notification.getMatch_id() + "/"
							+ CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/");
		case Host.DIVYA_WEB_HOST:
			return pushNotificationService
					.getBasicURL(IPLConstants.DIVYA_BHASKAR_LIVE_SCORE_URL + notification.getMatch_id() + "/"
							+ CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/");
		case Host.DIVYA_APP_ANDROID_HOST:
			return pushNotificationService.getBasicURL(IPLConstants.M_DIVYA_BHASKAR_LIVE_SCORE_URL + notification.getMatch_id() + "/"
					+ CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/");
		default:
			return pushNotificationService.getBasicURL(IPLConstants.BHASKAR_LIVE_SCORE_URL + notification.getMatch_id()
					+ "/" + CricketUtils.replacePunct(notification.getTourName().toLowerCase()) + "/");
		}
	}

	/**
	 * Based on the host provided, prepare a cricket notification for the host.
	 * 
	 * @param notificationMeta
	 *            the meta object to create notification
	 * @param hosts
	 *            the host to create notification for.
	 * @param notificationData
	 *            contains the notification id and image.
	 * @return a <code>NotificationStory</code> object representing the notification
	 *         to send or null.
	 */
	private NotificationStory prepareCricketNotification(CricketNotification notificationMeta, String hosts,
			Map<?, ?> notificationData) {

		int host = Integer.parseInt(hosts.trim());

		NotificationStory notification = null;

		switch (host) {

		case Host.BHASKAR_WEB_HOST:
		case Host.BHASKAR_MOBILE_WEB_HOST:
		case Host.DIVYA_WEB_HOST:
		case Host.DIVYA_MOBILE_WEB_HOST:

			notification = createBrowserNotificationStory(notificationMeta, notificationData, host);
			break;
		case Host.BHASKAR_APP_ANDROID_HOST:
		case Host.DIVYA_APP_ANDROID_HOST:

			notification = createAppNotificationStory(notificationMeta, notificationData, host);
			break;
		}

		return notification;
	}

	/**
	 * Creates a browser notification story for the specified browser host.
	 * 
	 * @param cricketNotification
	 *            notification meta for a cricket event.
	 * @param notificationData
	 *            contains other notification meta such as image.
	 * @param target
	 *            target host for the notification.
	 * @return a <code>NotificationStory</code> object representing the notification
	 *         to send.
	 */
	private NotificationStory createBrowserNotificationStory(CricketNotification cricketNotification,
			Map<?, ?> notificationData, int target) {
		Integer sl_id = getSl_Id(notificationData, target);

		if (sl_id == null) {
			return null;
		}

		String nicon = getIconUrl(target);

		LOG.info("createBrowserNotificationStory--" + cricketNotification);
		BrowserNotificationStory browserNotificationStory = new BrowserNotificationStory();
		browserNotificationStory.setSl_id(sl_id);
		browserNotificationStory.setNtitle(createTitleMessage(cricketNotification, target));
		// browserNotificationStory.setNtitle("Live Score Updates");
		browserNotificationStory.setNbody(createSlugIntroMessage(cricketNotification, target));
		browserNotificationStory.setNicon(nicon);
		browserNotificationStory.getOption().setTime_to_live(3 * 60);
		browserNotificationStory.getOption().setNdefaultLandingUrl(getLandingUrl(cricketNotification, target));
		browserNotificationStory.getOption().setNrequireInteraction(true);
		browserNotificationStory.getOption().setStoryid(String.valueOf(sl_id));
		browserNotificationStory.getOption().setNutm(
				"?ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_cricket_autobot");
		browserNotificationStory.getOption().setNimage("");
		return browserNotificationStory;
	}

	/**
	 * Creates an app notification story for the specified browser host.
	 * 
	 * @param notification
	 *            notification meta for a cricket event.
	 * @param notificationData
	 *            contains other notification meta such as image.
	 * @param target
	 *            target host for the notification.
	 * @return a <code>NotificationStory</code> object representing the notification
	 *         to send.
	 */
	private NotificationStory createAppNotificationStory(CricketNotification notification, Map<?, ?> notificationData,
			int target) {

		Integer sl_id = getSl_Id(notificationData, target);

		if (sl_id == null) {
			return null;
		}

		String nicon = getIconUrl(target);

		NotificationStory story = new NotificationStory();
		story.setMessage(notification.toNotificationString(target));
		story.setSl_id(sl_id);
		story.setImage(nicon);
		story.setWebUrl(getLandingUrl(notification, target));
		story.setPush_cat_id(7);
		return story;
	}

	/**
	 * Get the sl_id value for the given notification target.
	 * 
	 * @param notificationData
	 *            data te extract sl_id from.
	 * @param target
	 *            the target host for the notification.
	 * @return the sl_id for the given host or null if none exists.
	 */
	private Integer getSl_Id(Map<?, ?> notificationData, int target) {

		try {
			if (notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD) instanceof String) {
				return Integer.parseInt(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString());
			} else if (notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD) instanceof List) {

				List<?> sl_ids = (List<?>) notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD);

				if (Host.BHASKAR_WEB_HOST == target && sl_ids.size() >= 1) {
					return Integer.parseInt(sl_ids.get(0).toString());
				} else if (Host.BHASKAR_MOBILE_WEB_HOST == target && sl_ids.size() >= 2) {
					return Integer.parseInt(sl_ids.get(1).toString());
				} else if (Host.DIVYA_WEB_HOST == target && sl_ids.size() >= 3) {
					return Integer.parseInt(sl_ids.get(2).toString());
				} else if (Host.DIVYA_MOBILE_WEB_HOST == target && sl_ids.size() >= 4) {
					return Integer.parseInt(sl_ids.get(3).toString());
				} else if (Host.DIVYA_APP_ANDROID_HOST == target && sl_ids.size() >= 5) {
					return Integer.parseInt(sl_ids.get(4).toString());
				} /*
					 * else if (Host.DIVYA_APP_ANDROID_HOST == target && sl_ids.size() == 6) {
					 * return Integer.parseInt(sl_ids.get(5).toString()); }
					 */
			}
		} catch (Exception e) {
			LOG.warn("Null sl_id received.", e);
		}

		return null;
	}

	// public static void main(String[] args) {
	//
	// TossNotification notification = new TossNotification();
	// notification.setTossWonByName("India");
	// notification.setTeamName("South Africa");
	// notification.setTossElectedTo("bat");
	// notification.setMatch_id("184809");
	// notification.setTourName("india-in-south-africa-2018");
	// notification.setSeriesName("india-in-south-africa-2018");
	//
	// List<String> list = Arrays.asList("1000019754", "1000019755", "1000019756",
	// "1000019757");
	//
	// Map<String, Object> notificationData = new HashMap<>();
	// notificationData.put("sl_id", list);
	// notificationData.put("nicon",
	// "https://i10.dainikbhaskar.com/cricketscoreboard/image/cricket.jpg");
	// CricketNotificationService service = new CricketNotificationService();
	// service.sendNotification(notification, notificationData);
	// }
	
	public void sendBidWinNotifications(Map<String, Set<String>> notificationMap, Map<String, Object> notificationUtil) {
		List<Integer> appNotificationVendorsList = CricketGlobalVariableUtils.getAppNotificationVendorsList();
		Set<String> hosts = notificationMap.keySet();
		
		for (String host : hosts) {
			try {
				if (appNotificationVendorsList.contains(Integer.parseInt(host))) {
					sendAppNotification(notificationMap, host, notificationUtil);
				} else
					sendBrowserNotification(notificationMap, host, notificationUtil);
				
			} catch (Exception e) {
				LOG.error("Error while sending notification for host:" + host, e);
			}
		}
	}
	
	private void sendAppNotification(Map<String, Set<String>> notificationMap, String host, Map<String, Object> notificationUtil) {
		NotificationStory notificationStory = new NotificationStory();

		notificationStory.setMessage((String)notificationUtil.get(NotificationConstants.MESSAGE));
		notificationStory.setImage(Constants.Cricket.PredictWinConstants.ICON_URL);
		notificationStory.setCat_id(Integer.parseInt(notificationUtil.get(NotificationConstants.CAT_ID).toString()));
		notificationStory.setStoryURL("https://www.cricketpundit.in/");

		if(Boolean.parseBoolean((String) notificationUtil.get(NotificationConstants.DRY_RUN))){
			notificationStory.setDryRun(true);
		}

		if(notificationUtil.get(NotificationConstants.STORY_ID) != null){
			notificationStory.setStoryid((String)notificationUtil.get(NotificationConstants.STORY_ID));
		}
		
		if(notificationUtil.get(NotificationConstants.AUTO_NOTIFICATION_KEY) != null){
			notificationStory.setAutoNotificationKey((String)notificationUtil.get(NotificationConstants.AUTO_NOTIFICATION_KEY));
		}

		Map<String, List<String>> targetMap = new HashMap<>();
		List<String> usersList = new ArrayList<>();
		usersList.addAll(notificationMap.get(host));

		targetMap.put("uniqueUserId", usersList);

		notificationStory.setTarget(targetMap);
		notificationStory.setHosts(host);

		if(notificationUtil.containsKey(NotificationConstants.SLID_KEY)){
			GenericSLIDRequestBody genericSLIDRequestBody = new GenericSLIDRequestBody();

			genericSLIDRequestBody.setAuthorname("Autobot");
			genericSLIDRequestBody.setHost(host);
			genericSLIDRequestBody.setImage(notificationStory.getImage());
			genericSLIDRequestBody.setStoryURL(notificationStory.getStoryURL());
			genericSLIDRequestBody.setStoryid((String)notificationUtil.get(NotificationConstants.SLID_KEY));
			genericSLIDRequestBody.setTitle(notificationStory.getMessage());
			
			Integer slId = pushNotificationService.getSLID(genericSLIDRequestBody, notificationStory);

			notificationStory.setSl_id(slId);
			
			if(notificationUtil.get(NotificationConstants.STORY_ID) == null){
				notificationStory.setStoryid(slId.toString());
			}
		}

		pushNotificationService.setDefaultParameters(notificationStory);

		pushNotificationService.pushNotification(notificationStory,
				Integer.parseInt(host.trim()),
				NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
	}

	private void sendBrowserNotification(Map<String, Set<String>> notificationMap, String host, Map<String, Object> notificationUtil) {
		BrowserNotificationStory notificationStory = new BrowserNotificationStory();
		
		if (notificationUtil.get(NotificationConstants.STORY_ID) != null){
			notificationStory.setStoryid((String)notificationUtil.get(NotificationConstants.STORY_ID));
		}
		
		if(notificationUtil.get(NotificationConstants.AUTO_NOTIFICATION_KEY) != null){
			notificationStory.setAutoNotificationKey((String)notificationUtil.get(NotificationConstants.AUTO_NOTIFICATION_KEY));
		}
		
		notificationStory.setCat_id(Integer.parseInt(notificationUtil.get(NotificationConstants.CAT_ID).toString()));
		notificationStory.setNtitle(StringEscapeUtils
				.unescapeHtml((String)notificationUtil.get(NotificationConstants.MESSAGE)).replace("\\", ""));
		notificationStory.setNicon("https://i9.dainikbhaskar.com/notification/customimages/cricketpundit/Dainik-150x150.jpg");//todo iconf from abhilash
		notificationStory.setNbody("");
		
		if(Boolean.parseBoolean((String) notificationUtil.get(NotificationConstants.DRY_RUN))){
			notificationStory.setDryRun(true);
		}
		
		BrowserNotificationStory.BrowserNotificationOption browserNotificationOption = notificationStory.getOption();
		
		browserNotificationOption
				.setNimage("https://i9.dainikbhaskar.com/notification/customimages/cricketpundit/Dainik-360x240.jpg");// todo get url from abhilash
		browserNotificationOption.setNtag("autobot");
		browserNotificationOption.setNclickable(true);
		browserNotificationOption.setNrequireInteraction(true);
		browserNotificationOption.setNdefaultLandingUrl("https://www.cricketpundit.in/dashboard.php");
		browserNotificationOption.setNutm(
				"?ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_autobot");
		browserNotificationOption.setTime_to_live(1 * 60 * 60);

		if(notificationUtil.containsKey(NotificationConstants.SLID_KEY)){
			GenericSLIDRequestBody genericSLIDRequestBody = new GenericSLIDRequestBody();

			genericSLIDRequestBody.setAuthorname("Autobot");
			genericSLIDRequestBody.setHost(host);
			genericSLIDRequestBody.setImage(browserNotificationOption.getNimage());
			genericSLIDRequestBody.setStoryURL(browserNotificationOption.getNdefaultLandingUrl());
			genericSLIDRequestBody.setStoryid((String)notificationUtil.get(NotificationConstants.SLID_KEY));
			genericSLIDRequestBody.setTitle(notificationStory.getNtitle());

			Integer slId = pushNotificationService.getSLID(genericSLIDRequestBody, notificationStory);

			notificationStory.setSl_id(slId);
			browserNotificationOption.setStoryid(slId.toString());
		}
		notificationStory.setImage(Constants.Cricket.PredictWinConstants.ICON_URL);
		
		Map<String, List<String>> targetMap = new HashMap<>();
		List<String> usersList = new ArrayList<>();

		usersList.addAll(notificationMap.get(host));
		targetMap.put("uniqueUserId", usersList);
		
		notificationStory.setTarget(targetMap);
		notificationStory.setHosts(host);
		
		pushNotificationService.setDefaultParameters(notificationStory);

		 pushNotificationService.pushNotification(notificationStory,
				 Integer.parseInt(host.trim()),
				 	NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER);
	}

	/**
	 * Get the url of icon image per host.
	 * 
	 * @param host
	 * @return
	 */
	private String getIconUrl(int host) {
		if (host == Host.BHASKAR_WEB_HOST || host == Host.BHASKAR_MOBILE_WEB_HOST) {
			return CricketConstants.COMMENT_ICON_URL;
		} else if (host == Host.DIVYA_WEB_HOST || host == Host.DIVYA_MOBILE_WEB_HOST) {
			return CricketConstants.COMMENT_ICON_URL;
		} else {
			return CricketConstants.COMMENT_ICON_URL;
		}
	}

}
