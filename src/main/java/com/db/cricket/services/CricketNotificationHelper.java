package com.db.cricket.services;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.Target;
import com.db.common.constants.Indexes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.KeyGenerator;
import com.db.cricket.model.CricketNotification;
import com.db.notification.v1.enums.NotificationTargetType;
import com.db.notification.v1.model.AutomatedNotificationStory;
import com.db.notification.v1.model.BrowserNotificationStory;
import com.db.notification.v1.model.NotificationStory;
import com.db.notification.v1.services.PushNotificationService;

/**
 * Helper Class for preparing and sending notifications.
 *
 * @author Satya
 * @since 20-09-2017
 */
@Component
public class CricketNotificationHelper {

	@Autowired
	private PushNotificationService pushNotificationService = new PushNotificationService();

	private static final Logger log = LogManager.getLogger(CricketNotificationHelper.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private DBConfig config = DBConfig.getInstance();

	@SuppressWarnings("unchecked")
	void sendNotification(CricketNotification cricketNotification, Map<?, ?> notificationData) {

		try {
			log.info("sendNotification--" + cricketNotification);
			AutomatedNotificationStory automatedNotificationStory = new AutomatedNotificationStory();
			automatedNotificationStory.setHosts(String.valueOf(Constants.Host.BHASKAR_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_MOBILE_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_MOBILE_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_APP_ANDROID_HOST) + ","
					+ String.valueOf(Constants.Host.DIVYA_APP_ANDROID_HOST));
			automatedNotificationStory.setNotificationTargetType("BROWSER");

			if (notificationData == null || notificationData.isEmpty()) {
				log.error("Received invalid notification details.");
				return;
			}

			Map<?, ?> target = (Map<?, ?>) notificationData.get(CricketConstants.NOTIFICATION_TARGET_FIELD);

			if (notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD) == null || StringUtils
					.isBlank(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString())) {
				log.error("Received Invalid notification data.");
				return;
			}

			/*
			 * Check if the notification contains null as string. If it does, abort
			 * notification.
			 */
			if (cricketNotification.toNotificationString().contains("null")) {
				log.error("Received null in Notification.");
				return;
			} else if (cricketNotification.slugIntro().contains("null")) {
				log.error("Received null in Notification.");
				return;
			}

			NotificationStory notificationStory = prepareCricketNotificationStory(cricketNotification,
					automatedNotificationStory, notificationData);

			NotificationStory divyaNotificationStory = prepareCricketNotificationStoryForDivya(cricketNotification,
					automatedNotificationStory, notificationData);

			NotificationStory appNotification = createAppNotificationStory(cricketNotification, notificationData,
					Target.DIVYA_BHASKAR_WAP);

			NotificationStory bhaskarAppNotification = createAppNotificationStory(cricketNotification, notificationData,
					Target.BHASKAR_WAP);

			if (target != null && !target.isEmpty()) {

				log.info(target);
				Map<String, List<String>> targetMap = new HashMap<>();

				targetMap.put(CricketConstants.NOTIFICATION_FOLLOW_TAG_FIELD,
						(List<String>) target.get(CricketConstants.NOTIFICATION_FOLLOW_TAG_FIELD));
				notificationStory.setTarget(targetMap);
				divyaNotificationStory.setTarget(targetMap);
			}

			// testing for specific id
			// Map<String, List<String>> targetMap = new HashMap<>();
			// targetMap.put("deviceId", Arrays.asList("1354852351", "1354849084"));
			// notificationStory.setTarget(targetMap);
			// divyaNotificationStory.setTarget(targetMap);
			// appNotification.setTarget(targetMap);
			// bhaskarAppNotification.setTarget(targetMap);
			// notificationStory.setDryRun(true);

			for (String host : automatedNotificationStory.getHosts().split(",")) {

				if (host.equalsIgnoreCase(String.valueOf(Constants.Host.DIVYA_MOBILE_WEB_HOST))
						|| host.equalsIgnoreCase(String.valueOf(Constants.Host.DIVYA_WEB_HOST))) {
					log.info(divyaNotificationStory + " pushing notification to host " + host);
					divyaNotificationStory.setHosts(host);
					pushNotificationService.pushNotification(divyaNotificationStory, Integer.parseInt(host.trim()),
							NotificationTargetType
									.getEnumConstant(automatedNotificationStory.getNotificationTargetType()));
				} else if (host.equalsIgnoreCase(String.valueOf(Constants.Host.DIVYA_APP_ANDROID_HOST))
						&& Boolean.parseBoolean(config.getProperty("cricket.notification.app.enabled"))) {
					log.info("Divya App Notification: " + appNotification);
					appNotification.setHosts(host);
					pushNotificationService.pushNotification(appNotification, Integer.parseInt(host.trim()),
							NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
				} else if (host.equalsIgnoreCase(String.valueOf(Constants.Host.BHASKAR_APP_ANDROID_HOST))
						&& Boolean.parseBoolean(config.getProperty("cricket.notification.app.enabled"))) {
					log.info("Bhaskar App Notification: " + bhaskarAppNotification);
					bhaskarAppNotification.setHosts(host);
					pushNotificationService.pushNotification(bhaskarAppNotification, Integer.parseInt(host.trim()),
							NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
				} else {
					log.info(notificationStory + " pushing notification to host " + host);
					notificationStory.setHosts(host);
					pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),
							NotificationTargetType
									.getEnumConstant(automatedNotificationStory.getNotificationTargetType()));
				}
			}

		} catch (Exception e) {
			log.error("Some error occured while sending notification.", e);
		}
	}

	/**
	 * Prepare a notification story object representing a cricket notification.
	 *
	 * @param cricketNotification
	 *            the cricket notification to send
	 * @param automatedNotificationStory
	 *            the automated notifications story to send
	 * @param sl_id
	 *            tracking id for notifications
	 * @return a prepared notification story representing the cricket notification.
	 */
	NotificationStory prepareCricketNotificationStory(CricketNotification cricketNotification,
			AutomatedNotificationStory automatedNotificationStory, Map<?, ?> notificationData) {
		log.info("prepareCricketNotificationStory--" + automatedNotificationStory);
		NotificationStory notificationStory = null;
		if (NotificationTargetType.getEnumConstant(automatedNotificationStory
				.getNotificationTargetType()) == NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER) {
			notificationStory = createBrowserNotificationStory(cricketNotification, notificationData);
		}
		pushNotificationService.setDefaultParameters(notificationStory);
		return notificationStory;
	}

	NotificationStory prepareCricketNotificationStoryForDivya(CricketNotification cricketNotification,
			AutomatedNotificationStory automatedNotificationStory, Map<?, ?> notificationData) {
		log.info("prepareCricketNotificationStory--" + automatedNotificationStory);
		NotificationStory notificationStory = null;
		if (NotificationTargetType.getEnumConstant(automatedNotificationStory
				.getNotificationTargetType()) == NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER) {
			notificationStory = createBrowserNotificationStoryForDivya(cricketNotification, notificationData);
		}
		pushNotificationService.setDefaultParameters(notificationStory);
		return notificationStory;
	}

	/**
	 * Prepare the notification title
	 *
	 * @param cricketNotification
	 * @return the notification title
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

	NotificationStory createBrowserNotificationStory(CricketNotification cricketNotification,
			Map<?, ?> notificationData) {
		int sl_id = Integer.parseInt(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString());
		String nicon = (String) notificationData.get(CricketConstants.NOTIFICATION_ICON_FIELD);

		log.info("createBrowserNotificationStory--" + cricketNotification);
		BrowserNotificationStory browserNotificationStory = new BrowserNotificationStory();
		browserNotificationStory.setSl_id(sl_id);
		browserNotificationStory.setNtitle(createTitleMessage(cricketNotification));
		// browserNotificationStory.setNtitle("Live Score Updates");
		browserNotificationStory.setNbody(createSlugIntroMessage(cricketNotification));
		browserNotificationStory.setNicon(nicon);
		browserNotificationStory.getOption().setTime_to_live(3 * 60);
		browserNotificationStory.getOption().setNdefaultLandingUrl(getLandingURL(cricketNotification));
		browserNotificationStory.getOption().setNrequireInteraction(true);
		browserNotificationStory.getOption().setStoryid(String.valueOf(sl_id));
		// browserNotificationStory.getOption().setNimage(getCricketIcon());
		browserNotificationStory.getOption().setNutm(
				"?ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_cricket_autobot");
		browserNotificationStory.getOption().setNimage("");
		return browserNotificationStory;
	}

	NotificationStory createBrowserNotificationStoryForDivya(CricketNotification cricketNotification,
			Map<?, ?> notificationData) {
		int sl_id = Integer.parseInt(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString());
		String nicon = (String) notificationData.get(CricketConstants.NOTIFICATION_ICON_FIELD);

		log.info("createBrowserNotificationStory--" + cricketNotification);
		BrowserNotificationStory browserNotificationStory = new BrowserNotificationStory();
		browserNotificationStory.setSl_id(sl_id);
		browserNotificationStory.setNtitle(createTitleMessage(cricketNotification));
		// browserNotificationStory.setNtitle("Live Score Updates");
		browserNotificationStory.setNbody(createSlugIntroMessage(cricketNotification));
		browserNotificationStory.setNicon(nicon);
		browserNotificationStory.getOption().setTime_to_live(3 * 60);
		browserNotificationStory.getOption().setNdefaultLandingUrl(getLandingUrlForDivya(cricketNotification));
		browserNotificationStory.getOption().setNrequireInteraction(true);
		browserNotificationStory.getOption().setStoryid(String.valueOf(sl_id));
		// browserNotificationStory.getOption().setNimage(getCricketIcon());
		browserNotificationStory.getOption().setNutm(
				"?ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_cricket_autobot");
		browserNotificationStory.getOption().setNimage("");
		return browserNotificationStory;
	}

	String getLandingURL(CricketNotification cricketNotification) {
		return pushNotificationService
				.getBasicURL("https://www.bhaskar.com/cric-score/" + cricketNotification.getMatch_id() + "/"
						+ replacePunct(cricketNotification.getTourName().toLowerCase()) + "/");
	}

	String getLandingUrlForDivya(CricketNotification cricketNotification) {
		return pushNotificationService
				.getBasicURL("https://www.divyabhaskar.co.in/cric-score/" + cricketNotification.getMatch_id() + "/"
						+ replacePunct(cricketNotification.getTourName().toLowerCase()) + "/");
	}

	static String replacePunct(String string) {
		return string == null ? string : string.replace(",", "").replaceAll("\\s|/", "-");
	}

	public void sendComment(CricketNotification notification) {

		if (notification.toCommentDescription().contains("null")) {
			log.error("Received null in notification.");
			return;
		}

		String key = KeyGenerator.getKey();
		Map<String, Object> commentMap = new HashMap<>();
		commentMap.put(Constants.POST_ID, postHashCode("/cric-score/" + notification.getMatch_id() + "/"
				+ replacePunct(notification.getTourName().toLowerCase()) + "/"));
		commentMap.put("user_id", "db@dainikbhaskar.com");
		commentMap.put("email", "db@dainikbhaskar.com");
		commentMap.put(Constants.DESCRIPTION, notification.toCommentDescription());
		commentMap.put(Constants.MODIFIED_DESCRIPTION, notification.toCommentDescription());
		commentMap.put(Constants.CHANNEL_SLNO, 521);
		commentMap.put("name", CricketConstants.CRICKET_COMMENT_USER_NAME);
		commentMap.put(Constants.IMAGE, "https://i10.dainikbhaskar.com/cricketscoreboard/image/cricket.jpg");
		commentMap.put(Constants.URL, getLandingURL(notification));
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

		log.info("Outgoinng Comment: " + commentMap);

		elasticSearchIndexService.index(Indexes.DB_COMMENT, commentMap);
	}

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

	NotificationStory createAppNotificationStory(CricketNotification notification, Map<?, ?> notificationData,
			int target) {

		int sl_id = Integer.parseInt(notificationData.get(CricketConstants.NOTIFICATION_SL_ID_FIELD).toString());
		String nicon = (String) notificationData.get(CricketConstants.NOTIFICATION_ICON_FIELD);

		NotificationStory story = new NotificationStory();
		story.setMessage(notification.toNotificationString());
		story.setStoryid("");
		story.setSl_id(sl_id);
		// story.setImage(nicon);
		story.setPush_cat_id(7);
		story.setWebUrl(getLandingUrl(notification, target));
		return story;
	}

	/**
	 * Given the notification and target, provide a landing url for when the user
	 * clicks on the notification.
	 * 
	 * @param notification
	 *            the cricket notification to use.
	 * @param target
	 *            the target
	 * @return
	 */
	String getLandingUrl(CricketNotification notification, int target) {
		if (target == Target.DIVYA_BHASKAR_WAP) {
			return pushNotificationService.getBasicURL("https://m.divyabhaskar.co.in/cric-score/"
					+ notification.getMatch_id() + "/" + replacePunct(notification.getTourName().toLowerCase()) + "/");
		} else if (target == Target.DIVYA_BHASKAR_WEB) {
			return pushNotificationService.getBasicURL("https://www.divyabhaskar.co.in/cric-score/"
					+ notification.getMatch_id() + "/" + replacePunct(notification.getTourName().toLowerCase()) + "/");
		} else if (target == Target.BHASKAR_WAP) {
			return pushNotificationService.getBasicURL("https://m.bhaskar.com/cric-score/" + notification.getMatch_id()
					+ "/" + replacePunct(notification.getTourName().toLowerCase()) + "/");
		} else {
			return pushNotificationService.getBasicURL("https://www.bhaskar.com/cric-score/"
					+ notification.getMatch_id() + "/" + replacePunct(notification.getTourName().toLowerCase()) + "/");
		}
	}

	// public static void main(String[] args) {
	// System.out.println("Do Not Help me ");
	// System.out.println(
	// new
	// CricketNotificationHelper().postHashCode("/cric-score/184285/australia-tour-of-india-2017/"));
	//
	// TossNotification notification = new TossNotification();
	// notification.setTossWonByName("India");
	// notification.setTeamName("Australia");
	// notification.setTossElectedTo("bat");
	// notification.setMatch_id("184284");
	// notification.setTourName("australia-tour-of-india-2017");
	//
	// new CricketNotificationHelper().sendComment(notification);
	// }
}
