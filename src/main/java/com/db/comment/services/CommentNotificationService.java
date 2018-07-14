package com.db.comment.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.db.comment.model.CommentNotification;
import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Host;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.notification.v1.enums.NotificationTargetType;
import com.db.notification.v1.model.AutomatedNotificationStory;
import com.db.notification.v1.model.BrowserNotificationStory;
import com.db.notification.v1.model.NotificationStory;
import com.db.notification.v1.services.PushNotificationService;

/**
 * This class handles building and sending of notification for replies received
 * on user comments.
 * 
 * @author Piyush Gupta
 */
public class CommentNotificationService {

	/**
	 * Logger instance for CommentNotificationService
	 */
	private static final Logger LOG = LogManager.getLogger(CommentNotificationService.class);

	/**
	 * Push notification service to push notification on various hosts
	 */
	private PushNotificationService pushNotificationService = new PushNotificationService();

	/**
	 * Elasticsearch client instance
	 */
	private Client client = ElasticSearchIndexService.getInstance().getClient();

	private DBConfig config = DBConfig.getInstance();

	/**
	 * Process the incoming message object and sends a notification.
	 * 
	 * @param commentMsg
	 */
	public void sendCommentNotification(Map<String, Object> commentMsg) {

		LOG.info("Comment Message: " + commentMsg);

		try {
			CommentNotification notificationMeta = processCommentRecord(commentMsg);

			if (notificationMeta.getEvent() != null && Constants.LIKE.equalsIgnoreCase(notificationMeta.getEvent())) {
				return;
			}

			BoolQueryBuilder commentQuery = QueryBuilders.boolQuery();

			/*
			 * Prepare query to get additional data for notification.
			 */

			String postId = null;

			if (!notificationMeta.getIsReply()) {
				postId = String.valueOf(notificationMeta.getPostId());
			}

			Set<String> sessionIds = new HashSet<>();

			if (StringUtils.isNotBlank(notificationMeta.getId())) {

				commentQuery.should(QueryBuilders.termQuery(Constants.ID, notificationMeta.getId()));
				commentQuery
						.should(QueryBuilders.termQuery(Constants.IN_REPLY_TO_COMMENT_ID, notificationMeta.getId()));
				commentQuery.minimumShouldMatch(1);
				commentQuery
						.mustNot(QueryBuilders.termQuery(Constants.SESSION_ID_FIELD, notificationMeta.getSessionId()));

				LOG.info("Query for Comment: " + commentQuery);

				SearchResponse searchResponse = client.prepareSearch(Indexes.DB_COMMENT)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(commentQuery).execute().actionGet();
				SearchHit[] hits = searchResponse.getHits().getHits();

				for (SearchHit hit : hits) {
					String sessId = (String) hit.getSourceAsMap().get(Constants.SESSION_ID_FIELD);
					if (StringUtils.isNotBlank(sessId) && !sessionIds.contains(sessId)) {
						sessionIds.add(sessId);
					}
				}
			}

			/* Populate a map of users to target for notification. */

			Map<String, List<String>> targetMap = new HashMap<>();
			Map<String, List<String>> excludeTargetMap = new HashMap<>();

			if (!sessionIds.isEmpty()) {
				targetMap.put("deviceId", new ArrayList<>(sessionIds));
			}

			if (notificationMeta.getSessionId() != null) {
				excludeTargetMap.put(Constants.DEVICEID, Arrays.asList(notificationMeta.getSessionId()));
			}

			if (postId != null && !postId.isEmpty()) {
				targetMap.put(Constants.FOLLOW_COMMENT, Arrays.asList(postId));
			}

			/*
			 * This object provides configuration for sending notifications.
			 */

			AutomatedNotificationStory automatedNotificationStory = new AutomatedNotificationStory();
			automatedNotificationStory.setHosts(String.valueOf(Constants.Host.BHASKAR_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_MOBILE_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_APP_ANDROID_HOST));
			automatedNotificationStory.setNotificationTargetType("BROWSER");

			NotificationStory notification = null;

			/*
			 * Prepare and send notification for each host.
			 */

			for (String host : automatedNotificationStory.getHosts().split(",")) {

				notification = prepareCommentNotification(notificationMeta, host);

				if (notification == null) {
					continue;
				}

				if (targetMap.isEmpty()) {
					return;
				} else {
					notification.setTarget(targetMap);
					notification.setExcludeTarget(excludeTargetMap);
					notification.setTargetOperator("OR");
				}

				// testing for specific id
				// targetMap = new HashMap<>();
				// targetMap.put("deviceId",
				// Arrays.asList("40f27f3b-678d-a572-9be0-c0e90a069748",
				// "2c6c96dd-ca7a-cdef-d393-231e26a33ca2",
				// "f383e4ce-7abb-9f68-d4cd-5018ea554f6c", "1354849084"));
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
				} else if (Boolean.parseBoolean(config.getProperty("notification.comment.app.enabled"))) {
					pushNotificationService.pushNotification(notification, Integer.parseInt(host.trim()),
							NotificationTargetType.NOTIFICATION_TARGET_TYPE_APP);
				}
			}
		} catch (Exception e) {
			LOG.error("Error Occurred while sending notification on comment: ", e);
		}
	}

	/**
	 * Process the comment message record to provide a
	 * <code>CommentNotification</code> object to send notification.
	 * 
	 * @param msg
	 *            the comment record.
	 * @return the <code>CommentNotification</code> representing this comment msg.
	 */
	private CommentNotification processCommentRecord(Map<String, Object> msg) {

		CommentNotification notificationMeta = new CommentNotification();

		if (StringUtils.isNotBlank((String) msg.get(Constants.STORY_ID_FIELD))) {
			notificationMeta.setStoryid(msg.get(Constants.STORY_ID_FIELD).toString());
		}

		if (StringUtils.isNotBlank((String) msg.get(Constants.EVENT))) {
			notificationMeta.setEvent(msg.get(Constants.EVENT).toString());
		}

		if (StringUtils.isNotBlank((String) msg.get(Constants.NAME))) {
			notificationMeta.setUserName((String) msg.get(Constants.NAME));
		}

		if (StringUtils.isNotBlank((String) msg.get(Constants.URL))) {
			notificationMeta.setUrl((String) msg.get(Constants.URL));
		}

		if (StringUtils.isNotBlank((String) msg.get(Constants.IN_REPLY_TO_COMMENT_ID))) {
			notificationMeta.setId((String) msg.get(Constants.IN_REPLY_TO_COMMENT_ID));
		}

		if (StringUtils.isNotBlank((String) msg.get(Constants.SESSION_ID_FIELD))) {
			notificationMeta.setSessionId((String) msg.get(Constants.SESSION_ID_FIELD));
		}

		if (StringUtils.isNotBlank(String.valueOf(msg.get(Constants.POST_ID)))) {
			notificationMeta.setPostId((Integer) msg.get(Constants.POST_ID));
		}

		if (msg.containsKey(Constants.IS_REPLY)) {
			notificationMeta.setIsReply(Boolean.parseBoolean(String.valueOf(msg.get(Constants.IS_REPLY))));
		}

		if (!notificationMeta.getIsReply()) {
			notificationMeta.setMsgType("follow");
			notificationMeta.setId((String) msg.get(Constants.ID));
		}

		return notificationMeta;
	}

	/**
	 * Prepare a <code>NotificationStory</code> for the comment reply received.
	 * 
	 * @param notificationMeta
	 *            the meta for this notification.
	 * @param target
	 *            the target host for this notification.
	 * @return the <code>NotificationStory</code> for this reply.
	 */
	private NotificationStory prepareCommentNotification(CommentNotification notificationMeta, String target) {

		LOG.info("Notification Meta: " + notificationMeta);
		int host = Integer.parseInt(target);

		NotificationStory notification = null;

		switch (host) {

		case Host.BHASKAR_WEB_HOST:
		case Host.BHASKAR_MOBILE_WEB_HOST:
		case Host.DIVYA_WEB_HOST:
		case Host.DIVYA_MOBILE_WEB_HOST:

			notification = createBrowserNotificationStory(notificationMeta);
			break;
		case Host.BHASKAR_APP_ANDROID_HOST:
		case Host.DIVYA_APP_ANDROID_HOST:

			notification = createAppNotificationStory(notificationMeta);
			break;
		}

		return notification;
	}

	/**
	 * Create a browser notification for the given meta
	 * 
	 * @param notificationMeta
	 *            the notification meta
	 * @param target
	 *            the target host
	 * @return the browser notification story
	 */
	private NotificationStory createBrowserNotificationStory(CommentNotification notificationMeta) {
		BrowserNotificationStory browserNotificationStory = new BrowserNotificationStory();
		browserNotificationStory.setSl_id(1000004917);
		browserNotificationStory.setStoryid(1511162840 + "");
		browserNotificationStory.setNtitle(createTitleMessage(notificationMeta));
		browserNotificationStory.setNbody(createSlugIntroMessage(notificationMeta));
		browserNotificationStory.setNicon(getCommentNotificationtIcon());
		browserNotificationStory.getOption().setTime_to_live(3 * 60);
		browserNotificationStory.getOption().setNdefaultLandingUrl(notificationMeta.getUrl());
		browserNotificationStory.getOption().setNrequireInteraction(true);
		browserNotificationStory.getOption().setStoryid(String.valueOf(1000004917));
		browserNotificationStory.getOption().setNimage("");
		browserNotificationStory.getOption().setNutm("?commentid=" + notificationMeta.getId()
				+ "&ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_autobot_comment");
		LOG.info("commentIdinUrl" + notificationMeta.getId());
		LOG.info("Browser Notification Story: " + browserNotificationStory);
		return browserNotificationStory;
	}

	/**
	 * Create a <code>NotificationStory</code> for App Notifications for Comment.
	 * 
	 * @param notificationMeta
	 * @return A <code>NotificationStory</code> instance or null if story id is
	 *         null.
	 */
	private NotificationStory createAppNotificationStory(CommentNotification notificationMeta) {

		if (notificationMeta.getStoryid() == null) {
			return null;
		}

		NotificationStory story = new NotificationStory();
		story.setMessage(createTitleMessage(notificationMeta));
		story.setSl_id(1000004917);
		story.setStoryid(notificationMeta.getStoryid());
		story.setImage(getCommentNotificationtIcon());
		story.setStoryURL(notificationMeta.getUrl());
		return story;
	}

	String getCommentNotificationtIcon() {
		return pushNotificationService.getIcon(1, null);
	}

	String createSlugIntroMessage(CommentNotification commentNotification) {
		return commentNotification.slugIntro();
	}

	String createTitleMessage(CommentNotification commentNotification) {
		if (commentNotification != null) {
			if (StringUtils.equals(commentNotification.getMsgType(), "follow")) {
				return commentNotification.toFollowNotificationString();
			} else if (StringUtils.equals(commentNotification.getMsgType(), "like")) {
				return commentNotification.toLikeCommentNotificationString();
			} else {
				return commentNotification.toNotificationString();
			}
		}
		return null;
	}

	public static void main(String[] args) {

		CommentNotificationService service = new CommentNotificationService();

		Map<String, Object> comment = new HashMap<>();

		comment.put("storyid", "121331020");

		comment.put("image", "https://graph.facebook.com/509834059368851/picture");

		comment.put("description",
				"All Type of Astrological Advises relating to the problems +91-9928972179 {{ मनचाहा प्यार }}{{ काम-कारोबार }}{{ पति - पत्नी में अनबन }}   chat whatsp +91-9928972179\r\n"
						+ "Love problems solution , love breakup solution,\r\n"
						+ "Love marriage, Advice Your Love Life +91-9928972179Whatspp Available}\r\n"
						+ "Husband- wife disturbance ,Control your lover, \r\n"
						+ "Busines Problems solution ,It is related to Love \r\n"
						+ "Astrology problems or any other like Love\r\n"
						+ "Family problems ,Solve Your All Type Problems");

		comment.put("session_id", "1337778985");

		comment.put("ip_address", null);

		comment.put("title", "LIVE: इस्लाम की विरासत को बता नहीं सकते, इसे सिर्फ महसूस किया जाता है- नरेंद्");

		comment.put("isByAnonymousUser", false);

		comment.put("url",
				"/indian-national-news-in-hindi/news/NAT-NAN-HDLN-pm-modi-and-jordan-king-speech-on-islamic-heritage-in-delhi-5822162-NOR.html");

		comment.put("isAgree", true);

		comment.put("post_id", 1940891035);

		comment.put("user_id", "509834059368851");

		comment.put("name", "Ankit Kumar Sharma");

		comment.put("host", 16);

		comment.put("modified_description",
				"All Type of Astrological Advises relating to the problems +91-9928972179 {{ मनचाहा प्यार }}{{ काम-कारोबार }}{{ पति - पत्नी में अनबन }}   chat whatsp +91-9928972179\r\n"
						+ "Love problems solution , love breakup solution,\r\n"
						+ "Love marriage, Advice Your Love Life +91-9928972179Whatspp Available}\r\n"
						+ "Husband- wife disturbance ,Control your lover, \r\n"
						+ "Busines Problems solution ,It is related to Love \r\n"
						+ "Astrology problems or any other like Love\r\n"
						+ "Family problems ,Solve Your All Type Problems");

		comment.put("id", "781821b3162e96732bfcdea3becfe223");
		comment.put("email", "");
		comment.put("isReply", false);
		comment.put("channel_slno", 521);
		comment.put("status", 1);
		comment.put("reply_count", 3);

		service.sendCommentNotification(comment);

		Map<String, Object> commentReply = new HashMap<>();

		commentReply.put("storyid", "121331020");

		commentReply.put("image", "https://graph.facebook.com/148107572500367/picture");

		commentReply.put("description", "good");

		commentReply.put("session_id", "1356351587");

		commentReply.put("ip_address", null);

		commentReply.put("title", "LIVE: इस्लाम की विरासत को बता नहीं सकते, इसे सिर्फ महसूस किया जाता है- नरेंद्");

		commentReply.put("isByAnonymousUser", false);

		commentReply.put("url",
				"/indian-national-news-in-hindi/news/NAT-NAN-HDLN-pm-modi-and-jordan-king-speech-on-islamic-heritage-in-delhi-5822162-NOR.html");

		commentReply.put("isAgree", true);

		commentReply.put("post_id", 1940891035);

		commentReply.put("user_id", "148107572500367");

		commentReply.put("name", "Raj Jangir");

		commentReply.put("in_reply_to_user_id", "509834059368851");

		commentReply.put("in_reply_to_comment_id", "781821b3162e96732bfcdea3becfe223");

		commentReply.put("host", 16);

		commentReply.put("modified_description", "good");

		commentReply.put("id", "354f2348b2785f44e4035498464f7b5c");
		commentReply.put("email", "rajjangir7523@gmail.com");
		commentReply.put("isReply", true);
		commentReply.put("channel_slno", 521);
		commentReply.put("status", 1);
		commentReply.put("reply_count", 3);
		service.sendCommentNotification(commentReply);
	}

}
