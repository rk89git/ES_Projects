package com.db.comment.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;

import com.db.comment.model.CommentNotification;
import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.notification.v1.enums.NotificationTargetType;
import com.db.notification.v1.model.AutomatedNotificationStory;
import com.db.notification.v1.model.BrowserNotificationStory;
import com.db.notification.v1.model.NotificationStory;
import com.db.notification.v1.services.PushNotificationService;

public class CommentNotificationHelper {

	@Autowired
	private PushNotificationService pushNotificationService = new PushNotificationService();
	private Client client = ElasticSearchIndexService.getInstance().getClient();
	private static Logger log = LogManager.getLogger(CommentNotificationHelper.class);
	
	public void
	sendNotification(Map<String, Object> msg) {
		try {
			AutomatedNotificationStory automatedNotificationStory = new AutomatedNotificationStory();
			automatedNotificationStory.setHosts(String.valueOf(Constants.Host.BHASKAR_WEB_HOST) + ","
					+ String.valueOf(Constants.Host.BHASKAR_MOBILE_WEB_HOST));
			automatedNotificationStory.setNotificationTargetType("BROWSER");
			
			CommentNotification commentNotification = new CommentNotification();
			ArrayList<String> sessionIds = new ArrayList<>();
			Map<String, List<String>> targetMap = new HashMap<>();
			Map<String, List<String>> excludeTargetMap = new HashMap<>();
			BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
			
			String postIds = null;
		
			//boolQuery in case of like 
			log.info("recordMessage"+msg);
			if((msg.containsKey(Constants.EVENT)) && (StringUtils.equals((String) msg.get(Constants.EVENT), Constants.LIKE))){			
				boolQueryBuilder.must(QueryBuilders.termQuery(Constants.ID, msg.get(Constants.ID)));				
				log.info("boolQueryForLike"+boolQueryBuilder);
				commentNotification.setMsgType("like");
			}
			else{
			commentNotification.setUserName((String) msg.get(Constants.NAME));
			commentNotification.setUrl((String) msg.get(Constants.URL));
			
			String commentId = (String) msg.get(Constants.IN_REPLY_TO_COMMENT_ID);			
			commentNotification.setId(commentId);			

			if ((boolean)msg.get(Constants.IS_REPLY) == false) {
				commentNotification.setMsgType("follow");
				postIds = String.valueOf(msg.get(Constants.POST_ID));
				commentNotification.setId((String)msg.get(Constants.ID));
				log.info("commentIdForUrl : "+(String)msg.get(Constants.ID));
				log.info("postidOfComment" + postIds);
			}
			if (StringUtils.isNotBlank(commentId)) {
			boolQueryBuilder.should(QueryBuilders.termQuery(Constants.ID, commentId));			
			boolQueryBuilder.should(QueryBuilders.termQuery(Constants.IN_REPLY_TO_COMMENT_ID, commentId));
			boolQueryBuilder.minimumShouldMatch(1);
			boolQueryBuilder.mustNot(QueryBuilders.termQuery(Constants.SESSION_ID_FIELD, msg.get(Constants.SESSION_ID_FIELD)));									
			log.info("boolQueryBuilder"+boolQueryBuilder);
			SearchResponse searchResponse = client.prepareSearch(Indexes.DB_COMMENT)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQueryBuilder).execute().actionGet();			
				SearchHit[] result = searchResponse.getHits().getHits();
								
				for (SearchHit hits : result) {					
					String sessId = (String) hits.getSource().get(Constants.SESSION_ID_FIELD);					
					if(StringUtils.isNotBlank(sessId)&&!sessionIds.contains(sessId)){
						sessionIds.add(sessId);
				   	}
				}
				
				}
			}

				if(!sessionIds.isEmpty()){
					targetMap.put("deviceId", sessionIds);
				}	
				if(msg.get(Constants.SESSION_ID_FIELD)!=null) {
                    excludeTargetMap.put(Constants.DEVICEID, Arrays.asList((String)msg.get(Constants.SESSION_ID_FIELD)));
                }
				if(postIds!=null&&!postIds.isEmpty()){
				targetMap.put(Constants.FOLLOW_COMMENT, Arrays.asList(postIds));				
				}
			    log.info("target map "+targetMap);
				log.info("exclude target map"+excludeTargetMap);
			    
			log.info("sendNotification--" + commentNotification);			
			NotificationStory notificationStory = prepareCommentNotificationStory(commentNotification,automatedNotificationStory);			    
			
			if (targetMap.isEmpty()) {
				return;
			} else {
			notificationStory.setTarget(targetMap);
			notificationStory.setExcludeTarget(excludeTargetMap);
			notificationStory.setTargetOperator("OR");
			}
			// notificationStory.setDryRun(true);
			for (String host : automatedNotificationStory.getHosts().split(",")) {
				notificationStory.setHosts(host);
				log.info(notificationStory + " Pushing notification to host " + host);
				pushNotificationService.pushNotification(notificationStory, Integer.parseInt(host.trim()),						
						NotificationTargetType.getEnumConstant(automatedNotificationStory.getNotificationTargetType()));                        
			}
		} catch (Exception e) {
			log.error("Some error occured while sending notification.", e);
		}
	}

	// check verifies if target type is browser
	NotificationStory prepareCommentNotificationStory(CommentNotification commentNotification,
			AutomatedNotificationStory automatedNotificationStory) {
		log.info("prepareCommentNotificationStory--" + automatedNotificationStory);

		NotificationStory notificationStory = null;
		if (NotificationTargetType.getEnumConstant(automatedNotificationStory
				.getNotificationTargetType()) == NotificationTargetType.NOTIFICATION_TARGET_TYPE_BROWSER) {
			notificationStory = createBrowserNotificationStory(commentNotification);			
		}
		pushNotificationService.setDefaultParameters(notificationStory);
		
		return notificationStory;
	}

	String createTitleMessage(CommentNotification commentNotification) {
		if (commentNotification != null) {
			if(StringUtils.equals(commentNotification.getMsgType(), "follow")){
				return commentNotification.toFollowNotificationString();	
			}
			else if(StringUtils.equals(commentNotification.getMsgType(), "like")){
				return commentNotification.toLikeCommentNotificationString();	
			}			
			else{
				return commentNotification.toNotificationString();		
			}
		}
		return null;
	}
	
	String createSlugIntroMessage(CommentNotification commentNotification) {
		return commentNotification.slugIntro();
	}

	NotificationStory createBrowserNotificationStory(CommentNotification commentNotification) {
		log.info("createBrowserNotificationStory--" + commentNotification);
		BrowserNotificationStory browserNotificationStory = new BrowserNotificationStory();
		browserNotificationStory.setSl_id(1000004917);
		browserNotificationStory.setStoryid(1511162840+"");
		browserNotificationStory.setNtitle(createTitleMessage(commentNotification));		
		browserNotificationStory.setNbody(createSlugIntroMessage(commentNotification));		
		browserNotificationStory.setNicon(getCommentNotificationtIcon());
		browserNotificationStory.getOption().setTime_to_live(3 * 60);		
		browserNotificationStory.getOption().setNdefaultLandingUrl(commentNotification.getUrl());
		browserNotificationStory.getOption().setNrequireInteraction(true);		
		browserNotificationStory.getOption().setStoryid(String.valueOf(1000004917));
	    browserNotificationStory.getOption().setNimage("");		
		browserNotificationStory.getOption().setNutm(
				  "?commentid="+commentNotification.getId()+"&ref=dbnotify&utm_source=browser&utm_medium=push_notification&utm_campaign=db_notifications_autobot_comment"
				  );		 
		log.info("commentIdinUrl"+commentNotification.getId());
		return browserNotificationStory;
	}

	String getCommentNotificationtIcon() {
		return pushNotificationService.getIcon(1,null);
	}

	static String replacePunct(String string) {
		return string == null ? string : string.replace(",", "").replaceAll("\\s|/", "-");
	}

	public static void main(String[] args) {
		CommentNotificationHelper obj = new CommentNotificationHelper();
		Map<String, Object> mp = new HashMap<>();
		obj.sendNotification(mp);
		//System.out.println("data in map :"+mp);
	}

}
