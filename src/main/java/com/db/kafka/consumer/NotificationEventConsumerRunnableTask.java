package com.db.kafka.consumer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Host;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.kafka.codecs.KafkaMessageDecoder;

@Configuration
@EnableScheduling
public class NotificationEventConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(NotificationEventConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	private int threadIndex = 0;
	private AtomicInteger in = new AtomicInteger(0);
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listNotificationEvent = new ArrayList<Map<String, Object>>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = getBatchSize();
	
	private long batchSleepInterval = 0;

	private ObjectMapper mapper = new ObjectMapper();

	/*
	 * Maps t o handle local counters for various session ids.
	 */
	Map<String, Integer> rowIdToClickCount = new HashMap<>();
	Map<String, Integer> rowIdToImpressionCount = new HashMap<>();
	Map<String, Integer> webRowIdToClickCount = new HashMap<>();
	Map<String, Integer> webRowIdToImpressionCount = new HashMap<>();
	
	private int getBatchSize() {
		if (config.getProperty("comment.consumer.batch.size") != null)
			return Integer.valueOf(config.getProperty("comment.consumer.batch.size"));
		else
			return Integer.valueOf(config.getProperty("consumer.batch.size"));
	}

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public NotificationEventConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);
		
		if(StringUtils.isNotBlank(DBConfig.getInstance().getProperty("kafka.batch.sleep.interval.ms"))){
			batchSleepInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.batch.sleep.interval.ms"));
		}
	}

	public NotificationEventConsumerRunnableTask() {
	}

	public void shutdown() {
		consumer.wakeup();
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(topics);
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, byte[]> record : records) {
					processMessage(record.value());
				}
			}
		} catch (WakeupException e) {
			// Inform Shutdown
			log.info(getClass().getName() + "-threadNumer " + threadIndex + " is going to shutdown.");
		} finally {
			consumer.close();
		}
	}

	private void processMessage(byte[] message) {
//		log.info("NOTIFICATION EVENT recieved records by consumer :CASE 3333 ");
		Map<String, Object> inputRecord = null;
		try {
		    threadRecordCount=in.getAndIncrement();
			inputRecord = decoder.decode(message);
			Map<String, Object> record = new HashMap<>();
			
			inputRecord = convertInputRecordFormat(inputRecord);
			
			//log.info("recieved records by consumer : " + inputRecord);
			Date date = new Date();
			SimpleDateFormat notificationSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String eventDateTime = notificationSDF.format(date);
			String storyID = "";

			if(inputRecord.containsKey("web_storyid")){
				storyID = inputRecord.get("web_storyid").toString();
			}else if(inputRecord.get("storyid") != null){
				String inputStoryID = inputRecord.get("storyid").toString();
				storyID = inputStoryID.split("00001")[0];
			}
			
			if(inputRecord.get("p_sessionID") != null && StringUtils.isNotBlank(inputRecord.get("p_sessionID").toString())){
				String rowId = inputRecord.get("p_sessionID").toString() + "_" + storyID;
				String userId = inputRecord.get("p_sessionID").toString();
				String index  = Indexes.APP_USER_PROFILE;

				if(inputRecord.containsKey("index")){
					index = inputRecord.get("index").toString();
				}

				int eventType = 0;
				String inputEventType = (String)inputRecord.get("event_type");
				if (inputEventType != null) {
					eventType = Integer.parseInt(inputEventType);
				} else {
					eventType = 0;
				}
				if (eventType == 0) {
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_FIELD, "invalidImpression");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_DATETIME_FIELD, eventDateTime);
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_FIELD, "invalidClick");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_DATETIME_FIELD, eventDateTime);
				} else if (eventType == 1) {
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_FIELD, "impression");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_DATETIME_FIELD, eventDateTime);
					
					if(index.equals(Indexes.USER_PERSONALIZATION_STATS)){
						incrementLocalCounter(userId, webRowIdToImpressionCount);
					}else{
						incrementLocalCounter(userId, rowIdToImpressionCount);
					}
					
				} else if (eventType == 2) {
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_FIELD, "click");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_DATETIME_FIELD, eventDateTime);
					
					if(index.equals(Indexes.USER_PERSONALIZATION_STATS)){
						incrementLocalCounter(userId, webRowIdToClickCount);
					}else{
						incrementLocalCounter(userId, rowIdToClickCount);
					}
					
				} else if (eventType == 3) {
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_FIELD, "impression");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_IMPRESSION_DATETIME_FIELD, eventDateTime);
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_FIELD, "click");
					record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_CLICK_DATETIME_FIELD, eventDateTime);
					
					if(index.equals(Indexes.USER_PERSONALIZATION_STATS)){
						incrementLocalCounter(userId, webRowIdToImpressionCount);
						incrementLocalCounter(userId, webRowIdToClickCount);
					}else{
						incrementLocalCounter(userId, rowIdToImpressionCount);
						incrementLocalCounter(userId, rowIdToClickCount);
					}
				}
				record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_SUB_CAT_ID_FIELD, null);
				record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_STORY_TYPE_FIELD, null);
				record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_APP_ID_FIELD, (String) inputRecord.get("app_id"));
				record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_DOMAIN_TYPE_FIELD,
						(String) inputRecord.get("domaintype"));
				record.put(Constants.NotificationConstants.DIVYA_NOTIFICATION_WIDGET_NAME_FIELD,
						(String) inputRecord.get("widget_name"));
				record.put(Constants.NotificationConstants.DIVYA_ROW_ID_FILED, rowId);
				
				if(inputRecord.get(Constants.HOST) != null){
					record.put(Constants.HOST, inputRecord.get("host"));
				}
				//record.put(Constants.STORY_ID_FIELD, storyID);
				record.put(Constants.USER_ID, userId);
				record.put(Constants.SESSION_ID_FIELD, userId);

				listNotificationEvent.add(record);
				// Start: Index the data into table
				if (threadRecordCount % batchSize == 0) {
					log.info("batchSize " + batchSize);
					insertData();

					try{
						Thread.sleep(batchSleepInterval);
					}catch(Exception e){
						log.info("Batch sleep Interval exit");
					}
				}

				if (System.currentTimeMillis() - startTime >= flushInterval) {
					log.info("Thread " + threadIndex
							+ ", Notification Detail event update API Flushing the Data in commenting indexes..");
					insertData();
				}
				//log.info("Notification Detail event update API notifyEvent. SUCESSFULLY PROCESSED");
			}
			// End: Index the raw data in realtime table
		} catch (Exception e) {
			log.error("Notification payload-"+inputRecord);
			log.error("ERROR in Notification Detail event update API notifyEvent.", e);
		}
		
	}

	private void insertData() {
//		log.info("NOTIFICATION EVENT recieved records by consumer :CASE 444");
		Calendar calendar = Calendar.getInstance();
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int month = calendar.get(Calendar.MONTH) + 1;
		int year = calendar.get(Calendar.YEAR);
		String indexMonth, indexDay;
		if (day <= 9) {
			indexDay = "0" + day;
		} else {
			indexDay = "" + day;
		}
		if (month <= 9) {
			indexMonth = "0" + month;
		} else {
			indexMonth = "" + month;
		}
		String notificationIndex = Constants.NotificationConstants.DIVYA_NOTIFICATION_INDEX + indexDay + "_"
				+ indexMonth + "_" + year;
		resetTime();
		// Index the comments
		if (listNotificationEvent.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(notificationIndex, MappingTypes.MAPPING_REALTIME,
					listNotificationEvent);
			log.info("Thread " + threadIndex + ",NOTIFICATION EVENT Records inserted in index, "+notificationIndex+" & RECORD SIZE "
					+ listNotificationEvent.size());
			listNotificationEvent.clear();
			
			/*
			 * First: increment counters in app_user_profile_device_id index.
			 */

			boolean shouldChangeInProfileIndex=Boolean.parseBoolean(DBConfig.getInstance().getProperty("shouldChangeProfileIndex"));
			if(shouldChangeInProfileIndex) {
				elasticSearchIndexService.incrementCounter(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						rowIdToClickCount, "notification_clicks");
				elasticSearchIndexService.incrementCounter(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						rowIdToImpressionCount, "notification_impressions");

				replaceKeys(rowIdToClickCount);
				replaceKeys(rowIdToImpressionCount);
				/*
				 * Next: increment counter in user profile daywise.
				 */
				elasticSearchIndexService.incrementCounter(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
						rowIdToClickCount, "notification_clicks");
				elasticSearchIndexService.incrementCounter(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
						rowIdToImpressionCount, "notification_impressions");
				
				if(!webRowIdToImpressionCount.isEmpty()){
					elasticSearchIndexService.incrementCounter(Indexes.USER_PERSONALIZATION_STATS, MappingTypes.MAPPING_REALTIME,
							webRowIdToImpressionCount, "notification_impressions");
					
					replaceKeys(webRowIdToImpressionCount);
					
					elasticSearchIndexService.incrementCounter(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
							webRowIdToImpressionCount, "notification_impressions");
					
					webRowIdToImpressionCount.clear();
				}
				
				if(!webRowIdToClickCount.isEmpty()){
					elasticSearchIndexService.incrementCounter(Indexes.USER_PERSONALIZATION_STATS, MappingTypes.MAPPING_REALTIME,
							webRowIdToClickCount, "notification_clicks");
					
					replaceKeys(webRowIdToClickCount);
					
					elasticSearchIndexService.incrementCounter(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
							webRowIdToClickCount, "notification_clicks");
					
					webRowIdToClickCount.clear();
				}
			}
			
			rowIdToClickCount.clear();
			rowIdToImpressionCount.clear();
		}
	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
	
	/**
	 * Uitlity method to increment counter locally until the flush interval expires
	 * or a batch is filled.
	 * 
	 * @param rowId
	 *            the the session id of a user
	 * @param counterMap
	 *            the map to locally increment the counter in.
	 */
	private void incrementLocalCounter(String rowId, Map<String, Integer> counterMap) {
		if (counterMap.containsKey(rowId)) {
			counterMap.put(rowId, counterMap.get(rowId) + 1);
		} else {
			counterMap.put(rowId, 1);
		}
	}

	/**
	 * Utility method to replace session id with sessionId_date.
	 * 
	 * @param target
	 */
	private void replaceKeys(Map<String, Integer> target) {
		Set<String> rows = new HashSet<>(target.keySet());
		Integer value;
		
		for (String key : rows) {
			value = target.remove(key);
			target.put(key + "_" + DateUtil.getCurrentDate(), value);
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> convertInputRecordFormat(Map<String, Object> inputRecord){
		try{
			if(inputRecord.get("action") != null){
				Map<String, Object> convertedFormat = new HashMap<>();
				int host = 22;
				String action = (String) inputRecord.get("action");

				switch(action) {
				
					case "impressions":
						convertedFormat.put("event_type", "1");
						break;
						
					case "clicks": 
						convertedFormat.put("event_type", "2");
						break;
						
					default:
						convertedFormat.put("event_type", "0");
						break;
				}

				if(inputRecord.get("userData") != null && inputRecord.get("userData") instanceof Map){
					
					Map<String, Object> userData = (Map<String, Object>)inputRecord.get("userData");
					
					if(!userData.isEmpty() && userData.get("bhaskarUUID") != null){
						convertedFormat.put("p_sessionID", userData.get("bhaskarUUID").toString());
					}else{
						convertedFormat.put("p_sessionID", "");
					}
				}else{
					log.warn("userData info is "+inputRecord.get("userData"));
					log.warn("Notification user impression click payload : "+ inputRecord);
				}

				if(inputRecord.get("payload") != null){
					Map<String, Object> payload = (Map<String, Object>)inputRecord.get("payload");
					Map<String, Object> data = (Map<String, Object>)payload.get("data");

					if(data.get("option") != null){
						Map<String, Object> optionData = mapper.readValue((String)data.get("option"), new TypeReference<Map<String, Object>>() {
						});

						if(optionData.get("storyid") != null){
							convertedFormat.put("web_storyid", optionData.get("storyid").toString());
						}else{
							convertedFormat.put("web_storyid", "");
						}

						if(optionData.get("host") != null && StringUtils.isNotBlank(optionData.get("host").toString())){
							host = Integer.parseInt(optionData.get("host").toString());
							convertedFormat.put("host", host);
						}
					}
				}
				
				if(host == Host.BHASKAR_WEB_HOST){
					convertedFormat.put("index", Indexes.USER_PERSONALIZATION_STATS);
					return convertedFormat;
				}
			}
		}catch(Exception e){
			log.error("Notification payload Exception", e);
			log.error("Notification user impression click payload : "+ inputRecord);
			throw new DBAnalyticsException(e);
		}
		
		return inputRecord;
	}
}