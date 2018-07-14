package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class IdentificationConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(IdentificationConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();

	List<Map<String, Object>> identificationStoryDetails = new ArrayList<Map<String, Object>>();
	List<Map<String, Object>> identificationStoryUniqueDetails = new ArrayList<Map<String, Object>>();

	List<String> storyIdsforImpressionUpdate = new ArrayList<String>();
	List<String> storyIdsforImpressionUpdateUniqueDetails = new ArrayList<String>();
	List<String> storyIdsforClickUpdate = new ArrayList<String>();
	List<String> storyIdsforClickUpdateUniqueDetails = new ArrayList<String>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public IdentificationConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);

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
				//threadRecordCount = in.addAndGet(records.count());
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
		try{// Original Raw record.
		Map<String, Object> msg = decoder.decode(message);
		threadRecordCount = in.getAndIncrement();
		String session_id = (String) msg.get(Constants.SESSION_ID_FIELD);
		Integer storyId = (Integer) msg.get(Constants.STORY_ID_FIELD);
		Object catId = msg.get(Constants.CAT_ID_FIELD);
		Object parentCatId = msg.get(Constants.PARENT_CAT_ID_FIELD);
		String date = ((String) msg.get(Constants.DATE_TIME_FIELD)).split("T")[0];
		// Index date suffix for realtime and app data
		String indexDate = date.replaceAll("-", "_");

		listMessages.add(msg);

		String storyRowId = date + "_" + storyId + "_" + msg.get(Constants.HOST) + "_" + msg.get(Constants.REFERER_HOST)
				+ "_" + msg.get(Constants.WIDGET_NAME) + "_" + msg.get(Constants.POSITION) + "_"
				+ msg.get(Constants.TRACKER);

		// Prepare story data
		int eventType = (Integer) msg.get(Constants.EVENT_TYPE);

		Map<String, Object> storyDetail = new HashMap<String, Object>();
		storyDetail.put(Constants.STORY_ID_FIELD, msg.get(Constants.STORY_ID_FIELD));
		storyDetail.put(Constants.CHANNEL_SLNO, msg.get(Constants.CHANNEL_SLNO));
		storyDetail.put(Constants.HOST, msg.get(Constants.HOST));
		storyDetail.put(Constants.CAT_ID_FIELD, msg.get(Constants.CAT_ID_FIELD));
		storyDetail.put(Constants.URL, msg.get(Constants.URL));
		storyDetail.put(Constants.TRACKER, msg.get(Constants.TRACKER));
		storyDetail.put(Constants.REFERER, msg.get(Constants.REFERER));
		storyDetail.put(Constants.EVENT_TYPE, msg.get(Constants.EVENT_TYPE));
		storyDetail.put(Constants.DATE, msg.get(Constants.DATE));
		storyDetail.put(Constants.DATE_TIME_FIELD, msg.get(Constants.DATE_TIME_FIELD));
		storyDetail.put(Constants.STORY_MODIFIED_TIME, msg.get(Constants.STORY_MODIFIED_TIME));
		storyDetail.put(Constants.STORY_PUBLISH_TIME, msg.get(Constants.STORY_PUBLISH_TIME));
		storyDetail.put(Constants.PARENT_CAT_ID_FIELD, msg.get(Constants.PARENT_CAT_ID_FIELD));
		storyDetail.put(Constants.WIDGET_NAME, msg.get(Constants.WIDGET_NAME));
		storyDetail.put(Constants.POSITION, msg.get(Constants.POSITION));
		storyDetail.put(Constants.ROWID, storyRowId);
		storyDetail.put(Constants.REFERER_HOST, msg.get(Constants.REFERER_HOST));
		storyDetail.put(Constants.REFERER_PATH, msg.get(Constants.REFERER_PATH));
		identificationStoryDetails.add(storyDetail);
		Map<String,Object> tempStoryDetail= new HashMap<>(storyDetail);
		tempStoryDetail.replace(Constants.ROWID,storyId.toString());
		identificationStoryUniqueDetails.add(tempStoryDetail);

		if (eventType == Constants.EVENT_TYPE_IMPRESSION) {
			storyIdsforImpressionUpdate.add(storyRowId);
			storyIdsforImpressionUpdateUniqueDetails.add(storyId.toString());
		}

		if (eventType == Constants.EVENT_TYPE_CLICK) {
			storyIdsforClickUpdate.add(storyRowId);
			storyIdsforClickUpdateUniqueDetails.add(storyId.toString());
		}
		// Start: Index the raw data in realtime table
		if (threadRecordCount % batchSize == 0) {
			insertData(indexDate);
		}

		if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
			log.info("Thread " + threadIndex + ", Flushing the Data in realtime tables..");
			insertData(indexDate);
		}
		// End: Index the raw data in realtime table
		}catch(Exception e){
			log.error("An error occurred in IdentificationConsumerRunnableTask.",e);
		}
	}

	private void insertData(String indexDate) {
		String tableName = "identification_" + indexDate;
		resetTime();
		// Index the raw data in realtime mapping of daily table
		if (listMessages.size() > 0) {
			elasticSearchIndexService.index(tableName, listMessages);
			log.info("Thread " + threadIndex + ", Records inserted in realtime daily index, size: "
					+ listMessages.size());

			listMessages.clear();
		}
		// Index the story details in identification_story_detail index
		if (identificationStoryDetails.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.IDENTIFICATION_STORY_DETAIL, MappingTypes.MAPPING_REALTIME,
					identificationStoryDetails);
			log.info("Thread " + threadIndex + ", Records inserted in identification_story_detail index, size: "
					+ identificationStoryDetails.size());
			identificationStoryDetails.clear();
			elasticSearchIndexService.indexOrUpdate(Indexes.IDENTIFICATION_UNIQUE_STORY_DETAIL, MappingTypes.MAPPING_REALTIME,
					identificationStoryUniqueDetails);
			log.info("Thread " + threadIndex + ", Records inserted in identification_unique_story_detail index, size: "
					+ identificationStoryUniqueDetails.size());
			identificationStoryUniqueDetails.clear();
		}

		if (storyIdsforImpressionUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.IDENTIFICATION_STORY_DETAIL,
					MappingTypes.MAPPING_REALTIME, storyIdsforImpressionUpdate, Constants.IMPRESSIONS, 1);
			log.info("Thread " + threadIndex + ", Impressions updated in " + Indexes.IDENTIFICATION_STORY_DETAIL
					+ " index, size: " + storyIdsforImpressionUpdate.size());
			storyIdsforImpressionUpdate.clear();
			elasticSearchIndexService.incrementCounter(Indexes.IDENTIFICATION_UNIQUE_STORY_DETAIL,
					MappingTypes.MAPPING_REALTIME, storyIdsforImpressionUpdateUniqueDetails, Constants.IMPRESSIONS, 1);
			log.info("Thread " + threadIndex + ", Impressions updated in " + Indexes.IDENTIFICATION_UNIQUE_STORY_DETAIL
					+ " index, size: " + storyIdsforImpressionUpdateUniqueDetails.size());
			storyIdsforImpressionUpdateUniqueDetails.clear();
		}

		if (storyIdsforClickUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.IDENTIFICATION_STORY_DETAIL,
					MappingTypes.MAPPING_REALTIME, storyIdsforClickUpdate, Constants.VIEWS, 1);
			log.info("Thread " + threadIndex + ", Views updated in " + Indexes.IDENTIFICATION_STORY_DETAIL
					+ " index, size: " + storyIdsforClickUpdate.size());
			elasticSearchIndexService.incrementCounter(Indexes.IDENTIFICATION_UNIQUE_STORY_DETAIL,
					MappingTypes.MAPPING_REALTIME, storyIdsforClickUpdateUniqueDetails, Constants.VIEWS, 1);
			log.info("Thread " + threadIndex + ", Views updated in " + Indexes.IDENTIFICATION_UNIQUE_STORY_DETAIL
					+ " index, size: " + storyIdsforClickUpdateUniqueDetails.size());
			storyIdsforClickUpdate.clear();
			storyIdsforClickUpdateUniqueDetails.clear();
		}

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
}