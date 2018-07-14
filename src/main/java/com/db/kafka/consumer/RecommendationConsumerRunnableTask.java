package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
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

public class RecommendationConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(RecommendationConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	private AtomicInteger in = new AtomicInteger(0);

	private int threadIndex = 0;

	private int threadRecordCount = 1;

	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();

	List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();

	List<Map<String, Object>> recommendationStoryDetails = new ArrayList<Map<String, Object>>();

	List<String> storyIdsforViewsUpdate = new ArrayList<String>();

	private long startTime = System.currentTimeMillis();

	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public RecommendationConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
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
			//	threadRecordCount = in.addAndGet(records.count());
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
		try {
			byte[] kafkaMessageForStoryDetails = message;
			// Original Raw record.
			Map<String, Object> msg = decoder.decode(message);
			threadRecordCount = in.getAndIncrement();
			
			listMessages.add(msg);

			// System.arraycopy(message, 0, message2, 0,
			// message.length);
			Map<String, Object> kafkaMapForStoryDetails = decoder.decode(kafkaMessageForStoryDetails);

			String date = ((String) msg.get(Constants.DATE_TIME_FIELD)).split("T")[0];
			String indexDate = date.replaceAll("-", "_");

			kafkaMapForStoryDetails.remove(Constants.SESSION_ID_FIELD);
			kafkaMapForStoryDetails.remove(Constants.IP1);
			kafkaMapForStoryDetails.remove(Constants.IP2);
			kafkaMapForStoryDetails.remove(Constants.DEVICE_TYPE);
			kafkaMapForStoryDetails.remove(Constants.CITY);
			kafkaMapForStoryDetails.remove(Constants.STATE);
			kafkaMapForStoryDetails.remove(Constants.COUNTRY);
			kafkaMapForStoryDetails.remove(Constants.CONTINENT);

			String storyId = (String) msg.get(Constants.STORY_ID);

			if (StringUtils.isNotBlank(storyId)) {
				kafkaMapForStoryDetails.put(Constants.ROWID, storyId);
				recommendationStoryDetails.add(kafkaMapForStoryDetails);
				storyIdsforViewsUpdate.add(storyId);
			}

			// Recommendation end

			// Start: Index the raw data in realtime table
			if (threadRecordCount % batchSize == 0) {
				insertData(indexDate);
			}

			if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
				log.info("Thread " + threadIndex + ", Flushing the Data in realtime tables..");
				insertData(indexDate);
			}
		} catch (Exception e) {
			log.error("Error in RecommendationConsumerRunnableTask. ", e);
		}
		// End: Index the raw data in realtime table
	}

	private void insertData(String indexDate) {
		String tableName = Constants.REC_DAILY_LOG_INDEX_PREFIX + indexDate;
		resetTime();
		if (listMessages.size() > 0) {
			// Index the raw data in realtime mapping of daily table
			elasticSearchIndexService.index(tableName, listMessages);
			log.info("Thread " + threadIndex + ", Records inserted in " + tableName + " daily index, size: "
					+ listMessages.size());

			listMessages.clear();

		}

		// Index the story details in story_detail index
		if (recommendationStoryDetails.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.RECOMMENDATION_STORY_DETAIL, MappingTypes.MAPPING_REALTIME,
					recommendationStoryDetails);
			log.info("Thread " + threadIndex + ", Story Details inserted in " + Indexes.RECOMMENDATION_STORY_DETAIL
					+ " index, size: " + recommendationStoryDetails.size());
			recommendationStoryDetails.clear();
		}

		if (storyIdsforViewsUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.RECOMMENDATION_STORY_DETAIL,
					MappingTypes.MAPPING_REALTIME, storyIdsforViewsUpdate, Constants.VIEWS, 1);
			log.info("Thread " + threadIndex + ", Views updated in " + Indexes.RECOMMENDATION_STORY_DETAIL
					+ " index, size: " + storyIdsforViewsUpdate.size());
			storyIdsforViewsUpdate.clear();
		}

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
}