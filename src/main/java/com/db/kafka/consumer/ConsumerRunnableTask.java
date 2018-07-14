package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.IndexUtils;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class ConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(ConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();

	private Map<String, List<Map<String, Object>>> indexListMessageMap = new HashMap<>();

	// private List<String> videoIdsforVideoViewsUpdate = new
	// ArrayList<String>();
	private Map<String, Integer> videoIdToCounterMap = new HashMap<>();
	private Map<String, Integer> videoIdToImpressionCounterMap = new HashMap<>();
	private Map<String, Integer> videoIdToClicksCounterMap = new HashMap<>();
//	private List<Map<String, Object>> videoDetailData = new ArrayList<Map<String, Object>>();
	private long startTime = System.currentTimeMillis();

	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	private String indexName = null;

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public ConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
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
				// threadRecordCount = in.addAndGet(records.count());
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
			Map<String, Object> msg = decoder.decode(message);
			threadRecordCount = in.getAndIncrement();
			indexName = (String) msg.remove(Constants.INDEX_NAME);

			String datetime = ((String) msg.get(Constants.DATE_TIME_FIELD));
			String date = datetime.split("T")[0];

			if (StringUtils.isBlank(indexName)) {
				log.error("Received Record with empty indexName: " + msg);
				return;
			}

			if (indexName.startsWith("video_identification")) {
				String videoDetailRowId = msg.get(Constants.VIDEO_ID) + "_" + date + "_"
						+ msg.get(Constants.WIDGET_NAME) + "_" + msg.get(Constants.WIDGET_REC_POS);
				Map<String, Object> videoDetailMap = new HashMap<>(msg);
				String event = (String) videoDetailMap.remove(Constants.EVENT_TYPE);
				videoDetailMap.put(Constants.ROWID, videoDetailRowId);
				videoDetailMap.remove(Constants.STATE);
				videoDetailMap.remove(Constants.CITY);
				videoDetailMap.remove(Constants.COUNTRY);
				videoDetailMap.remove(Constants.SESSION_ID_FIELD);
				videoDetailMap.remove(Constants.REFERER_DOMAIN);
				videoDetailMap.remove(Constants.DEVICE_TYPE);
				videoDetailMap.remove(Constants.SITE_TRACKER);
				videoDetailMap.remove(Constants.DOMAINTYPE);
				videoDetailMap.remove(Constants.BROWSER);
				videoDetailMap.remove(Constants.USER_ID);

				/*
				 * To update clicks/impressions in video_detail
				 */
				if (event.equalsIgnoreCase("impression")) {
					if (videoIdToImpressionCounterMap.containsKey(videoDetailRowId)) {
						videoIdToImpressionCounterMap.put(videoDetailRowId,
								videoIdToImpressionCounterMap.get(videoDetailRowId) + 1);
					} else {
						videoIdToImpressionCounterMap.put(videoDetailRowId, 1);
					}
				} else if (event.equalsIgnoreCase("click")) {

					if (videoIdToClicksCounterMap.containsKey(videoDetailRowId)) {
						videoIdToClicksCounterMap.put(videoDetailRowId,
								videoIdToClicksCounterMap.get(videoDetailRowId) + 1);
					} else {
						videoIdToClicksCounterMap.put(videoDetailRowId, 1);
					}
				}

				if (indexListMessageMap.containsKey(Indexes.VIDEO_IDENTIFICATION_DETAIL)) {
					indexListMessageMap.get(Indexes.VIDEO_IDENTIFICATION_DETAIL).add(videoDetailMap);
				} else {
					List<Map<String, Object>> listMessages = new ArrayList<>();
					listMessages.add(videoDetailMap);
					indexListMessageMap.put(Indexes.VIDEO_IDENTIFICATION_DETAIL, listMessages);
				}
			} else if (indexName.startsWith("video_tracking")) {
				String videoId = (String) msg.get(Constants.VIDEO_ID);
				Integer quartile = -1;

				try {
					if (msg.get(Constants.QUARTILE) instanceof String) {
						quartile = Integer.parseInt((String) msg.get(Constants.QUARTILE));
					} else {
						quartile = (Integer) msg.get(Constants.QUARTILE);
					}
				} catch (Exception e) {
					log.error("Error in quartile parsing in video tracking index ", e);
				}
				// Logic to capture video views for zero quartile
				if (StringUtils.isNotBlank(videoId) && quartile == 0) {
					if (videoIdToCounterMap.containsKey(videoId)) {
						videoIdToCounterMap.put(videoId, videoIdToCounterMap.get(videoId) + 1);
					} else {
						videoIdToCounterMap.put(videoId, 1);
					}
					// videoIdsforVideoViewsUpdate.add(videoId);
				}
			}

			if (indexListMessageMap.containsKey(indexName)) {
				indexListMessageMap.get(indexName).add(msg);
			} else {
				List<Map<String, Object>> listMessages = new ArrayList<>();
				listMessages.add(msg);
				indexListMessageMap.put(indexName, listMessages);
			}

			// Index the data if one batch prepared.
			if (threadRecordCount % batchSize == 0) {
				insertData();

			}

			if (System.currentTimeMillis() - startTime >= flushInterval && indexListMessageMap.size() > 0) {
				log.info("Flushing Data...");
				insertData();
			}

		} catch (Exception e) {
			log.error("Error in KafkaConsumerService. ", e);
		}
	}

//	private void processMessage(byte[] message) {
//		try {
//			Map<String, Object> msg = decoder.decode(message);
//			threadRecordCount = in.getAndIncrement();
//			indexName = (String) msg.remove(Constants.INDEX_NAME);
//
//			listMessages.add(msg);
//
//			String datetime = ((String) msg.get(Constants.DATE_TIME_FIELD));
//			String date = datetime.split("T")[0];
//
//			if (indexName.startsWith("video_identification")) {
//				String videoDetailRowId = msg.get(Constants.VIDEO_ID) + "_" + date + "_"
//						+ msg.get(Constants.WIDGET_NAME) + "_" + msg.get(Constants.WIDGET_REC_POS);
//				Map<String, Object> videoDetailMap = new HashMap<>(msg);
//				String event = (String) videoDetailMap.remove(Constants.EVENT_TYPE);
//				videoDetailMap.put(Constants.ROWID, videoDetailRowId);
//				videoDetailMap.remove(Constants.STATE);
//				videoDetailMap.remove(Constants.CITY);
//				videoDetailMap.remove(Constants.COUNTRY);
//				videoDetailMap.remove(Constants.SESSION_ID_FIELD);
//				videoDetailMap.remove(Constants.REFERER_DOMAIN);
//				videoDetailMap.remove(Constants.DEVICE_TYPE);
//				videoDetailMap.remove(Constants.SITE_TRACKER);
//				videoDetailMap.remove(Constants.DOMAINTYPE);
//				videoDetailMap.remove(Constants.BROWSER);
//				videoDetailMap.remove(Constants.USER_ID);
//
//				// For impression update in video_detail
//				if (event.equalsIgnoreCase("impression")) {
//					if (videoIdToImpressionCounterMap.containsKey(videoDetailRowId)) {
//						videoIdToImpressionCounterMap.put(videoDetailRowId,
//								videoIdToImpressionCounterMap.get(videoDetailRowId) + 1);
//					} else {
//						videoIdToImpressionCounterMap.put(videoDetailRowId, 1);
//					}
//				}
//
//				// For clicks update in video_detail
//				else if (event.equalsIgnoreCase("click")) {
//					if (videoIdToClicksCounterMap.containsKey(videoDetailRowId)) {
//						videoIdToClicksCounterMap.put(videoDetailRowId,
//								videoIdToClicksCounterMap.get(videoDetailRowId) + 1);
//					} else {
//						videoIdToClicksCounterMap.put(videoDetailRowId, 1);
//					}
//				}
//				videoDetailData.add(videoDetailMap);
//			}
//			// To update video views
//			if (indexName.startsWith("video_tracking")) {
//				String videoId = (String) msg.get(Constants.VIDEO_ID);
//				Integer quartile = -1;
//
//				try {
//					if (msg.get(Constants.QUARTILE) instanceof String) {
//						quartile = Integer.parseInt((String) msg.get(Constants.QUARTILE));
//					} else {
//						quartile = (Integer) msg.get(Constants.QUARTILE);
//					}
//				} catch (Exception e) {
//					log.error("Error in quartile parsing in video tracking index ", e);
//				}
//				// Logic to capture video views for zero quartile
//				if (StringUtils.isNotBlank(videoId) && quartile == 0) {
//					if (videoIdToCounterMap.containsKey(videoId)) {
//						videoIdToCounterMap.put(videoId, videoIdToCounterMap.get(videoId) + 1);
//					} else {
//						videoIdToCounterMap.put(videoId, 1);
//					}
//					// videoIdsforVideoViewsUpdate.add(videoId);
//				}
//			}
//
//			// Index the data if one batch prepared.
//			if (threadRecordCount % batchSize == 0) {
//				insertData();
//
//			}
//
//			if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
//				log.info("Flushing Data...");
//				insertData();
//			}
//		} catch (Exception e) {
//			log.error("Error in KafkaConsumerService. ", e);
//		}
//	}

	/**
	 * This method is designed to index data for any index.
	 */
	private void insertData() {
		resetTime();

		for (String index : indexListMessageMap.keySet()) {
			/*
			 * Run an index/update for video identification or run an index command for
			 * other indexes.
			 */
			if (Indexes.VIDEO_IDENTIFICATION_DETAIL.equalsIgnoreCase(index)) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
						MappingTypes.MAPPING_REALTIME, indexListMessageMap.get(index));
				log.info("IndexName: " + Indexes.VIDEO_IDENTIFICATION_DETAIL + ",Thread " + threadIndex
						+ ": Number of record stored " + indexListMessageMap.get(index).size());
			} else {
				elasticSearchIndexService.index(index, indexListMessageMap.get(index));
				log.info("IndexName: " + index + ",Thread " + threadIndex
						+ ": Number of record stored " + indexListMessageMap.get(index).size());
			}
			indexListMessageMap.get(index).clear();
		}

		if (videoIdToClicksCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
					MappingTypes.MAPPING_REALTIME, videoIdToClicksCounterMap, Constants.CLICKS);
			log.info("Thread " + threadIndex + ", clicks updated in video_identification " + " index, size: "
					+ videoIdToClicksCounterMap.size());
			videoIdToClicksCounterMap.clear();
		}

		if (videoIdToImpressionCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
					MappingTypes.MAPPING_REALTIME, videoIdToImpressionCounterMap, Constants.IMPRESSIONS);
			log.info("Thread " + threadIndex + ", impressions updated in video_identification " + " index, size: "
					+ videoIdToImpressionCounterMap.size());
			videoIdToImpressionCounterMap.clear();
		}

		if (videoIdToCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.VIDEO_DETAILS, MappingTypes.MAPPING_REALTIME,
					videoIdToCounterMap, Constants.VIDEO_VIEWS);
			log.info("Thread " + threadIndex + ", Video Views updated in video_details " + " index, size: "
					+ videoIdToCounterMap.size());
			videoIdToCounterMap.clear();
		}
	}

//	private void insertData() {
//		resetTime();
//		if (listMessages.size() > 0) {
//			elasticSearchIndexService.index(indexName, listMessages);
//			log.info("IndexName: " + indexName + ",Thread " + threadIndex + ": Number of record stored "
//					+ listMessages.size());
//			listMessages.clear();
//		}
//		// if (videoIdsforVideoViewsUpdate.size() > 0) {
//		// elasticSearchIndexService.incrementCounter(Indexes.VIDEO_DETAILS,
//		// MappingTypes.MAPPING_REALTIME,
//		// videoIdsforVideoViewsUpdate, Constants.VIDEO_VIEWS, 1);
//		// log.info("Thread " + threadIndex + ", Video Views updated in
//		// video_details " + " index, size: "
//		// + videoIdsforVideoViewsUpdate.size());
//		// videoIdsforVideoViewsUpdate.clear();
//		// }
//		if (videoDetailData.size() > 0) {
//			elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
//					MappingTypes.MAPPING_REALTIME, videoDetailData);
//			log.info("IndexName: " + Indexes.VIDEO_IDENTIFICATION_DETAIL + ",Thread " + threadIndex
//					+ ": Number of record stored " + videoDetailData.size());
//			videoDetailData.clear();
//		}
//
//		if (videoIdToClicksCounterMap.size() > 0) {
//			elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
//					MappingTypes.MAPPING_REALTIME, videoIdToClicksCounterMap, Constants.CLICKS);
//			log.info("Thread " + threadIndex + ", clicks updated in video_identification " + " index, size: "
//					+ videoIdToClicksCounterMap.size());
//			videoIdToClicksCounterMap.clear();
//		}
//
//		if (videoIdToImpressionCounterMap.size() > 0) {
//			elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.VIDEO_IDENTIFICATION_DETAIL),
//					MappingTypes.MAPPING_REALTIME, videoIdToImpressionCounterMap, Constants.IMPRESSIONS);
//			log.info("Thread " + threadIndex + ", impressions updated in video_identification " + " index, size: "
//					+ videoIdToImpressionCounterMap.size());
//			videoIdToImpressionCounterMap.clear();
//		}
//
//		if (videoIdToCounterMap.size() > 0) {
//			elasticSearchIndexService.incrementCounter(Indexes.VIDEO_DETAILS, MappingTypes.MAPPING_REALTIME,
//					videoIdToCounterMap, Constants.VIDEO_VIEWS);
//			log.info("Thread " + threadIndex + ", Video Views updated in video_details " + " index, size: "
//					+ videoIdToCounterMap.size());
//			videoIdToCounterMap.clear();
//		}
//	}

	/**
	 * Reset start time for the next flush interval.
	 */
	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
}
