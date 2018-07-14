package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.constants.WisdomConstants;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class WisdomRealTimeConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(WisdomRealTimeConsumerRunnableTask.class);

	private static List<String> ucbTrackers = Arrays.asList("news-m-ucb", "news-ucb", "news-m-ucb_1", "news-ucb_1");

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	// private NLPParser nlpParser=new NLPParser();//NLPParser.getInstance();

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();

	private List<Map<String, Object>> realtimeData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storyDetailData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storyDetailHourlyData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storyUniqueDetailData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storySequenceData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> upvsData = new ArrayList<Map<String, Object>>();
	private List<String> storyDetailRowIds = new ArrayList<>();
	private Map<String, Integer> storyDetailRowIdMapForPvCounterUpdate = new HashMap<>();
	
	private Map<String, Integer> storyUniqueDetailRowIdMapForPvCounterUpdate = new HashMap<>();
	private Map<String, Integer> storyDetailHourlyRowIdMapForPvCounterUpdate = new HashMap<>();
	private Map<String, Map<String, Integer>> storySequenceFieldToIdMap = new HashMap<>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public WisdomRealTimeConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);
		storySequenceFieldToIdMap.put(WisdomConstants.WPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(WisdomConstants.MPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(WisdomConstants.APVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(WisdomConstants.IPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(WisdomConstants.UCBPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(WisdomConstants.OTHERPVS, new HashMap<String, Integer>());
	}

	public void shutdown() {
		consumer.wakeup();
		log.info(" Wisdom Realtime Consumer wake up requested.");
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
		log.info("Wisdom Realtime consumer stopped");
	}

	private void processMessage(byte[] message) {
		try {
			threadRecordCount = in.getAndIncrement();
			//realtime index data
			Map<String, Object> realtimeMap = decoder.decode(message);

			String datetime = ((String) realtimeMap.get(Constants.DATE_TIME_FIELD));
			String date = datetime.split("T")[0];

			// Index date suffix for realtime and app data
			String indexDate = date.replaceAll("-", "_");

			realtimeData.add(realtimeMap);

			//realtime_upvs index data
			String upvsRowId = realtimeMap.get(WisdomConstants.USER_COOKIE_ID) + "_" + realtimeMap.get(WisdomConstants.USER_SESSION_ID)+"_"+realtimeMap.get(WisdomConstants.STORY_ID)+"_"+date;
			Map<String, Object> upvsMap = new HashMap<String, Object>(realtimeMap);
			upvsMap.put(WisdomConstants.ROWID,upvsRowId);
			upvsData.add(upvsMap);
			
			//story_detail index data
			String storyDetailRowId = realtimeMap.get(WisdomConstants.STORY_ID) + "_"+realtimeMap.get(WisdomConstants.DOMAIN_ID) + "_" + realtimeMap.get(WisdomConstants.PLATFORM)+"_" + realtimeMap.get(WisdomConstants.TRACKER)+"_" + realtimeMap.get(WisdomConstants.HTTP_REFERER)+ "_" + realtimeMap.get(WisdomConstants.MODIFIED_DATE) + "_" + date;
			Map<String, Object> storyDetailMap = new HashMap<>(realtimeMap);
			storyDetailMap.remove(WisdomConstants.USER_SESSION_ID);
			storyDetailMap.remove(WisdomConstants.USER_COOKIE_ID);
			storyDetailMap.remove(WisdomConstants.SLIDE_NUMBER);
			storyDetailMap.remove(WisdomConstants.IP_ADDRESS);
			storyDetailMap.remove(WisdomConstants.BROWSER_TYPE);
			storyDetailMap.remove(WisdomConstants.BROWSER_VERSION);
			storyDetailMap.remove(WisdomConstants.SCREEN_SIZE);	
			storyDetailMap.remove(WisdomConstants.COUNTRY);	
			storyDetailMap.remove(WisdomConstants.STATE);	
			storyDetailMap.remove(WisdomConstants.CITY);	
			storyDetailMap.put(WisdomConstants.ROWID, storyDetailRowId);
			// For PV update in story_detail
			if (storyDetailRowIdMapForPvCounterUpdate.containsKey(storyDetailRowId)) {
				storyDetailRowIdMapForPvCounterUpdate.put(storyDetailRowId,
						storyDetailRowIdMapForPvCounterUpdate.get(storyDetailRowId) + 1);
			} else {
				storyDetailRowIdMapForPvCounterUpdate.put(storyDetailRowId, 1);
			}
			storyDetailData.add(storyDetailMap);
			storyDetailRowIds.add(storyDetailRowId);

			//story_detail_hourly index data
			String storyDetailHourlyRowId = storyDetailRowId + "_" + DateUtil.getHour(datetime);
			// clone story detail map object
			Map<String, Object> storyDetailHourlyMap = new HashMap<>(storyDetailMap);
			storyDetailHourlyMap.put(WisdomConstants.ROWID, storyDetailHourlyRowId);			
			// Map of storyDetailHourlyRowId to PV counter update
			if (storyDetailHourlyRowIdMapForPvCounterUpdate.containsKey(storyDetailHourlyRowId)) {
				storyDetailHourlyRowIdMapForPvCounterUpdate.put(storyDetailHourlyRowId,
						storyDetailHourlyRowIdMapForPvCounterUpdate.get(storyDetailHourlyRowId) + 1);
			} else {
				storyDetailHourlyRowIdMapForPvCounterUpdate.put(storyDetailHourlyRowId, 1);
			}
			storyDetailHourlyData.add(storyDetailHourlyMap);

			//story_unique_detail index data
			String storyUniqueDetailRowId = realtimeMap.get(WisdomConstants.DOMAIN_ID) + "_" + (String)realtimeMap.get(WisdomConstants.STORY_ID);
			Map<String, Object> storyUniqueDetailMap = new HashMap<>(storyDetailMap);
			storyUniqueDetailMap.put(WisdomConstants.ROWID, storyUniqueDetailRowId);
			// Map of storyUniqueDetailRowId to PV counter update
			if (storyUniqueDetailRowIdMapForPvCounterUpdate.containsKey(storyUniqueDetailRowId)) {
				storyUniqueDetailRowIdMapForPvCounterUpdate.put(storyUniqueDetailRowId,
						storyUniqueDetailRowIdMapForPvCounterUpdate.get(storyUniqueDetailRowId) + 1);
			} else {
				storyUniqueDetailRowIdMapForPvCounterUpdate.put(storyUniqueDetailRowId, 1);
			}
			storyUniqueDetailData.add(storyUniqueDetailMap);

			//story_sequence data
			String storySequenceRowId = realtimeMap.get(WisdomConstants.STORY_ID)+"_"+realtimeMap.get(WisdomConstants.SLIDE_NUMBER);
			String platform = (String) realtimeMap.get(WisdomConstants.PLATFORM);
			String tracker = (String) realtimeMap.get(WisdomConstants.TRACKER);
			if (platform.equals(WisdomConstants.WEB)) {
				Map<String, Integer> wpvsData = storySequenceFieldToIdMap.get(WisdomConstants.WPVS);
				if (wpvsData.containsKey(storySequenceRowId)) {
					wpvsData.put(storySequenceRowId, wpvsData.get(storySequenceRowId) + 1);
				} else {
					wpvsData.put(storySequenceRowId, 1);
				}

			} else if (ucbTrackers.contains(tracker)) {
				Map<String, Integer> ucbpvsData = storySequenceFieldToIdMap.get(WisdomConstants.UCBPVS);
				if (ucbpvsData.containsKey(storySequenceRowId)) {
					ucbpvsData.put(storySequenceRowId, ucbpvsData.get(storySequenceRowId) + 1);
				} else {
					ucbpvsData.put(storySequenceRowId, 1);
				}

			} else if (platform.equals(WisdomConstants.MOBILE)) {
				Map<String, Integer> mpvsData = storySequenceFieldToIdMap.get(WisdomConstants.MPVS);
				if (mpvsData.containsKey(storySequenceRowId)) {
					mpvsData.put(storySequenceRowId, mpvsData.get(storySequenceRowId) + 1);
				} else {
					mpvsData.put(storySequenceRowId, 1);
				}

			} else if (platform.equals(WisdomConstants.ANDROID)) {
				Map<String, Integer> apvsData = storySequenceFieldToIdMap.get(WisdomConstants.APVS);
				if (apvsData.containsKey(storySequenceRowId)) {
					apvsData.put(storySequenceRowId, apvsData.get(storySequenceRowId) + 1);
				} else {
					apvsData.put(storySequenceRowId, 1);
				}

			} else if (platform.equals(WisdomConstants.IPHONE)) {
				Map<String, Integer> ipvsData = storySequenceFieldToIdMap.get(WisdomConstants.IPVS);
				if (ipvsData.containsKey(storySequenceRowId)) {
					ipvsData.put(storySequenceRowId, ipvsData.get(storySequenceRowId) + 1);
				} else {
					ipvsData.put(storySequenceRowId, 1);
				}
			} else {
				Map<String, Integer> otherpvsData = storySequenceFieldToIdMap.get(WisdomConstants.OTHERPVS);
				if (otherpvsData.containsKey(storySequenceRowId)) {
					otherpvsData.put(storySequenceRowId, otherpvsData.get(storySequenceRowId) + 1);
				} else {
					otherpvsData.put(storySequenceRowId, 1);
				}
			}

			Map<String, Object> storySequenceMap = new HashMap<String, Object>();
			storySequenceMap.put(WisdomConstants.ROWID, storySequenceRowId);
			storySequenceMap.put(WisdomConstants.SLIDE_NUMBER, realtimeMap.get(WisdomConstants.SLIDE_NUMBER));
			storySequenceMap.put(WisdomConstants.STORY_ID, realtimeMap.get(WisdomConstants.STORY_ID));
			storySequenceMap.put(WisdomConstants.DATETIME, realtimeMap.get(WisdomConstants.DATETIME));
			storySequenceData.add(storySequenceMap);

			if (threadRecordCount % batchSize == 0) {
				log.info("Batch size: " + batchSize + "; ThreadRecordCount: " + threadRecordCount);
				insertData(indexDate);
			}

			if (System.currentTimeMillis() - startTime >= flushInterval && realtimeData.size() > 0) {
				log.info("Thread " + threadIndex + ", Flushing the Data in wisdom realtime tables..");
				insertData(indexDate);
			}

		} catch (Exception e) {
			log.error("Error in WisdomRealTimeConsumerRunnableTask",e);
		}
	}

	private void insertData(String indexDate) {	

		String tableName = "wisdom_realtime_" + indexDate;
		String upvsTableName = "wisdom_realtime_upvs_" + indexDate;
		resetTime();

		// Index the raw data in realtime mapping of daily table
		elasticSearchIndexService.index(tableName, realtimeData);
		log.info("Thread " + threadIndex + ", Records inserted in wisdom_realtime daily index, size: " + realtimeData.size());
		realtimeData.clear();

		//index upvs data
		List<IndexResponse> upvsIndexResponses = null;
		upvsIndexResponses = elasticSearchIndexService.indexWithResponse(upvsTableName,
				MappingTypes.MAPPING_REALTIME, upvsData);
		log.info("Thread " + threadIndex + ", Records inserted in realtime upvs mapping index, size: "
				+ upvsData.size());
		log.info(upvsTableName + "-" + MappingTypes.MAPPING_REALTIME + " record count "
				+ upvsData.size());
		upvsData.clear();

		// Process response for UPV
		
		Map<String, Integer> storyDetailRowIDMapForUPVCounterUpdate = new HashMap<>();
		int indexOfResponse = 0;
		for (IndexResponse indexResponse : upvsIndexResponses) {
			if (DocWriteResponse.Result.CREATED==indexResponse.getResult()) {
				String storyDetailRowId = storyDetailRowIds.get(indexOfResponse);
				if (storyDetailRowIDMapForUPVCounterUpdate.containsKey(storyDetailRowId)) {
					storyDetailRowIDMapForUPVCounterUpdate.put(storyDetailRowId,
							storyDetailRowIDMapForUPVCounterUpdate.get(storyDetailRowId) + 1);
				} else {
					storyDetailRowIDMapForUPVCounterUpdate.put(storyDetailRowId, 1);
				}
				// TODO: Story unique detail UPV
			}
			indexOfResponse++;
		}	
		
		storyDetailRowIds.clear();

		// Index the story details in story_detail index
		if (storyDetailData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL), MappingTypes.MAPPING_REALTIME,
					storyDetailData);
			log.info("Thread " + threadIndex + ", Records inserted in wisdom_story_detail index, size: "
					+ storyDetailData.size());
			storyDetailData.clear();
		}

		// Update PV in story_detail index
		if (storyDetailRowIdMapForPvCounterUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getMonthlyIndex(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL), MappingTypes.MAPPING_REALTIME,
					storyDetailRowIdMapForPvCounterUpdate, WisdomConstants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in wisdom_story_detail index, size: "
					+ storyDetailRowIdMapForPvCounterUpdate.size());
			storyDetailRowIdMapForPvCounterUpdate.clear();
		}

		// Update UPVs in story_detail index
		if (storyDetailRowIDMapForUPVCounterUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getMonthlyIndex(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL), MappingTypes.MAPPING_REALTIME,
					storyDetailRowIDMapForUPVCounterUpdate, WisdomConstants.UPVS);
			log.info("Thread " + threadIndex + ", UPVs updated in wisdom_story_detail index, map: "
					+ storyDetailRowIDMapForUPVCounterUpdate);
			storyDetailRowIDMapForUPVCounterUpdate.clear();
		}

		// Index the unique story details in story_unique_detail index
		if (storyUniqueDetailData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.WisdomIndexes.WISDOM_STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME,
					storyUniqueDetailData);
			log.info("Thread " + threadIndex + ", Records inserted in wisdom_story_unique_detail index, size: "
					+ storyUniqueDetailData.size());
			storyUniqueDetailData.clear();
		}

		// Update PV in story_unique_detail index
		if (storyUniqueDetailRowIdMapForPvCounterUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.WisdomIndexes.WISDOM_STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME,
					storyUniqueDetailRowIdMapForPvCounterUpdate, WisdomConstants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in wisdom_story_unique_detail index, size: "
					+ storyUniqueDetailRowIdMapForPvCounterUpdate.size());
			storyUniqueDetailRowIdMapForPvCounterUpdate.clear();
		}

		// index story_detail_hourly data
		if (storyDetailHourlyData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL_HOURLY), MappingTypes.MAPPING_REALTIME,
					storyDetailHourlyData);
			log.info("Thread " + threadIndex + ", Records inserted in " + Indexes.WisdomIndexes.WISDOM_STORY_DETAIL_HOURLY + " index, size: "
					+ storyDetailHourlyData.size());
			storyDetailHourlyData.clear();
		}

		//update pvs in story_detail_hourly index
		if (storyDetailHourlyRowIdMapForPvCounterUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getMonthlyIndex(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL_HOURLY), MappingTypes.MAPPING_REALTIME,
					storyDetailHourlyRowIdMapForPvCounterUpdate, WisdomConstants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in wisdom_story_detail_hourly index, size: "
					+ storyDetailHourlyRowIdMapForPvCounterUpdate.size());
			storyDetailHourlyRowIdMapForPvCounterUpdate.clear();
		}		

		// Index story_sequence data
		if (storySequenceData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.WisdomIndexes.WISDOM_STORY_SEQUENCE, MappingTypes.MAPPING_REALTIME,
					storySequenceData);
			log.info("Thread " + threadIndex + ", Records inserted in wisdom_story_sequence index, size: "
					+ storySequenceData.size());
			storySequenceData.clear();
		}

		//Update pvs in story_sequence index
		if (storySequenceFieldToIdMap.size() > 0) {
			for (String field : storySequenceFieldToIdMap.keySet()) {
				if (storySequenceFieldToIdMap.get(field).size() > 0) {
					elasticSearchIndexService.incrementCounter(Indexes.WisdomIndexes.WISDOM_STORY_SEQUENCE,
							MappingTypes.MAPPING_REALTIME, storySequenceFieldToIdMap.get(field), field);
					log.info("Thread " + threadIndex + ", " + field + " PVs updated in " + Indexes.WisdomIndexes.WISDOM_STORY_SEQUENCE
							+ " index, size: " + storySequenceFieldToIdMap.get(field).size());
					storySequenceFieldToIdMap.get(field).clear();
				}
			}
		}			
	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
	
}
