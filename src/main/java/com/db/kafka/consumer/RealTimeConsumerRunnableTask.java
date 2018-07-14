package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.HostType;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class RealTimeConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(RealTimeConsumerRunnableTask.class);

	private static List<String> ucbTrackers = Arrays.asList("news-m-ucb", "news-ucb", "news-m-ucb_1", "news-ucb_1");

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	// private NLPParser nlpParser=new NLPParser();//NLPParser.getInstance();

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> summaryMessages = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> startRatingData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> feedbackData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storyDetailsData = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> storyDetailsDataHostWise = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> upvsData = new ArrayList<Map<String, Object>>();
	//private List<Map<String, Object>> realtimeTrackingData = new ArrayList<Map<String, Object>>();
	private Map<String, Integer> storyDetailRowIdMapForPvCounterUpdate = new HashMap<>();

	private List<String> storyIdsforCounterUpdate = new ArrayList<String>();
	private List<String> storyUniqueDetailsPVUpdateRowIds = new ArrayList<String>();
	private Map<String, Integer> storyUniqueDetailsPVRowidCounterMap = new HashMap<>();

	// UPDATE PVS IN STORY CONTENT DIVYA
	private Map<String, Integer> storyContentDivyaPVRowidCounterMap = new HashMap<>();
	private List<String> storyContentDivyaPVUpdateRowIds = new ArrayList<String>();

	// map key- PV type, value= Map of storyid to counter
	private Map<String, Map<String, Integer>> storySequenceFieldToIdMap = new HashMap<>();
	private Map<String, Map<String, Object>> storySequenceDataMap = new HashMap<>();

	// Objects for story_detail_hourly index
	private Map<String, Map<String, Object>> storyDetailHourlyDataMap = new HashMap<>();
	private Map<String, Integer> storyDetailHourlyRowidCounterMap = new HashMap<>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	private boolean isPrd = true;

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	// App & browser realtime read stories Tracking
	private List<Map<String, Object>> realtimeReadStoriesBrowser = new ArrayList<>();
	private List<Map<String, Object>> realtimeReadStoriesApp = new ArrayList<>();

	public RealTimeConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);
		storySequenceFieldToIdMap.put(Constants.WPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(Constants.MPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(Constants.APVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(Constants.IPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(Constants.UCBPVS, new HashMap<String, Integer>());
		storySequenceFieldToIdMap.put(Constants.OTHERPVS, new HashMap<String, Integer>());
	}

	public void shutdown() {
		consumer.wakeup();
		log.info(" Realtime Consumer wake up requested.");
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
		log.info("Realtime consumer stopped");
	}

	private void processMessage(byte[] message) {
		try {
			// Original Raw record.
			Map<String, Object> msg = decoder.decode(message);
			threadRecordCount = in.getAndIncrement();

			String session_id = (String) msg.get(Constants.SESSION_ID_FIELD);
			String sess_id = (String) msg.get(Constants.SESS_ID_FIELD);
			String storyId = (String) msg.get(Constants.STORY_ID_FIELD);
			String userCookieId = (String) msg.get(Constants.USER_COOKIE_ID);
			String splTracker = "-1";

			Map<String, Object> realtimeStories = new HashMap<>();
			realtimeStories.put(Constants.ROWID, session_id);
			realtimeStories.put(Constants.STORIES, storyId);

			if (msg.get(Constants.SPL_TRACKER) != null) {
				splTracker = msg.get(Constants.SPL_TRACKER).toString();
			}

			if (msg.containsKey(Constants.RATING)) {
				Map<String, Object> storyDetail = new HashMap<String, Object>();
				storyDetail.put(Constants.STORY_ID_FIELD, msg.get(Constants.STORY_ID_FIELD));
				storyDetail.put(Constants.SESSION_ID_FIELD, msg.get(Constants.SESSION_ID_FIELD));
				storyDetail.put(Constants.RATING, msg.get(Constants.RATING));
				storyDetail.put(Constants.ROWID, session_id + "_" + storyId);
				storyDetail.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				startRatingData.add(storyDetail);
				return;
			}

			if (msg.containsKey(Constants.FEEDBACK)) {
				// log.info("##########feedback exist##########");
				Map<String, Object> storyDetail = new HashMap<String, Object>();
				storyDetail.put(Constants.STORY_ID_FIELD, msg.get(Constants.STORY_ID_FIELD));
				storyDetail.put(Constants.SESSION_ID_FIELD, msg.get(Constants.SESSION_ID_FIELD));
				storyDetail.put(Constants.FEEDBACK, msg.get(Constants.FEEDBACK));
				storyDetail.put(Constants.ROWID, session_id + "_" + storyId);
				feedbackData.add(storyDetail);
				return;
			}

			Object classificationId = msg.get(Constants.CLASSFICATION_ID_FIELD);
			Object catId = msg.get(Constants.CAT_ID_FIELD);
			Object parentCatId = msg.get(Constants.PARENT_CAT_ID_FIELD);
			String datetime = ((String) msg.get(Constants.DATE_TIME_FIELD));
			String date = datetime.split("T")[0];

			// Index date suffix for realtime and app data
			String indexDate = date.replaceAll("-", "_");

			String storyRowId = storyId + "_" + msg.get(Constants.TRACKER) + "_" + splTracker + "_"
					+ msg.get(Constants.REF_PLATFORM) + "_" + msg.get(Constants.VERSION) + "_" + msg.get(Constants.HOST)
					+ "_" + date;

			String storyRowIdHourly = storyRowId + "_" + DateUtil.getHour(datetime);

			if (StringUtils.isNotBlank(storyId) && Integer.valueOf(storyId) != 0) {
				Map<String, Object> summaryData = new HashMap<String, Object>();
				summaryData.put(Constants.CAT_ID_FIELD, catId);
				summaryData.put(Constants.CLASSFICATION_ID_FIELD, classificationId);
				summaryData.put(Constants.SESSION_ID_FIELD, session_id);
				summaryData.put(Constants.STORY_ID_FIELD, storyId);
				if (msg.get(Constants.SESS_ID) != null) {
					summaryData.put(Constants.SESS_ID, msg.get(Constants.SESS_ID));
				}

				summaryData.put(Constants.PARENT_CAT_ID_FIELD, parentCatId);
				summaryData.put(Constants.SUPER_CAT_ID, msg.get(Constants.SUPER_CAT_ID));
				summaryData.put(Constants.SUPER_CAT_NAME, msg.get(Constants.SUPER_CAT_NAME));
				// Row Key calculation based on web and app data
				if (!((String) msg.get(Constants.TRACKER)).equalsIgnoreCase(Constants.APP)) {
					summaryData.put(Constants.ROWID, session_id + "_" + storyId);
					if(StringUtils.isNotBlank(session_id)){
					realtimeReadStoriesBrowser.add(realtimeStories);
				}
				}
				// If data is from app then index data based on
				// device token id
				else if (((String) msg.get(Constants.TRACKER)).equalsIgnoreCase(Constants.APP)
				// && StringUtils.isNotBlank((String)
				// msg.get(Constants.DEVICE_TOKEN))
				) {
					summaryData.put(Constants.ROWID, (String) msg.get(Constants.DEVICE_TOKEN) + "_" + storyId);
					summaryData.put(Constants.DEVICE_TOKEN, (String) msg.get(Constants.DEVICE_TOKEN));
					summaryData.put(Constants.APP_VERSION, msg.get(Constants.APP_VERSION));
					if(StringUtils.isNotBlank(session_id)){
					realtimeReadStoriesApp.add(realtimeStories);
				}
				}
				summaryData.put(Constants.HOST, msg.get(Constants.HOST));
				summaryData.put(Constants.DATE_TIME_FIELD, msg.get(Constants.DATE_TIME_FIELD));
				summaryData.put(Constants.STORY_ATTRIBUTE, msg.get(Constants.STORY_ATTRIBUTE));
				summaryData.put(Constants.REC_TYPE, msg.get(Constants.REC_TYPE));
				summaryData.put(Constants.STORY_PUBLISH_TIME, msg.get(Constants.STORY_PUBLISH_TIME));
				summaryData.put(Constants.STORY_MODIFIED_TIME, msg.get(Constants.STORY_MODIFIED_TIME));
				summaryData.put(Constants.TRACKER, msg.get(Constants.TRACKER));
				if (StringUtils.isNotBlank((String) msg.get(Constants.CITY))) {
					summaryData.put(Constants.CITY, msg.get(Constants.CITY));
				}
				if (StringUtils.isNotBlank((String) msg.get(Constants.STATE))) {
					summaryData.put(Constants.STATE, msg.get(Constants.STATE));
				}
				if (StringUtils.isNotBlank((String) msg.get(Constants.COUNTRY))) {
					summaryData.put(Constants.COUNTRY, msg.get(Constants.COUNTRY));
				}
				// 07-03-2017: Added author_name, uid
				summaryData.put(Constants.AUTHOR_NAME, msg.get(Constants.AUTHOR_NAME));
				summaryData.put(Constants.UID, msg.get(Constants.UID));
				summaryData.put(Constants.PGTOTAL, msg.get(Constants.PGTOTAL));
				summaryData.put(Constants.REF_HOST, msg.get(Constants.REF_HOST));
				summaryData.put(Constants.TITLE, msg.get(Constants.TITLE));
				summaryData.put(Constants.SECTION, msg.get(Constants.SECTION));
				summaryData.put(Constants.PGNO, msg.get(Constants.PGNO));
				summaryData.put(Constants.HOST_TYPE, msg.get(Constants.HOST_TYPE));
				summaryData.put(Constants.CHANNEL_SLNO, msg.get(Constants.CHANNEL_SLNO));
				summaryData.put(Constants.REF_PLATFORM, msg.get(Constants.REF_PLATFORM));
				summaryData.put(Constants.VERSION, msg.get(Constants.VERSION));
				summaryData.put(Constants.URL, msg.get(Constants.URL));

				// Added below fields for recommendation team
				summaryData.put(Constants.BROWSER_TITLE, msg.get(Constants.BROWSER_TITLE));
				summaryData.put(Constants.DESCRIPTION, msg.get(Constants.DESCRIPTION));
				summaryData.put(Constants.KEYWORDS, msg.get(Constants.KEYWORDS));
				summaryData.put(Constants.GA_SECTION, msg.get(Constants.GA_SECTION));
				summaryData.put(Constants.GA_SECTION1, msg.get(Constants.GA_SECTION1));
				summaryData.put(Constants.A_EVENT, msg.get(Constants.A_EVENT));
				summaryData.put(Constants.FLAG_V, msg.get(Constants.FLAG_V));

				// 29-03-2017: Added NLP fields
				summaryData.put(Constants.LOCATION, msg.get(Constants.LOCATION));
				summaryData.put(Constants.EVENT, msg.get(Constants.EVENT));
				summaryData.put(Constants.ORGANIZATION, msg.get(Constants.ORGANIZATION));
				summaryData.put(Constants.PEOPLE, msg.get(Constants.PEOPLE));
				summaryData.put(Constants.OTHER, msg.get(Constants.OTHER));
				summaryData.put(Constants.STORY_TYPE, msg.get(Constants.STORY_TYPE));
				summaryData.put(Constants.IMAGE, msg.get(Constants.IMAGE));
				summaryData.put(Constants.SELF_RATING, msg.get(Constants.SELF_RATING));

				// 13-03-2018: Added 3 new fields
				summaryData.put(Constants.ABBREVIATIONS, msg.get(Constants.ABBREVIATIONS));
				summaryData.put(Constants.SPL_TRACKER, msg.get(Constants.SPL_TRACKER));
				summaryData.put(Constants.URL_FOLDERNAME, msg.get(Constants.URL_FOLDERNAME));
				summaryData.put(Constants.USER_COOKIE_ID, msg.get(Constants.USER_COOKIE_ID));

				if (summaryData.containsKey(Constants.ROWID)) {
					summaryMessages.add(summaryData);
				}
				// 02-04-2018: Added 1 new fields
				
				/*Map<String, Object> upvsMap = new HashMap<String, Object>(summaryData);
				upvsMap.put(Constants.ROWID, session_id + "_" + storyId+"_"+sess_id+ "_"+splTracker+"_"+DateUtil.getCurrentDate().replaceAll("_", "-"));
				upvsData.add(upvsMap);

				Map<String, Object> realtimeTrackingMap = new HashMap<String, Object>(summaryData);
				realtimeTrackingMap.put(Constants.ROWID, userCookieId + "_" + storyId+"_"+sess_id+"_"+DateUtil.getCurrentDate().replaceAll("_", "-"));
				realtimeTrackingData.add(realtimeTrackingMap);*/
				
				Map<String, Object> upvsMap = new HashMap<String, Object>(summaryData);
				upvsMap.put(Constants.ROWID, storyId+"_"+sess_id+ "_"+splTracker+"_"+DateUtil.getCurrentDate().replaceAll("_", "-"));
				upvsData.add(upvsMap);

				/*
				 * START: Prepare data for story_detail_hourly
				 */
				Map<String, Object> storyDetailHourlyRecord = new HashMap<String, Object>(summaryData);
				storyDetailHourlyRecord.put(Constants.ROWID, storyRowIdHourly);
				storyDetailHourlyRecord.remove(Constants.CITY);
				storyDetailHourlyRecord.remove(Constants.STATE);
				storyDetailHourlyRecord.remove(Constants.COUNTRY);
				storyDetailHourlyRecord.remove(Constants.DEVICE_TOKEN);
				storyDetailHourlyRecord.remove(Constants.REC_TYPE);
				storyDetailHourlyRecord.remove(Constants.SESS_ID);
				storyDetailHourlyRecord.remove(Constants.SESSION_ID_FIELD);
				storyDetailHourlyRecord.remove(Constants.PGNO);
				storyDetailHourlyRecord.remove(Constants.IMAGE);
				storyDetailHourlyRecord.remove(Constants.A_EVENT);
				storyDetailHourlyRecord.remove(Constants.FLAG_V);

				storyDetailHourlyDataMap.put(storyRowIdHourly, storyDetailHourlyRecord);

				// Map of storyRowIdHourly to PV counter update
				if (storyDetailHourlyRowidCounterMap.containsKey(storyRowIdHourly)) {
					storyDetailHourlyRowidCounterMap.put(storyRowIdHourly,
							storyDetailHourlyRowidCounterMap.get(storyRowIdHourly) + 1);
				} else {
					storyDetailHourlyRowidCounterMap.put(storyRowIdHourly, 1);
				}

				/*
				 * END: Prepare data for story_detail_hourly
				 */

				// For PV update
				if (storyDetailRowIdMapForPvCounterUpdate.containsKey(storyRowId)) {
					storyDetailRowIdMapForPvCounterUpdate.put(storyRowId,
							storyDetailRowIdMapForPvCounterUpdate.get(storyRowId) + 1);
				} else {
					storyDetailRowIdMapForPvCounterUpdate.put(storyRowId, 1);
				}
				storyIdsforCounterUpdate.add(storyRowId);

				storyUniqueDetailsPVUpdateRowIds.add(storyId);
				// Map of storyid to PV counter update
				if (storyUniqueDetailsPVRowidCounterMap.containsKey(storyId)) {
					storyUniqueDetailsPVRowidCounterMap.put(storyId,
							storyUniqueDetailsPVRowidCounterMap.get(storyId) + 1);
				} else {
					storyUniqueDetailsPVRowidCounterMap.put(storyId, 1);
				}

				// TO UPDATE DIVYA STORY PVS IN STORY_CONTENT_DIVYA INDEX
				if (msg.get(Constants.CHANNEL_SLNO).toString().equalsIgnoreCase("960")) {
					storyContentDivyaPVUpdateRowIds.add(storyId);
					// Map of storyid to PV counter update
					if (storyContentDivyaPVRowidCounterMap.containsKey(storyId)) {
						storyContentDivyaPVRowidCounterMap.put(storyId,
								storyContentDivyaPVRowidCounterMap.get(storyId) + 1);
					} else {
						storyContentDivyaPVRowidCounterMap.put(storyId, 1);
					}
				}

				/*
				 * START: story_sequence data
				 */
				String storySequenceRowId = storyId + "_" + msg.get(Constants.PGNO);
				String hostType = (String) msg.get(Constants.HOST_TYPE);
				String tracker = (String) msg.get(Constants.TRACKER);
				if (hostType.equals(HostType.WEB)) {
					Map<String, Integer> wpvsData = storySequenceFieldToIdMap.get(Constants.WPVS);
					if (wpvsData.containsKey(storySequenceRowId)) {
						wpvsData.put(storySequenceRowId, wpvsData.get(storySequenceRowId) + 1);
					} else {
						wpvsData.put(storySequenceRowId, 1);
					}

				} else if (ucbTrackers.contains(tracker)) {
					Map<String, Integer> ucbpvsData = storySequenceFieldToIdMap.get(Constants.UCBPVS);
					if (ucbpvsData.containsKey(storySequenceRowId)) {
						ucbpvsData.put(storySequenceRowId, ucbpvsData.get(storySequenceRowId) + 1);
					} else {
						ucbpvsData.put(storySequenceRowId, 1);
					}

				} else if (hostType.equals(HostType.MOBILE)) {
					Map<String, Integer> mpvsData = storySequenceFieldToIdMap.get(Constants.MPVS);
					if (mpvsData.containsKey(storySequenceRowId)) {
						mpvsData.put(storySequenceRowId, mpvsData.get(storySequenceRowId) + 1);
					} else {
						mpvsData.put(storySequenceRowId, 1);
					}

				} else if (hostType.equals(HostType.ANDROID)) {
					Map<String, Integer> apvsData = storySequenceFieldToIdMap.get(Constants.APVS);
					if (apvsData.containsKey(storySequenceRowId)) {
						apvsData.put(storySequenceRowId, apvsData.get(storySequenceRowId) + 1);
					} else {
						apvsData.put(storySequenceRowId, 1);
					}

				} else if (hostType.equals(HostType.IPHONE)) {
					Map<String, Integer> ipvsData = storySequenceFieldToIdMap.get(Constants.IPVS);
					if (ipvsData.containsKey(storySequenceRowId)) {
						ipvsData.put(storySequenceRowId, ipvsData.get(storySequenceRowId) + 1);
					} else {
						ipvsData.put(storySequenceRowId, 1);
					}
				} else {
					Map<String, Integer> otherpvsData = storySequenceFieldToIdMap.get(Constants.OTHERPVS);
					if (otherpvsData.containsKey(storySequenceRowId)) {
						otherpvsData.put(storySequenceRowId, otherpvsData.get(storySequenceRowId) + 1);
					} else {
						otherpvsData.put(storySequenceRowId, 1);
					}
				}

				// Prepare Story Sequence
				Map<String, Object> storySequenceRecord = new HashMap<String, Object>();
				storySequenceRecord.put(Constants.ROWID, storySequenceRowId);
				storySequenceRecord.put(Constants.PGNO, msg.get(Constants.PGNO));
				storySequenceRecord.put(Constants.STORY_ID_FIELD, storyId);
				storySequenceRecord.put(Constants.DATE_TIME_FIELD, msg.get(Constants.DATE_TIME_FIELD));
				storySequenceDataMap.put(storySequenceRowId, storySequenceRecord);
			}
			/*
			 * END: story_sequence data
			 */

			Object checkObj = msg.remove("insert_story_detail");
			msg.remove("browser_key");

			// Add record for realtime mapping of raw table
			listMessages.add(msg);

			// // add to list for pv update
			// if (StringUtils.isNotBlank(storyId) &&
			// Integer.valueOf(storyId) != 0) {
			// storyIdsforCounterUpdate.add(storyRowId);
			// uniqueStoryDetailsPVUpdateRowIds.add(storyId);
			// }

			// Prepare story data
			if (checkObj != null) {
				boolean isInsertToStoryDetails = (Boolean) checkObj;
				if (isInsertToStoryDetails) {
					Map<String, Object> storyDetail = new HashMap<String, Object>();
					storyDetail.put(Constants.STORY_ID_FIELD, msg.get(Constants.STORY_ID_FIELD));
					storyDetail.put(Constants.TITLE, msg.get(Constants.TITLE));
					storyDetail.put(Constants.URL, msg.get(Constants.URL));
					storyDetail.put(Constants.CAT_ID_FIELD, msg.get(Constants.CAT_ID_FIELD));
					storyDetail.put(Constants.PARENT_CAT_ID_FIELD, msg.get(Constants.PARENT_CAT_ID_FIELD));
					storyDetail.put(Constants.SUPER_CAT_ID, msg.get(Constants.SUPER_CAT_ID));
					storyDetail.put(Constants.SUPER_CAT_NAME, msg.get(Constants.SUPER_CAT_NAME));
					storyDetail.put(Constants.STORY_ATTRIBUTE, msg.get(Constants.STORY_ATTRIBUTE));
					storyDetail.put(Constants.PGTOTAL, msg.get(Constants.PGTOTAL));
					storyDetail.put(Constants.TRACKER, msg.get(Constants.TRACKER));
					storyDetail.put(Constants.REF_PLATFORM, msg.get(Constants.REF_PLATFORM));
					storyDetail.put(Constants.VERSION, msg.get(Constants.VERSION));
					storyDetail.put(Constants.HOST, msg.get(Constants.HOST));
					storyDetail.put(Constants.CLASSFICATION_ID_FIELD, msg.get(Constants.CLASSFICATION_ID_FIELD));
					storyDetail.put(Constants.SECTION, msg.get(Constants.SECTION));
					storyDetail.put(Constants.IMAGE, msg.get(Constants.IMAGE));
					storyDetail.put(Constants.DIMENSION, msg.get(Constants.DIMENSION));
					storyDetail.put(Constants.DATE_TIME_FIELD, msg.get(Constants.DATE_TIME_FIELD));
					storyDetail.put(Constants.STORY_PUBLISH_TIME, msg.get(Constants.STORY_PUBLISH_TIME));
					storyDetail.put(Constants.STORY_MODIFIED_TIME, msg.get(Constants.STORY_MODIFIED_TIME));
					storyDetail.put(Constants.SELF_RATING, msg.get(Constants.SELF_RATING));
					// 06-03-2017: Added author_name, uid,host_type
					storyDetail.put(Constants.AUTHOR_NAME, msg.get(Constants.AUTHOR_NAME));
					storyDetail.put(Constants.UID, msg.get(Constants.UID));
					storyDetail.put(Constants.HOST_TYPE, msg.get(Constants.HOST_TYPE));

					// Added Channel_slno on 24-10-2015 to match
					// user-personalization with this
					storyDetail.put(Constants.CHANNEL_SLNO, msg.get(Constants.CHANNEL_SLNO));
					storyDetail.put(Constants.ROWID, storyRowId);

					// Added below fields for recommendation team
					storyDetail.put(Constants.BROWSER_TITLE, msg.get(Constants.BROWSER_TITLE));
					storyDetail.put(Constants.DESCRIPTION, msg.get(Constants.DESCRIPTION));
					storyDetail.put(Constants.KEYWORDS, msg.get(Constants.KEYWORDS));
					storyDetail.put(Constants.GA_SECTION, msg.get(Constants.GA_SECTION));
					storyDetail.put(Constants.GA_SECTION1, msg.get(Constants.GA_SECTION1));
					storyDetail.put(Constants.A_EVENT, msg.get(Constants.A_EVENT));
					storyDetail.put(Constants.FLAG_V, msg.get(Constants.FLAG_V));

					// 28-03-2017: Added NLP fields
					storyDetail.put(Constants.LOCATION, msg.get(Constants.LOCATION));
					storyDetail.put(Constants.EVENT, msg.get(Constants.EVENT));
					storyDetail.put(Constants.ORGANIZATION, msg.get(Constants.ORGANIZATION));
					storyDetail.put(Constants.PEOPLE, msg.get(Constants.PEOPLE));
					storyDetail.put(Constants.OTHER, msg.get(Constants.OTHER));
					storyDetail.put(Constants.STORY_TYPE, msg.get(Constants.STORY_TYPE));

					// 05-03-2018: Added 3 new fields
					storyDetail.put(Constants.ABBREVIATIONS, msg.get(Constants.ABBREVIATIONS));
					storyDetail.put(Constants.SPL_TRACKER, msg.get(Constants.SPL_TRACKER));
					storyDetail.put(Constants.URL_FOLDERNAME, msg.get(Constants.URL_FOLDERNAME));

					storyDetailsData.add(storyDetail);
					// Ignore APP data for story modification data
					if (!((String) msg.get(Constants.TRACKER)).equalsIgnoreCase(Constants.APP)) {
						Map<String, Object> storyDetailHostWise = new HashMap<String, Object>(storyDetail);
						storyDetailHostWise.put(Constants.ROWID, storyId);
						storyDetailsDataHostWise.add(storyDetailHostWise);
					} else {
						// In case of APP only insert NLP tags
						Map<String, Object> storyDetailHostWise = new HashMap<String, Object>();
						storyDetailHostWise.put(Constants.ROWID, storyId);
						storyDetailHostWise.put(Constants.LOCATION, msg.get(Constants.LOCATION));
						storyDetailHostWise.put(Constants.EVENT, msg.get(Constants.EVENT));
						storyDetailHostWise.put(Constants.ORGANIZATION, msg.get(Constants.ORGANIZATION));
						storyDetailHostWise.put(Constants.PEOPLE, msg.get(Constants.PEOPLE));
						storyDetailHostWise.put(Constants.OTHER, msg.get(Constants.OTHER));
						storyDetailHostWise.put(Constants.STORY_TYPE, msg.get(Constants.STORY_TYPE));
						storyDetailHostWise.put(Constants.TRACKER, msg.get(Constants.TRACKER));
						storyDetailHostWise.put(Constants.HOST, msg.get(Constants.HOST));
						storyDetailsDataHostWise.add(storyDetailHostWise);
					}
				}
			}

			// Start: Index the raw data in realtime table
			if (threadRecordCount % batchSize == 0) {
				log.info("Batch size: " + batchSize + "; ThreadRecordCount: " + threadRecordCount);
				insertData(indexDate);
			}

			if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
				log.info("Thread " + threadIndex + ", Flushing the Data in realtime tables..");
				insertData(indexDate);
			}
			// End: Index the raw data in realtime table

		} catch (Exception e) {
			log.error("Error in RealTimeConsumerRunnableTask", e);
		}
	}

	private void insertData(String indexDate) {
		String tableName = "realtime_" + indexDate;
		String upvsTableName = "realtime_upvs_" + indexDate;
		String realtimeTrackingTableName = "realtime_tracking_" + indexDate;
		resetTime();
		// Index the raw data in realtime mapping of daily table
		elasticSearchIndexService.index(tableName, listMessages);
		log.info("Thread " + threadIndex + ", Records inserted in realtime daily index, size: " + listMessages.size());

		listMessages.clear();
		// Index the summary data of unique story in unique_userstory
		// mapping of daily table
		// List<UpdateResponse> updateResponses = null;
		List<IndexResponse> indexResponses = null;
		List<IndexResponse> upvsIndexResponses = null;
		List<IndexResponse> realtimeTrackingIndexResponses = null;
		/*
		 * updateResponses=elasticSearchIndexService.indexOrUpdateWithResponse(
		 * tableName, MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY, summaryMessages);
		 */

		indexResponses = elasticSearchIndexService.indexWithResponse(tableName,
				MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY, summaryMessages);
		log.info("Thread " + threadIndex + ", Records inserted in realtime unique mapping index, size: "
				+ summaryMessages.size());
		log.info(tableName + "-" + MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY + " record count "
				+ summaryMessages.size() + "; Size of storyIds for PV Update " + storyIdsforCounterUpdate.size());
		summaryMessages.clear();

		upvsIndexResponses = elasticSearchIndexService.indexWithResponse(upvsTableName, MappingTypes.MAPPING_REALTIME,
				upvsData);
		log.info("Thread " + threadIndex + ", Records inserted in realtime upvs mapping index, size: "
				+ upvsData.size());
		log.info(upvsTableName + "-" + MappingTypes.MAPPING_REALTIME + " record count " + upvsData.size()
				+ "; Size of storyIds for PV Update " + storyIdsforCounterUpdate.size());
		upvsData.clear();


		/*realtimeTrackingIndexResponses = elasticSearchIndexService.indexWithResponse(realtimeTrackingTableName,
				MappingTypes.MAPPING_REALTIME, realtimeTrackingData);
		log.info("Thread " + threadIndex + ", Records inserted in realtime tracking mapping index, size: "
				+ realtimeTrackingData.size());
		log.info(realtimeTrackingTableName + "-" + MappingTypes.MAPPING_REALTIME + " record count "+ realtimeTrackingData.size());
		realtimeTrackingData.clear();*/


		// Index the story details in story_detail index
		if (storyDetailsData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.STORY_DETAIL),
					MappingTypes.MAPPING_REALTIME, storyDetailsData);
			log.info("Thread " + threadIndex + ", Records inserted in story_detail index, size: "
					+ storyDetailsData.size());
			storyDetailsData.clear();
		}

		// Index the unique story details in story_unique_detail index
		if (storyDetailsDataHostWise.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME,
					storyDetailsDataHostWise);
			log.info("Thread " + threadIndex + ", Records inserted in story_unique_detail index, size: "
					+ storyDetailsDataHostWise.size());
			storyDetailsDataHostWise.clear();
		}

/*		boolean shouldChangeInProfileIndex=Boolean.parseBoolean(DBConfig.getInstance().getProperty("realtime.shouldChangeProfileIndex"));
		
		if(shouldChangeInProfileIndex) {
		if (!realtimeReadStoriesBrowser.isEmpty()) {
				elasticSearchIndexService.indexOrUpdateAsArrayElements(Indexes.USER_PERSONALIZATION_STATS, MappingTypes.MAPPING_REALTIME,
						realtimeReadStoriesBrowser);

				log.info("Thread " + threadIndex + ", Records inserted in user_personalization_stats index, size: "
						+ realtimeReadStoriesBrowser.size());
				
		}

		if (!realtimeReadStoriesApp.isEmpty()) {
				elasticSearchIndexService.indexOrUpdateAsArrayElements(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						realtimeReadStoriesApp);

				log.info("Thread " + threadIndex + ", Records inserted in app_user_profile_device_id index, size: "
						+ realtimeReadStoriesApp.size());
				
			}
		}
		
		realtimeReadStoriesBrowser.clear();
		realtimeReadStoriesApp.clear();
*/
		if (isPrd) {

			// Process response for UV
			int indexOfResponse = 0;
			Map<String, Integer> storyDetailRowIDMapForUVCounterUpdate = new HashMap<>();
			for (IndexResponse indexResponse : upvsIndexResponses) {
				if (DocWriteResponse.Result.CREATED == indexResponse.getResult()) {
					String storyDetailRowId = storyIdsforCounterUpdate.get(indexOfResponse);
					if (storyDetailRowIDMapForUVCounterUpdate.containsKey(storyDetailRowId)) {
						storyDetailRowIDMapForUVCounterUpdate.put(storyDetailRowId,
								storyDetailRowIDMapForUVCounterUpdate.get(storyDetailRowId) + 1);
					} else {
						storyDetailRowIDMapForUVCounterUpdate.put(storyDetailRowId, 1);
					}
					// TODO: Story unique detail UV
				}
				indexOfResponse++;
			}
			// Update UV in story_detail index
			if (storyDetailRowIDMapForUVCounterUpdate.size() > 0) {
				elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.STORY_DETAIL),
						MappingTypes.MAPPING_REALTIME, storyDetailRowIDMapForUVCounterUpdate, Constants.UVS);
				log.info("Thread " + threadIndex + ", UVs updated in story_detail index, size: "
						+ storyDetailRowIDMapForUVCounterUpdate.size());
				storyDetailRowIDMapForUVCounterUpdate.clear();
			}
		}

		// Update PV in story_detail index
		if (storyIdsforCounterUpdate.size() > 0) {
			elasticSearchIndexService.incrementCounter(IndexUtils.getYearlyIndex(Indexes.STORY_DETAIL),
					MappingTypes.MAPPING_REALTIME, storyDetailRowIdMapForPvCounterUpdate, Constants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in story_detail index, size: "
					+ storyIdsforCounterUpdate.size());
			storyDetailRowIdMapForPvCounterUpdate.clear();
			storyIdsforCounterUpdate.clear();
		}

		// Update PV in story_unique_detail index
		if (storyUniqueDetailsPVRowidCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME,
					storyUniqueDetailsPVRowidCounterMap, Constants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in story_unique_detail index, size: "
					+ storyUniqueDetailsPVRowidCounterMap.size() + "; List size with duplicate stories: "
					+ storyUniqueDetailsPVUpdateRowIds.size());
			storyUniqueDetailsPVRowidCounterMap.clear();
		}
		storyUniqueDetailsPVUpdateRowIds.clear();

		// update pvs of divya bhaskar in story_content_divya
		if (storyContentDivyaPVRowidCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.STORY_CONTENT_DIVYA, MappingTypes.MAPPING_REALTIME,
					storyContentDivyaPVRowidCounterMap, Constants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in story_content_divya index, size: "
					+ storyContentDivyaPVRowidCounterMap.size() + "; List size with duplicate stories: "
					+ storyContentDivyaPVUpdateRowIds.size());
			storyContentDivyaPVRowidCounterMap.clear();
		}
		storyContentDivyaPVUpdateRowIds.clear();

		// Update storysequence data
		if (isPrd) {
			if (storySequenceDataMap.size() > 0) {
				List<Map<String, Object>> storySequenceDataList = new ArrayList<>(storySequenceDataMap.values());
				elasticSearchIndexService.indexOrUpdate(Indexes.STORY_SEQUENCE, MappingTypes.MAPPING_REALTIME,
						storySequenceDataList);
				log.info("Thread " + threadIndex + ", Records inserted in story_sequence index, size: "
						+ storySequenceDataMap.size());
				storySequenceDataMap.clear();
			}

			if (storySequenceFieldToIdMap.size() > 0) {
				for (String field : storySequenceFieldToIdMap.keySet()) {
					if (storySequenceFieldToIdMap.get(field).size() > 0) {
						elasticSearchIndexService.incrementCounter(Indexes.STORY_SEQUENCE,
								MappingTypes.MAPPING_REALTIME, storySequenceFieldToIdMap.get(field), field);
						log.info("Thread " + threadIndex + ", " + field + " PVs updated in " + Indexes.STORY_SEQUENCE
								+ " index, size: " + storySequenceFieldToIdMap.get(field).size());
						storySequenceFieldToIdMap.get(field).clear();
					}

				}
				// storySequenceFieldToIdMap.clear();
			}
		}

		/*// Update article rating data
		if (startRatingData.size() > 0) {
			log.info("Rating data is going to being ingested ");
			elasticSearchIndexService.indexOrUpdate(Indexes.ARTICLE_RATING_DETAIL, MappingTypes.MAPPING_REALTIME,
					startRatingData);
			log.info("Thread " + threadIndex + ", Article rating inserted in " + Indexes.ARTICLE_RATING_DETAIL
					+ " index, size: " + startRatingData.size());
			startRatingData.clear();
		}

		// Update article rating data
		if (feedbackData.size() > 0) {
			log.info("Feedback data is going to being ingested ");
			elasticSearchIndexService.indexOrUpdate(Indexes.ARTICLE_RATING_DETAIL, MappingTypes.MAPPING_REALTIME,
					feedbackData);
			log.info("Thread " + threadIndex + ", Feedback data inserted in realtime unique mapping index, size: "
					+ feedbackData.size());
			feedbackData.clear();
		}

		// story_detail_hourly data
		if (storyDetailHourlyDataMap.size() > 0) {
			List<Map<String, Object>> storyDetailHourlyData = new ArrayList<>(storyDetailHourlyDataMap.values());
			elasticSearchIndexService.indexOrUpdate(Indexes.STORY_DETAIL_HOURLY, MappingTypes.MAPPING_REALTIME,
					storyDetailHourlyData);
			log.info("Thread " + threadIndex + ", Records inserted in " + Indexes.STORY_DETAIL_HOURLY + " index, size: "
					+ storyDetailHourlyData.size());
			storyDetailHourlyDataMap.clear();
		}
		if (storyDetailHourlyRowidCounterMap.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.STORY_DETAIL_HOURLY, MappingTypes.MAPPING_REALTIME,
					storyDetailHourlyRowidCounterMap, Constants.PVS);
			log.info("Thread " + threadIndex + ", PVs updated in story_detail_hourly index, size: "
					+ storyDetailHourlyRowidCounterMap.size());
			storyDetailHourlyRowidCounterMap.clear();
		}*/

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}

}
