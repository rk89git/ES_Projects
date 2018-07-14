package com.db.common.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Cricket.PredictWinConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.GenericUtils;
import com.db.common.utils.IndexUtils;
import com.db.cricket.model.PredictAndWinBidsResponse;
import com.db.cricket.predictnwin.services.BidValidationService;
import com.db.kafka.codecs.KafkaUtils;
import com.db.kafka.producer.KafkaByteArrayProducerService;
import com.db.nlp.sentiment.SentimentAnalyser;
//import com.db.nlp.sentiment.SentimentAnalyser;
import com.db.notification.v1.enums.EventStatus;
import com.db.notification.v1.enums.EventType;
import com.db.notification.v1.model.Event;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class DataIngestionService {

	private KafkaByteArrayProducerService producerService = new KafkaByteArrayProducerService();
	private static Logger log = LogManager.getLogger(DataIngestionService.class);

	private KafkaUtils kafkaUtils = new KafkaUtils();
	
	SentimentAnalyser sentimentAnalyser = new SentimentAnalyser();

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private String realtimeTopic = DBConfig.getInstance().getString("ingestion.kafka.consumer.topic");

	private String predictAndWinTopic = DBConfig.getInstance().getString("kafka.predictwinbids.consumer.topic");
	
	@Autowired
	private BidValidationService validationService;

	private Client client = null;

	public DataIngestionService() {
		// For Kafka Generic Consumer
		KafkaIngestionGenericService kafkaIngestionGenericService = new KafkaIngestionGenericService();
		kafkaIngestionGenericService.execute();
		try {
			initializeClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public synchronized void ingestRealTimeData(Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(realtimeTopic, message);
	}

	public synchronized PredictAndWinBidsResponse ingestPredictAndWinBidsData(List<Map<String, Object>> recordList) {

//		double refundFactor = 0.0;
//
//		if (config.getProperty("predictwin.bid.reject.refund.factor") != null) {
//			refundFactor = Double.parseDouble(config.getProperty("predictwin.bid.reject.refund.factor"));
//		}

		String matchId = null;
		String userId = null;
		String vendorId = null;
		String pVendorId = null;
		int coinsDeducted = 0;
		int numberOfBidsAccepted = 0;
		String ticketId = null;
		int requestedBids = recordList.size();
		int refundGems = 0;
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}

			matchId = (String) record.get("matchId");
			userId = (String) record.get("userId");
			vendorId = (String) record.get("vendorId");
			pVendorId = (String) record.get("pVendorId");
			ticketId = (String) record.get("ticketId");

			int coinsBid = Integer.parseInt(String.valueOf(record.get("coinsBid")));

			if (validationService.isValid(record)) {
				record.put(PredictWinConstants.IS_BID_ACCEPTED, true);
				numberOfBidsAccepted++;
				coinsDeducted += coinsBid;
			} else {
				record.put(PredictWinConstants.IS_BID_ACCEPTED, false);
				coinsDeducted += coinsBid;
//				int refund = (int) (coinsBid * refundFactor);
//				refundGems += refund;
//				record.put(PredictWinConstants.REFUND_GEMS, refundGems);
			}

			byte[] message = kafkaUtils.toBytes(record);
			producerService.execute(predictAndWinTopic, message);
		}

		PredictAndWinBidsResponse bidsResponse = new PredictAndWinBidsResponse();
		bidsResponse.setMatchId(matchId);
		bidsResponse.setUserId(userId);
		bidsResponse.setVendorId(vendorId);
		bidsResponse.setpVendorId(pVendorId);
		bidsResponse.setCoinsDeduct(coinsDeducted);
		bidsResponse.setNoOfBidsAccepted(numberOfBidsAccepted);
		bidsResponse.setRequestedBids(requestedBids);
		bidsResponse.setTicketId(ticketId);
		bidsResponse.setRefundGems(refundGems);
		return bidsResponse;
	}

	public synchronized void ingestPollPlayData(Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(Constants.POLL, message);
	}

	public void indexData(String indexName, List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
			record.put(Constants.ROWID, String.valueOf(record.get(Constants.STORY_ID_FIELD)));
		}
		elasticSearchIndexService.index(indexName, recordList);
	}

	public void indexExplicitPersonalizationData(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}
		record.put(Constants.ROWID, String.valueOf(record.get(Constants.SESSION_ID_FIELD)));
		elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, ImmutableList.of(record));
	}

	public void index(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		if (indexName.equalsIgnoreCase(Indexes.USER_PERSONALIZATION_STATS)) {
			log.info(record);
			if (record.getOrDefault(Constants.NOTIFICATION_STATUS, Constants.NOTIFICATION_OFF)
					.equals(Constants.NOTIFICATION_OFF)) {
				record.put(Constants.NotificationConstants.UN_SUBSCRIBED_AT, record.get(Constants.DATE_TIME_FIELD));
			}
		}
		if (indexName.equalsIgnoreCase(Indexes.NOTIFICATIONS)) {
			record.put(Constants.NotificationConstants.AdminPanelConstants.UPDATED_AT,
					record.get(Constants.DATE_TIME_FIELD));
		} else if (indexName.equalsIgnoreCase(Indexes.USER_REGISTRATION)) {

			String vendorId = (String) record.get("vendorId");
			String uniqueUserId = (String) record.get("uniqueUserId");

			record.put(Constants.ROWID, uniqueUserId + "_" + vendorId);
			record.put(Constants.HOST, vendorId);

		}
		if (record.containsKey(Constants.ROWID)) {
			elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, ImmutableList.of(record));
		} else {
			elasticSearchIndexService.index(indexName, MappingTypes.MAPPING_REALTIME, ImmutableList.of(record));
		}

	}

	public void indexListofRecords(String indexName, List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
		}
		if (indexName.equals("fb_competitor_history")) {
			indexName = IndexUtils.getMonthlyIndex(indexName);
			indexData(Indexes.FB_COMPETITOR, recordList);
		}
		elasticSearchIndexService.index(indexName, recordList);

	}

	public Map<String, Object> indexWithResponse(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		Map<String, Object> responseMap = elasticSearchIndexService.indexWithResponseId(indexName,
				MappingTypes.MAPPING_REALTIME, record);

		return responseMap;
	}

	public int lotaMeIndexWithResponse(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}
		String rowId = record.get(Constants.SESSION_ID_FIELD) + "_" + record.get(Constants.PID);
		record.put(Constants.ROWID, rowId);
		return elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, record);
		/*
		 * if((!rowId.contains("null")) && (!rowId.endsWith("_"))){
		 * record.put(Constants.ROWID, rowId); return
		 * elasticSearchIndexService.indexOrUpdate(indexName,
		 * MappingTypes.MAPPING_REALTIME, record); }else{ return 0; }
		 */

	}

	@SuppressWarnings("unused")
	private void updateNotifyStatus(Map<String, Object> record) {
		Map<String, Object> notificationRecord = new HashMap<>();
		notificationRecord.put(Constants.ROWID, record.get(Constants.SUBSCRIBER_ID));
		if (record.get(Constants.STATUS).toString().equals("Y")) {
			notificationRecord.put(Constants.NOTIFICATION_STATUS, 1);
		} else {
			notificationRecord.put(Constants.NOTIFICATION_STATUS, 0);
		}
		notificationRecord.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

		elasticSearchIndexService.indexOrUpdate(Indexes.USER_PERSONALIZATION_STATS, MappingTypes.MAPPING_REALTIME,
				ImmutableList.of(notificationRecord));
	}

	/**
	 * Produce data to Kafka with topic name as similar to index name
	 *
	 * @param topicName
	 * @param record
	 */
	public void produce(String topicName, Map<String, Object> record) {
		String indexName = (String) record.get(Constants.INDEX_NAME);
		if (StringUtils.isBlank(indexName)) {
			throw new DBAnalyticsException("Invalid Request. indexName can not be empty in input record.");
		}

		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(topicName, message);
	}

	/**
	 * Produce data to Kafka with topic name as similar to index name
	 *
	 * @param record
	 */
	public void produce(Map<String, Object> record) {
		String indexName = (String) record.get(Constants.INDEX_NAME);
		if (StringUtils.isBlank(indexName)) {
			throw new DBAnalyticsException("Invalid Request. indexName can not be empty.");
		}

		String topicName = indexName.split("_")[0];
		produce(topicName, record);
	}

	public void ingestAppNotificationStatus(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}
		if (record.get(Constants.NotificationConstants.APP_ID_FIELD) != null) {
			if (record.get(Constants.NotificationConstants.APP_ID_FIELD).toString()
					.equals(Constants.AppId.APP_ID_BHASKAR_ANDROID)) {
				record.put(Constants.NotificationConstants.HOST_FIELD,
						Arrays.asList(Constants.Host.BHASKAR_APP_ANDROID_HOST));
			}
		}

		// Remove subscribedAt key if it's value is empty.
		if (StringUtils.isBlank((String) record.get(Constants.NotificationConstants.SUBSCRIBED_AT))) {
			record.remove(Constants.NotificationConstants.SUBSCRIBED_AT);
		}

		record.put(Constants.ROWID, String.valueOf(record.get(Constants.DEVICEID)));
		record.compute(Constants.NotificationConstants.APP_VC_FIELD,
				(key, value) -> value == null || value.toString().isEmpty() ? 0 : value);
		GenericUtils.addPaddedFields(record);
		int indexedRecord = elasticSearchIndexService.indexOrUpdateWithScriptAndAppend(indexName,
				MappingTypes.MAPPING_REALTIME, ImmutableList.of(record), Constants.NotificationConstants.HOST_FIELD);
		if (indexedRecord != 1) {
			throw new DBAnalyticsException("Notification status couldn't be updated due to some technical issues.");
		}
	}

	// ROHIT SHARMA NOTIFICATION EVENT
	private DBConfig config = DBConfig.getInstance();
	public void ingestAppNotificationEvent(Map<String, Object> record){
		byte[] message = kafkaUtils.toBytes(record);
//		System.out.println("Notification Record UPDATE in API notifyEvent. CASE 22222222222");
		String topicName = config.getString("ingestion.notification.event.kafka.consumer.topic");
		producerService.execute(topicName, message);
	}
	// ROHIT SHARMA NOTIFICATION EVENT

	public void ingestEventStatus(String jsonData) {
		String indexName = null;
		Event event = gson.fromJson(jsonData, Event.class);
		if (event.getProfileType().equalsIgnoreCase("WEB"))
			indexName = Indexes.USER_PERSONALIZATION_STATS;
		if (event.getProfileType().equalsIgnoreCase("APP"))
			indexName = Indexes.APP_USER_PROFILE;
		Map<String, Object> record = new HashMap<>();
		record.put(Constants.ROWID, event.get_id());
		EventType eventType = EventType.getEnumConstant(event.getType());
		String esFieldName = null;
		int indexedRecord = 0;
		esFieldName = eventType.getEsFieldName();
		EventStatus eventStatus = eventType.getEventStatus();
		record.put(esFieldName + "_" + event.getEntityType(), Arrays.asList(event.getEntityName()));
		if (eventStatus == EventStatus.ADD)
			indexedRecord = elasticSearchIndexService.indexOrUpdateWithScriptAndAppend(indexName,
					MappingTypes.MAPPING_REALTIME, ImmutableList.of(record), esFieldName + "_" + event.getEntityType());
		if (eventStatus == EventStatus.REMOVE)
			indexedRecord = elasticSearchIndexService.removeFromArrayElements(indexName, MappingTypes.MAPPING_REALTIME,
					ImmutableList.of(record), Arrays.asList(esFieldName + "_" + event.getEntityType()));
		if (indexedRecord != 1) {
			throw new DBAnalyticsException(
					event.getType() + " status couldn't be updated due to some technical issues.");
		}
	}

	public void identification(Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(Constants.IDENTIFICATION, message);
	}

	public void recommendation(Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(Constants.RECOMMENDATION, message);
	}

	public void facebookInsightsData(List<Map<String, Object>> recordList) {
		List<Map<String, Object>> fbInsightsRecordList = new ArrayList<>();

		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
			String fbDashboardRowId = (String) record.get(Constants.STORY_ID_FIELD);// +"_"+record.get(Constants.CHANNEL_SLNO);
			record.put(Constants.ROWID, fbDashboardRowId);

			Double field1 = 0.0;
			Double field2 = 0.0;
			Double field3 = 0.0;
			Double link_clicks = 1.0;
			Double unique_reach = 1.0;
			Double total_reach = 1.0;
			Double reactions = 1.0;
			Double reports = 1.0;
			Double shares = 1.0;

			if (record.get(Constants.LINK_CLICKS) != null) {
				link_clicks += ((Integer) record.get(Constants.LINK_CLICKS)).doubleValue();
			}
			if (record.get(Constants.UNIQUE_REACH) != null) {
				unique_reach += ((Integer) record.get(Constants.UNIQUE_REACH)).doubleValue();
			}
			if (record.get(Constants.TOTAL_REACH) != null) {
				total_reach += ((Integer) record.get(Constants.TOTAL_REACH)).doubleValue();
			}
			if (record.get(Constants.SHARES) != null) {
				shares += ((Integer) record.get(Constants.SHARES)).doubleValue();
			}
			if (record.get(Constants.REACTION_ANGRY) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_ANGRY)).doubleValue();
			}
			if (record.get(Constants.REACTION_HAHA) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_HAHA)).doubleValue();
			}
			if (record.get(Constants.REACTION_LOVE) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_LOVE)).doubleValue();
			}
			if (record.get(Constants.REACTION_SAD) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_SAD)).doubleValue();
			}
			if (record.get(Constants.REACTION_THANKFUL) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_THANKFUL)).doubleValue();
			}
			if (record.get(Constants.REACTION_WOW) != null) {
				reactions += ((Integer) record.get(Constants.REACTION_WOW)).doubleValue();
			}
			if (record.get(Constants.HIDE_CLICKS) != null) {
				reports += ((Integer) record.get(Constants.HIDE_CLICKS)).doubleValue();
			}
			if (record.get(Constants.HIDE_ALL_CLICKS) != null) {
				reports += ((Integer) record.get(Constants.HIDE_ALL_CLICKS)).doubleValue();
			}
			if (record.get(Constants.REPORT_SPAM_CLICKS) != null) {
				reports += ((Integer) record.get(Constants.REPORT_SPAM_CLICKS)).doubleValue();
			}

			if (link_clicks != 0) {
				field1 = unique_reach / link_clicks;
			}

			if (total_reach != 0) {
				field2 = (total_reach - unique_reach) / total_reach;
			}

			if (reports != 0) {
				field3 = (reactions * shares) / reports;
			}

			record.put(Constants.FIELD1, field1);
			record.put(Constants.FIELD2, field2);
			record.put(Constants.FIELD3, field3);

			// START: Prepare data for fb_insights_history
			/*String fbInsightHistoryRowId = (String) record.get(Constants.STORY_ID_FIELD) + "_"
					+ record.get(Constants.TOTAL_REACH) + "_" + record.get(Constants.LINK_CLICKS);*/
			
			Map<String, Object> fbInsightRecord = new HashMap<>(record);

			fbInsightRecord.remove(Constants.ROWID);
			fbInsightRecord.remove(Constants.FIELD1);
			fbInsightRecord.remove(Constants.FIELD2);
			fbInsightRecord.remove(Constants.FIELD3);
			fbInsightsRecordList.add(fbInsightRecord);
			// END: Prepare data for fb_insights_history	
		}
		elasticSearchIndexService.index(Indexes.FB_DASHBOARD, recordList);
		elasticSearchIndexService.index(IndexUtils.getYearlyIndex(Indexes.FB_INSIGHTS_HISTORY), fbInsightsRecordList);
	}

	public void facebookComments(List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
			int sentimentScore = sentimentAnalyser.findSentiment((String)record.get(Constants.MESSAGE));
			record.put(Constants.ROWID, record.get(Constants.COMMENT_ID));
			record.put(Constants.SCORE, sentimentScore);
		}
		elasticSearchIndexService.index(IndexUtils.getYearlyIndex(Indexes.FB_COMMENTS), recordList);
	}

	public int adMetricsDFP(List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
			String id = "";
			if (record.get(Constants.INTERVAL).toString().equals(Constants.DAY)) {
				id = record.get(Constants.DOMAIN).toString() + "_" + record.get(Constants.CATEGORY).toString() + "_"
						+ record.get(Constants.AD_UNIT_ID).toString() + "_" + record.get(Constants.DATE).toString()
						+ "_" + record.get(Constants.INTERVAL).toString();
			} else if (record.get(Constants.INTERVAL).toString().equals(Constants.MONTH)) {
				id = record.get(Constants.DOMAIN).toString() + "_" + record.get(Constants.CATEGORY).toString() + "_"
						+ record.get(Constants.AD_UNIT_ID).toString() + "_"
						+ record.get(Constants.DATE).toString().substring(0, 7) + "_"
						+ record.get(Constants.INTERVAL).toString();
			}
			record.put(Constants.ROWID, id);
		}
		return elasticSearchIndexService.index(Indexes.AD_METRICS, recordList);
	}

	public int adMetricsGA(List<Map<String, Object>> recordList) {
		List<Map<String, Object>> records = new ArrayList<>();
		for (Map<String, Object> record : recordList) {

			BoolQueryBuilder qb = new BoolQueryBuilder();
			if (record.get(Constants.DATE) != null) {
				qb.must(QueryBuilders.termQuery(Constants.DATE, record.get(Constants.DATE).toString()));
			}
			if (record.get(Constants.DOMAIN) != null) {
				qb.must(QueryBuilders.termQuery(Constants.DOMAIN, record.get(Constants.DOMAIN).toString()));
			}
			if (record.get(Constants.CATEGORY) != null) {
				qb.must(QueryBuilders.termQuery(Constants.CATEGORY, record.get(Constants.CATEGORY).toString()));
			}
			if (record.get(Constants.INTERVAL) != null) {
				qb.must(QueryBuilders.termQuery(Constants.INTERVAL, record.get(Constants.INTERVAL).toString()));
			}
			if (record.get(Constants.PLATFORM) != null) {
				qb.must(QueryBuilders.termQuery(Constants.PLATFORM, record.get(Constants.PLATFORM).toString()));
			}

			SearchResponse res = client.prepareSearch(Indexes.AD_METRICS).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).setSize(100).execute().actionGet();

			for (SearchHit hit : res.getHits().getHits()) {
				Map<String, Object> map = new HashMap<>();
				map.put(Constants.ROWID, hit.getId());
				map.putAll(record);
				records.add(map);
			}
		}
		return elasticSearchIndexService.indexOrUpdate(Indexes.AD_METRICS, MappingTypes.MAPPING_REALTIME, records);
	}

	public int fbPageData(List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			String rowId = record.get(Constants.CHANNEL_SLNO) + "_"
					+ record.get(Constants.DATE_TIME_FIELD).toString().substring(0, 10);
			record.put(Constants.ROWID, rowId);
		}
		return elasticSearchIndexService.indexOrUpdate(Indexes.FB_PAGE_INSIGHTS, MappingTypes.MAPPING_REALTIME,
				recordList);
	}

	public int pollCreationData(Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}
		if (!record.containsKey(Constants.POLL_ID)) {
			throw new DBAnalyticsException("Invalid record. poll_id cannot be empty.");
		}
		record.put(Constants.ROWID, record.get(Constants.POLL_ID));
		return elasticSearchIndexService.indexOrUpdate(Indexes.POLL_DETAILS, MappingTypes.MAPPING_REALTIME, record);
	}

	public static void main(String[] args) {
		int hash = 3;
		hash = 7 * hash + "120114390".hashCode();
		System.out.println(hash);

	}
	
	/**
	 * Update list of disabled players/teams during a match.
	 * 
	 * @param jsonData
	 * @return the number of records inserted.
	 */
	public int ingestDisabledMatchIds(List<Map<String, Object>> jsonData) {
		try {
			List<Map<String, Object>> matchDisabledIdsList = new ArrayList<>();

			for (Map<String, Object> matchObj : jsonData) {
				Map<String, Object> matchDisabledIdsMap = new HashMap<>();

				String matchId = (String) matchObj.get(IngestionConstants.MATCH_ID_FIELD);
				String matchPlayerTeamId = (String) matchObj.get(Constants.ID);
				String isEnabledId = (String) matchObj.get(IngestionConstants.ISENABLED);

				if (StringUtils.isNotBlank((String) matchId)) {

					if (StringUtils.isNotBlank((String) matchPlayerTeamId)) {
						matchDisabledIdsMap.put(Constants.ROWID, matchId + "_" + matchPlayerTeamId);
						matchDisabledIdsMap.put(Constants.ID, matchPlayerTeamId);
						matchDisabledIdsMap.put(IngestionConstants.MATCH_ID_FIELD, matchId);
						matchDisabledIdsMap.put(IngestionConstants.ISENABLED, Boolean.parseBoolean(isEnabledId));
						matchDisabledIdsMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					}

					matchDisabledIdsList.add(matchDisabledIdsMap);
				}
			}

			if (!matchDisabledIdsList.isEmpty()) {
				return elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_MATCH_DISABLED_IDS,
						MappingTypes.MAPPING_REALTIME, matchDisabledIdsList);
			}

		} catch (Exception e) {
			throw new DBAnalyticsException("Failed to update disabled match ids. ", e);
		}

		return 0;
	}

}
