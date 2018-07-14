package com.db.wisdom.product.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;

import com.db.common.constants.Indexes.WisdomIndexes;
import com.db.common.constants.WisdomConstants;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.services.KafkaIngestionGenericService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.kafka.codecs.KafkaUtils;
import com.db.kafka.producer.KafkaByteArrayProducerService;

public class WisdomProductDataIngestionService {

	private KafkaByteArrayProducerService producerService = new KafkaByteArrayProducerService();

	private KafkaUtils kafkaUtils = new KafkaUtils();

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private String wisdomRealtimeTopic = DBConfig.getInstance().getString("ingestion.kafka.wisdom.consumer.topic");

	private Client client = null;

	public WisdomProductDataIngestionService() {
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

	public synchronized void ingestWisdomRealTimeData(Map<String, Object> record) {
		if (!record.containsKey(WisdomConstants.DATETIME)) {
			record.put(WisdomConstants.DATETIME, DateUtil.getCurrentDateTime());
		}

		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(wisdomRealtimeTopic, message);
	}

	public void facebookInsightsData(List<Map<String, Object>> recordList) {
		List<Map<String, Object>> fbInsightsRecordList = new ArrayList<>();

		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(WisdomConstants.DATETIME)) {
				record.put(WisdomConstants.DATETIME, DateUtil.getCurrentDateTime());
			}
			String fbDashboardRowId = (String) record.get(WisdomConstants.FACEBOOK_STORY_ID);
			record.put(WisdomConstants.ROWID, fbDashboardRowId);

			Map<String, Object> fbInsightRecord = new HashMap<>(record);

			fbInsightRecord.remove(WisdomConstants.ROWID);
			fbInsightsRecordList.add(fbInsightRecord);
		}
		elasticSearchIndexService.index(WisdomIndexes.WISDOM_FB_DASHBOARD, recordList);
		elasticSearchIndexService.index(IndexUtils.getMonthlyIndex(WisdomIndexes.WISDOM_FB_INSIGHTS_HISTORY), fbInsightsRecordList);
		
	}

}
