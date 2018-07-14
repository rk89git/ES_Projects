

package com.db.comment.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.KeyGenerator;
import com.db.kafka.codecs.KafkaUtils;
import com.db.kafka.producer.KafkaByteArrayProducerService;

@Service
public class CommentIngestionService {

	private KafkaByteArrayProducerService producerService = new KafkaByteArrayProducerService();

	private KafkaUtils kafkaUtils = new KafkaUtils();	
	
	private static Logger log = LogManager.getLogger(CommentIngestionService.class);
	
	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	
	private String commentTopic = DBConfig.getInstance().getString("ingestion.comment.kafka.consumer.topic");
		
	public void ingestCommentingUserData(String indexName, List<Map<String, Object>> recordList) {
		for (Map<String, Object> record : recordList) {
			if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
				record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			}
			record.put(Constants.ROWID, String.valueOf(record.get(Constants.ID)));
		}
		elasticSearchIndexService.index(indexName, MappingTypes.MAPPING_REALTIME, recordList);
	}
	
	
	public String ingestUsersCommentData(Map<String, Object> record) {

		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());		
		}
		String key = KeyGenerator.getKey();			
		
		if(StringUtils.isBlank((String)record.get(Constants.ID))){
		record.put(Constants.ID, key);
		record.put(Constants.ROWID, key);
    }		
				
		byte[] message = kafkaUtils.toBytes(record);
		producerService.execute(commentTopic, message);
		return key;
	}

	public static void main(String[] args) {
		
		Map<String, Object> mp = new HashMap<String, Object>(); 
		mp.put(Constants.DESCRIPTION, "one of the most offensive word should be bable");
		
		CommentIngestionService pp = new CommentIngestionService();
		pp.ingestUsersCommentData(mp);
		
	}
	
}
