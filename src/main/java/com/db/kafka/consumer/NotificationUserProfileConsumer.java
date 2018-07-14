package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class NotificationUserProfileConsumer implements Runnable {
	
	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(NotificationUserProfileConsumer.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private int threadIndex = 0;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
	KafkaConsumer<String, byte[]> consumer;
	List<String> topics;
	
	private long batchSleepInterval = 0;
	
	public NotificationUserProfileConsumer(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);
		
		if(StringUtils.isNotBlank(config.getProperty("kafka.notificationUserProfile.batch.sleep.interval.ms"))){
			batchSleepInterval = Long.valueOf(config.getProperty("kafka.notificationUserProfile.batch.sleep.interval.ms"));
		}
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
			log.info(getClass().getName() + "-threadNumer " + threadIndex + " is going to shutdown.");
		} finally {
			consumer.close();
		}
	}
	
	private void processMessage(byte[] message) {
		Map<String, Object> msg = null;
		
		try{
			msg = decoder.decode(message);
			int counter = 0;

			if(msg.get(Indexes.USER_PROFILE_DAYWISE) != null){
				Map<String, Object> indexData = (Map<String, Object>) msg.get(Indexes.USER_PROFILE_DAYWISE);

				counter = elasticSearchIndexService.updateDocWithIncrementCounter(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
						(Map<String, Map<String, Integer>>) indexData.get("counter"),
						(Map<String, Map<String, Object>>) indexData.get("updateDoc"));
				
				log.info("Thread "+threadIndex+ "- update records "+counter+" for index "+Indexes.USER_PROFILE_DAYWISE);
			}

			if(msg.get(Indexes.APP_USER_PROFILE) != null){
				Map<String, Object> indexData = (Map<String, Object>) msg.get(Indexes.APP_USER_PROFILE);

				counter = elasticSearchIndexService.updateAndAppendDocWithIncrementCounter(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						(Map<String, Map<String, Integer>>) indexData.get("counter"), 
						(Map<String, Map<String, Object>>) indexData.get("updateDoc"), 
						(Map<String, Map<String, Collection<?>>>) indexData.get("arrayDoc"));
				
				log.info("Thread "+threadIndex+ "- update records "+counter+" for index "+Indexes.APP_USER_PROFILE);
			}

			if(msg.get(Indexes.USER_PERSONALIZATION_STATS) != null){
				Map<String, Object> indexData = (Map<String, Object>) msg.get(Indexes.USER_PERSONALIZATION_STATS);

				counter = elasticSearchIndexService.updateAndAppendDocWithIncrementCounter(Indexes.USER_PERSONALIZATION_STATS, MappingTypes.MAPPING_REALTIME,
						(Map<String, Map<String, Integer>>) indexData.get("counter"), 
						(Map<String, Map<String, Object>>) indexData.get("updateDoc"), 
						(Map<String, Map<String, Collection<?>>>) indexData.get("arrayDoc"));
				
				log.info("Thread "+threadIndex+ "- update records "+counter+" for index "+Indexes.USER_PERSONALIZATION_STATS);
			}

			try{
				Thread.sleep(batchSleepInterval);
			}catch(Exception e){
				log.info("Batch sleep Interval exit");
			}

		}catch(Exception e){
			log.error("message - ", msg);
			log.error("Thread "+threadIndex+"- Error while consuming record for notification user profile.", e);
		}
	}

}
