package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class PollConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(PollConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	// private NLPParser nlpParser=new NLPParser();//NLPParser.getInstance();

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
	private long startTime = System.currentTimeMillis();
	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public PollConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
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
			Map<String, Object> msg = decoder.decode(message);
			threadRecordCount = in.getAndIncrement();
			listMessages.add(msg);
			// Start: Index the raw data in realtime table
			if (threadRecordCount % batchSize == 0) {
				insertData();
			}

			if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
				log.info("Thread " + threadIndex + ", Flushing the Data in poll_play index.");
				insertData();
			}
		} // End: Index the raw data in realtime table
		catch (Exception e) {
			log.error("Error in PollConsumerRunnableTask. ", e);
		}
	}

	private void insertData() {
		resetTime();
		// Index the poll play data in poll data index
		if (listMessages.size() > 0) {
			elasticSearchIndexService.index(Indexes.POLL_PLAY, MappingTypes.MAPPING_REALTIME, listMessages);
			log.info("Thread " + threadIndex + ", Records inserted in poll_play index , size: " + listMessages.size());
			listMessages.clear();
		}

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
}
