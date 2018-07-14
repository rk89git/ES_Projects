package com.db.kafka.consumer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.kafka.codecs.KafkaMessageDecoder;

@Configuration
@EnableScheduling
public class NotificationContentConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(NotificationContentConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	ExecutorService eService = Executors.newFixedThreadPool(7);

	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listNotificationEvent = new ArrayList<Map<String, Object>>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = getBatchSize();
	
	private AtomicInteger in = new AtomicInteger(0);

	private int getBatchSize() {
		if (config.getProperty("comment.consumer.batch.size") != null)
			return Integer.valueOf(config.getProperty("comment.consumer.batch.size"));
		else
			return Integer.valueOf(config.getProperty("consumer.batch.size"));
	}

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	Long batchSleepInterval = (long) 0;

	public NotificationContentConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);

		if (StringUtils.isNotBlank(
				DBConfig.getInstance().getProperty("kafka.notification.batch.sleep.interval.ms"))) {
			batchSleepInterval = Long.valueOf(
					DBConfig.getInstance().getProperty("kafka.notification.batch.sleep.interval.ms"));
		}
	}

	public NotificationContentConsumerRunnableTask() {
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
		// log.info("NOTIFICATION EVENT recieved records by consumer :CASE 3333 ");
		try {
			threadRecordCount=in.getAndIncrement();
			Map<String, Object> inputRecord = decoder.decode(message);
			listNotificationEvent.add(inputRecord);
			// Start: Index the data into table
			if (threadRecordCount % batchSize == 0) {
				log.info("batchSize " + batchSize);
				insertData();
			}
			if (System.currentTimeMillis() - startTime >= flushInterval) {
				log.info("Thread " + threadIndex
						+ ", Notification Detail CONTENT API Flushing the Data in notification detail indexes..");
				insertData();
			}
			log.info("Notification Detail content API . SUCESSFULLY PROCESSED");
			Thread.sleep(batchSleepInterval);
			log.info("Batch sleep Interval exit");
			// End: Index the raw data in realtime table
		} catch (Exception e) {
			log.error("ERROR in AUTOMATED PUSH Notification Detail CONTENT API.", e);
		}
	}

	private void insertData() {
		// log.info("NOTIFICATION EVENT recieved records by consumer :CASE 444");
		Calendar calendar = Calendar.getInstance();
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int month = calendar.get(Calendar.MONTH) + 1;
		int year = calendar.get(Calendar.YEAR);
		String indexMonth, indexDay;
		if (day <= 9) {
			indexDay = "0" + day;
		} else {
			indexDay = "" + day;
		}
		if (month <= 9) {
			indexMonth = "0" + month;
		} else {
			indexMonth = "" + month;
		}
		String notificationIndex = Constants.NotificationConstants.DIVYA_NOTIFICATION_INDEX + indexDay + "_"
				+ indexMonth + "_" + year;
		resetTime();
		// Index the comments
		if (!listNotificationEvent.isEmpty()) {
			elasticSearchIndexService.indexOrUpdate(notificationIndex, MappingTypes.MAPPING_REALTIME,
					listNotificationEvent);
			log.info("Thread " + threadIndex + ", AUTOMATED PUSH NOTIFICATION CONTENT Records inserted in index, " + notificationIndex
					+ " & RECORD SIZE " + listNotificationEvent.size());
			listNotificationEvent.clear();
		}
	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
}