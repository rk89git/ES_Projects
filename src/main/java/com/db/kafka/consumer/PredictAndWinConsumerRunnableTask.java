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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;

import com.db.common.constants.Constants;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.kafka.codecs.KafkaMessageDecoder;

public class PredictAndWinConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(PredictAndWinConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));

	private AtomicInteger in = new AtomicInteger(0);

	private int threadIndex = 0;

	private int threadRecordCount = 1;

	private List<String> topics = null;

	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();

	List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
	List<String> impressionCounterIncrement = new ArrayList<String>();
	List<String> clickCounterIncrement = new ArrayList<String>();

	private long startTime = System.currentTimeMillis();

	private int batchSize = Integer.valueOf(config.getProperty("consumer.batch.size"));

	private KafkaConsumer<String, byte[]> consumer;

	public PredictAndWinConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);

		Thread dameonThread=new Thread() {

			@Override
			public void run() {
				while(true){
					try {
						insertData();
						Thread.sleep(flushInterval);
						
					} catch (InterruptedException e) {
						log.warn("flush thread now stopping",e);
						return;
					}catch (Exception e) {
						log.warn("flush thread now stopping",e);
						return;
					}
				}
			}
		};
		dameonThread.setDaemon(true);
		dameonThread.start();

	}

	public PredictAndWinConsumerRunnableTask(int threadIndex, Properties props) {

		this.threadIndex = threadIndex;
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
				// System.out.println("Recieved Message");
				ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
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

	private void processMessage(byte[] recordValue) {
		try {
			Map<String, Object> msg = decoder.decode(recordValue);
			log.info("message recieved for predict and win bids consumer : "+msg);
			threadRecordCount = in.getAndIncrement();

			String storyId = (String) msg.get("userId");
			String matchId = (String) msg.get("matchId");
			String ticketId = (String) msg.get("ticketId");
			String bidType = (String) msg.get("bidType");
			String bidTypeId = (String) msg.get("bidTypeId");

			if (!StringUtils.isEmpty(storyId) && !StringUtils.isEmpty(matchId) && !StringUtils.isEmpty(ticketId)
					&& !StringUtils.isEmpty(bidType) && !StringUtils.isEmpty(bidTypeId)) {
				String rowId = storyId + "_" + matchId + "_" + ticketId + "_" + bidType + "_" + bidTypeId;
				msg.put(Constants.ROWID, rowId);
				synchronized (listMessages) {
					listMessages.add(msg);
				}
			} else
				log.info("key is empty for storyId : " + storyId + "matchId : " + matchId + " ticketId : " + ticketId
						+ " bidType : " + bidType + " bidTypeId :" + bidTypeId);

			if (threadRecordCount % batchSize == 0) {
				insertData();
			}

			if (System.currentTimeMillis() - startTime >= flushInterval && listMessages.size() > 0) {
				log.info("Thread " + threadIndex + ", Flushing the Data in users_bids index ");
				insertData();
			}
			// End: Index the raw data in realtime table

		} catch (Exception e) {
			log.error("Error in ingesting data in user_bids index  " + getClass().getName(), e);
		}
	}

	private void insertData() {
		synchronized(listMessages){
			
			if(listMessages.size() > 0){
				try{
					String tableName = "users_bids";
					resetTime();
					elasticSearchIndexService.index(tableName, listMessages);
					log.info("Thread " + threadIndex + ", Records inserted in " + tableName + " index, size: "
							+ listMessages.size());
					listMessages.clear();
				}catch(Exception e){
					throw new RuntimeException(e);
				}
			}
		}

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}

}
