package com.db.common.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.db.common.utils.DBConfig;
import com.db.kafka.consumer.CommentConsumerRunnableTask;
import com.db.kafka.consumer.ConsumerRunnableTask;
import com.db.kafka.consumer.IdentificationConsumerRunnableTask;
import com.db.kafka.consumer.NotificationContentConsumerRunnableTask;
import com.db.kafka.consumer.NotificationEventConsumerRunnableTask;
import com.db.kafka.consumer.NotificationUserProfileConsumer;
import com.db.kafka.consumer.PollConsumerRunnableTask;
import com.db.kafka.consumer.PredictAndWinConsumerRunnableTask;
import com.db.kafka.consumer.RealTimeConsumerRunnableTask;
import com.db.kafka.consumer.RealTimeListingPageConsumerRunnableTask;
import com.db.kafka.consumer.RecommendationConsumerRunnableTask;
import com.db.kafka.consumer.WisdomRealTimeConsumerRunnableTask;

//import com.db.services.impl.KafkaRealtimeIngestionConsumerService.ConsumerRunnableTask;
//import com.google.common.collect.ImmutableList;
public class KafkaIngestionGenericService  {

	private final Properties props;

	private DBConfig config = DBConfig.getInstance();

	private static List<RealTimeConsumerRunnableTask> startedRealtimeConsumers = new ArrayList<>() ;
	private static List<RealTimeListingPageConsumerRunnableTask> startedRealtimeListingPageConsumers = new ArrayList<>() ;
	private static List<WisdomRealTimeConsumerRunnableTask> startedWisdomRealtimeConsumers = new ArrayList<>() ;
	private static List<IdentificationConsumerRunnableTask> startedIdentificationConsumers = new ArrayList<>() ;
	private static List<ConsumerRunnableTask> startedOtherConsumers = new ArrayList<>() ;
	private static List<PollConsumerRunnableTask> startedPollConsumers = new ArrayList<>() ;
	private static List<PredictAndWinConsumerRunnableTask> startedPredictAndWinConsumers = new ArrayList<>() ;
	private static List<RecommendationConsumerRunnableTask> startedRecommendationConsumers = new ArrayList<>() ;
	private static List<CommentConsumerRunnableTask> startedDbCommentRunnableTask = new ArrayList<>() ;
	private static List<NotificationUserProfileConsumer> startedNotificationUserProfileConsumer = new ArrayList<>() ;
	private static List<ExecutorService> executors = new ArrayList<>() ;
	
	// DIVYA NOTIFICATION BY ROHIT SHARMA
	private static List<NotificationEventConsumerRunnableTask> startedNotificationEventRunnableTask = new ArrayList<>() ;
	private static List<NotificationContentConsumerRunnableTask> startedNotificationContentRunnableTask = new ArrayList<>();
	
	private static Logger log = LogManager.getLogger(KafkaIngestionGenericService.class);

	/**
	 * Constructor for all consumer
	 */
	public KafkaIngestionGenericService() {
		props = new Properties();
			
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.metadata.broker.list"));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.groups.id"));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
	}

	/**
	 * Execute method for all consumer
	 * 
	 */

	public void execute() {
		try {
			log.info("Starting realtime data ingestion Consumers");

			if (config.getString("kafka.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting RealTime Consumers START TIME : - " + System.currentTimeMillis());
				startRealTimeConsumers();
				startRealTimeListingPageConsumers();
			}
			
			if (StringUtils.isNotBlank(config.getString("kafka.wisdom.consumer.required"))
					&&config.getString("kafka.wisdom.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting RealTime Consumers START TIME : - " + System.currentTimeMillis());
				startWisdomRealTimeConsumers();
			}

			if (StringUtils.isNotBlank(config.getString("kafka.identification.consumer.required"))
					&& config.getString("kafka.identification.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting Identification Consumers START TIME : - " + System.currentTimeMillis());
				startIdentificationConsumers();
			}

			if (StringUtils.isNotBlank(config.getString("kafka.recommendation.consumer.required"))
					&& config.getString("kafka.recommendation.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting Recommendation Consumers START TIME : - " + System.currentTimeMillis());
				startRecommendationConsumers();
			}

			if (StringUtils.isNotBlank(config.getString("kafka.poll.consumer.required"))
					&& config.getString("kafka.poll.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting Poll Consumers START TIME : - " + System.currentTimeMillis());
				startPollConsumers();
			}
			
			if (StringUtils.isNotBlank(config.getString("kafka.predictwinbids.consumer.required"))
					&& config.getString("kafka.predictwinbids.consumer.required").trim().equalsIgnoreCase("true")) {
				log.info("Starting Predict and Win Bid Consumer START TIME : - " + System.currentTimeMillis());
				startPredictAndWinBidsConsumer();
			}
			

			if (StringUtils.isNotBlank(config.getString("kafka.other.consumers.required"))
					&& config.getString("kafka.other.consumers.required").trim().equalsIgnoreCase("true")) {
				String topics = config.getString("ingestion.kafka.consumer.other.topics");
				if (StringUtils.isNotBlank(topics)) {
					List<String> topicList = Arrays.asList(topics.split(","));
					for (String topicName : topicList) {
						log.info("Starting Consumers for topic: " + topicName + ", START TIME : - "
								+ System.currentTimeMillis());
						startConsumerForTopic(topicName);
					}
				}

			}
			
			if (StringUtils.isNotBlank(config.getString("kafka.comment.consumer.required"))
					&& config.getString("kafka.comment.consumer.required").trim().equalsIgnoreCase("true")) {
				String topics = config.getString("ingestion.comment.kafka.consumer.topic");
				if (StringUtils.isNotBlank(topics)) {
					List<String> topicList = Arrays.asList(topics.split(","));
					
					for (String topicName : topicList) {
						log.info("Starting Consumers for topic: " + topicName + ", START TIME : - "
								+ System.currentTimeMillis());
						startCommentConsumer(topicName);
					}
				}
			}
			
			// DIVYA NOTIFCIATION TRACKING
			if (StringUtils.isNotBlank(config.getString("kafka.notification.event.consumer.required"))
					&& config.getString("kafka.notification.event.consumer.required").trim().equalsIgnoreCase("true")) {
				String topics = config.getString("ingestion.notification.event.kafka.consumer.topic");
				if (StringUtils.isNotBlank(topics)) {
					List<String> topicList = Arrays.asList(topics.split(","));
					for (String topicName : topicList) {
						log.info("Starting Consumers for topic: " + topicName + ", START TIME : - "
								+ System.currentTimeMillis());
						startNotificationEventConsumer(topicName);
					}
				}

			}
			
			if (StringUtils.isNotBlank(config.getString("kafka.notificationUserProfile.consumers.required"))
					&& config.getString("kafka.notificationUserProfile.consumers.required").trim().equalsIgnoreCase("true")) {
				
				log.info("Starting Notification User Profile Consumer START TIME : - " + System.currentTimeMillis());
				startNotificationUserProfileConsumer();
			}
		
			// DIVYA NOTIFICATION STORY INGESTION
			if (StringUtils.isNotBlank(config.getString("kafka.notification.content.consumer.required")) && config
					.getString("kafka.notification.content.consumer.required").trim().equalsIgnoreCase("true")) {
				String topics = config.getString("ingestion.notification.content.kafka.consumer.topic");
				if (StringUtils.isNotBlank(topics)) {
					List<String> topicList = Arrays.asList(topics.split(","));
					for (String topicName : topicList) {
						log.info("Starting Consumers for topic: " + topicName + ", START TIME : - "
								+ System.currentTimeMillis());
						startNotificationContentConsumer(topicName);
					}
				}

			}

		} catch (Exception exception) {
			log.error("Failed to start the consumer : " + exception.getMessage(), exception);
		}
	}


	/**
	 * 
	 * @param numThreads
	 * @return
	 */
	private ExecutorService getExecutor(int numThreads) {
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		executors.add(executor);
		return executor;
	}

	private int getNumberOfThread() {
		return config.getInteger("kafka.num.partitions");
	}

	private void startRealTimeConsumers() {
		String wallTopic = config.getString("ingestion.kafka.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			RealTimeConsumerRunnableTask task = new RealTimeConsumerRunnableTask(threadIndex, topics, props);
			startedRealtimeConsumers.add(task);
			executorService.submit(task);
		}
	}
	
	private void startRealTimeListingPageConsumers() {
		String wallTopic = config.getString("ingestion.kafka.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			RealTimeListingPageConsumerRunnableTask task = new RealTimeListingPageConsumerRunnableTask(threadIndex, topics, props);
			startedRealtimeListingPageConsumers.add(task);
			executorService.submit(task);
		}
	}
	
	private void startWisdomRealTimeConsumers() {
		String wisdomTopic = config.getString("ingestion.kafka.wisdom.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wisdomTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			WisdomRealTimeConsumerRunnableTask task = new WisdomRealTimeConsumerRunnableTask(threadIndex, topics, props);
			startedWisdomRealtimeConsumers.add(task);
			executorService.submit(task);
		}
	}

	/*
	 * private void startStreamingForOtherConsumer(List<KafkaStream<byte[],
	 * byte[]>> streams, ExecutorService executor, int threadIndex) { for (final
	 * KafkaStream<byte[], byte[]> stream : streams) { ConsumerRunnableTasks
	 * task = new ConsumerRunnableTasks(stream, threadIndex);
	 * executor.submit(task); threadIndex++; } }
	 */

	private void startIdentificationConsumers() {
		String wallTopic = config.getString("kafka.identification.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			IdentificationConsumerRunnableTask task = new IdentificationConsumerRunnableTask(threadIndex, topics,
					props);
			startedIdentificationConsumers.add(task);
			executorService.submit(task);
		}
	}

	private void startRecommendationConsumers() {
		String wallTopic = config.getString("kafka.recommendation.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			RecommendationConsumerRunnableTask task = new RecommendationConsumerRunnableTask(threadIndex, topics,
					props);
			startedRecommendationConsumers.add(task);
			executorService.submit(task);
		}
	}

	private void startPollConsumers() {
		String wallTopic = config.getString("kafka.poll.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			PollConsumerRunnableTask task = new PollConsumerRunnableTask(threadIndex, topics, props);
			startedPollConsumers.add(task);
			executorService.submit(task);
		}
	}
	
	private void startPredictAndWinBidsConsumer() {
		String wallTopic = config.getString("kafka.predictwinbids.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(wallTopic);

		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			PredictAndWinConsumerRunnableTask task = new PredictAndWinConsumerRunnableTask(threadIndex, topics, props);
			startedPredictAndWinConsumers.add(task);
			executorService.submit(task);
		}
	}

	private void startConsumerForTopic(String topic) {
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			ConsumerRunnableTask task = new ConsumerRunnableTask(threadIndex, topics, props);
			startedOtherConsumers.add(task);
			executorService.submit(task);
		}
	}
	
	private void startCommentConsumer(String topic) {
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			CommentConsumerRunnableTask task = new CommentConsumerRunnableTask(threadIndex, topics, props);
			startedDbCommentRunnableTask.add(task);
			executorService.submit(task);
		}
	}
	
	private void startNotificationEventConsumer(String topic) {
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			NotificationEventConsumerRunnableTask task = new NotificationEventConsumerRunnableTask(threadIndex, topics, props);
			startedNotificationEventRunnableTask.add(task);
			executorService.submit(task);
		}
	}
	
	private void startNotificationUserProfileConsumer() {
		String topic = config.getString("kafka.notificationUserProfile.consumer.topic");
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		
		int numberOfThreads = Integer.parseInt(config.getProperty("kafka.notificationUserProfile.num.partitions"));

		ExecutorService executorService = getExecutor(numberOfThreads);
		for (int threadIndex = 0; threadIndex < numberOfThreads; threadIndex++) {
			NotificationUserProfileConsumer task = new NotificationUserProfileConsumer(threadIndex, topics, props);
			startedNotificationUserProfileConsumer.add(task);
			executorService.submit(task);
		}
	}
	
	private void startNotificationContentConsumer(String topic) {
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		ExecutorService executorService = getExecutor(getNumberOfThread());
		for (int threadIndex = 0; threadIndex < getNumberOfThread(); threadIndex++) {
			NotificationContentConsumerRunnableTask task = new NotificationContentConsumerRunnableTask(threadIndex,
					topics, props);
			startedNotificationContentRunnableTask.add(task);
			executorService.submit(task);
		}
	}

	/**
	 * This method stops all the Consumers consuming from
	 */
	private static void stopPredictAndWinConsumers() {
		log.info("Stopping predict and win consumers.");
		for (PredictAndWinConsumerRunnableTask task : startedPredictAndWinConsumers) {
			task.shutdown();
		}
	}
	
	private static void stopRealTimeConsumers() {
		log.info("Stopping realtime consumers.");
		for (RealTimeConsumerRunnableTask task : startedRealtimeConsumers) {
			task.shutdown();
		}
	}
	
	private static void stopRealTimeListingConsumers() {
		log.info("Stopping realtime listing consumers.");
		for (RealTimeListingPageConsumerRunnableTask task : startedRealtimeListingPageConsumers) {
			task.shutdown();
		}
	}

	private static void stopIdentificationConsumers() {
		log.info("Stopping idenfication consumers.");
		for (IdentificationConsumerRunnableTask task : startedIdentificationConsumers) {
			task.shutdown();
		}
	}

	private static void stopRecommendationConsumers() {
		log.info("Stopping recommendation consumers. ");
		for (RecommendationConsumerRunnableTask task : startedRecommendationConsumers) {
			task.shutdown();
		}
	}
	

	private static void stopPollConsumers() {
		log.info("Stopping poll consumers.");
		for (PollConsumerRunnableTask task : startedPollConsumers) {
			task.shutdown();
		}
	}

	private static void stopConsumerForCommentTopic() {
		log.info("Stopping comment consumers.");
		for (CommentConsumerRunnableTask task : startedDbCommentRunnableTask) {
			task.shutdown();
		}
	}
	
	private static void stopOtherConsumers() {
		log.info("Stopping other consumers.");
		for (ConsumerRunnableTask task : startedOtherConsumers) {
			task.shutdown();
		}
	}

	private static void stopExecutors() {
		log.info("Shutting down executors.");
		for (ExecutorService executor : executors) {
			executor.shutdownNow();
		}
	}
	
	private static void stopConsumerForNotificationEventTopic() {
		log.info("Stopping Notification Event consumers.");
		for (NotificationEventConsumerRunnableTask task : startedNotificationEventRunnableTask) {
			task.shutdown();
		}
	}
	
	private static void stopNotificationUserProfileConsumer() {
		log.info("Stopping Notification Event consumers.");
		for (NotificationUserProfileConsumer task : startedNotificationUserProfileConsumer) {
			task.shutdown();
		}
	}
	
	private static void stopConsumerForNotificationContentTopic() {
		log.info("Stopping Notification Content consumers.");
		for (NotificationContentConsumerRunnableTask task : startedNotificationContentRunnableTask) {
			task.shutdown();
		}
	}

	public static void main(String[] args) {
		KafkaIngestionGenericService genericService = new KafkaIngestionGenericService();
		genericService.execute();
	}

	public static void close(){
		
		log.info("Going to shutdown kafka consumers.");
		//Stop all types of consumers one by one 
		stopRealTimeConsumers();
		stopIdentificationConsumers();
		stopRecommendationConsumers();
		stopPollConsumers();
		stopOtherConsumers();
		stopConsumerForCommentTopic();
		stopRealTimeListingConsumers();
		stopPredictAndWinConsumers();
		stopConsumerForNotificationEventTopic();
		stopNotificationUserProfileConsumer();
		stopConsumerForNotificationContentTopic();
		//At last stop the executors on which the threads were running.
		stopExecutors();
	}

}
