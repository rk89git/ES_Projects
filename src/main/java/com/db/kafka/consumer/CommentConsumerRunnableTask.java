package com.db.kafka.consumer;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.db.comment.services.CommentNotificationHelper;
import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.GenericUtils;
import com.db.kafka.codecs.KafkaMessageDecoder;

@Configuration
@EnableScheduling
public class CommentConsumerRunnableTask implements Runnable {

	private DBConfig config = DBConfig.getInstance();

	private static Logger log = LogManager.getLogger(CommentConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private long flushInterval = Long.valueOf(DBConfig.getInstance().getProperty("kafka.flush.interval.ms"));
	
	private CommentNotificationHelper commentNotificationHelper = new CommentNotificationHelper();
	
	ExecutorService eService = Executors.newFixedThreadPool(7);
	
	private JSONObject filtersRule = new JSONObject();

	private AtomicInteger in = new AtomicInteger(0);
	private int threadIndex = 0;
	private int threadRecordCount = 1;
	private KafkaMessageDecoder decoder = new KafkaMessageDecoder();
	private List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();

	private List<String> inReplyToCommentsList = new ArrayList<String>();
	private List<String> reportAbuseCountList = new ArrayList<String>();
	private List<String> likesCountList = new ArrayList<String>();

	private long startTime = System.currentTimeMillis();
	private int batchSize = getBatchSize();

	private HashSet<String> vulgarKeywords = new HashSet<>();
	
	private static List<Map<String, Object>> spamWordsList = new ArrayList<Map<String, Object>>();

	private static Client client = ElasticSearchIndexService.getInstance().getClient();

	private int getBatchSize() {
		if (config.getProperty("comment.consumer.batch.size") != null)
			return Integer.valueOf(config.getProperty("comment.consumer.batch.size"));
		else
			return Integer.valueOf(config.getProperty("consumer.batch.size"));
	}

	List<String> topics;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;

	public CommentConsumerRunnableTask(int threadIndex, List<String> topics, Properties props) {
		this.threadIndex = threadIndex;
		this.topics = topics;
		consumer = new KafkaConsumer<>(props);
		getNegativeKeywords();
		getFilterRules();
	}
	
	public CommentConsumerRunnableTask() {
		getCommentSpamWords();
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
		try {// Original Raw record.
			Map<String, Object> msg = decoder.decode(message);
			log.info("recieved records by consumer : " +msg);
			threadRecordCount = in.getAndIncrement();

			String eventIn = (String) msg.get(Constants.COMMENT_EVENT);
			String commentId = (String) msg.get(Constants.ID);

			int status_flag = 0;
			if (eventIn == null) {
				String inReplyToCommentId = (String) msg.get(Constants.IN_REPLY_TO_COMMENT_ID);
				if (StringUtils.isNotBlank(inReplyToCommentId)) {
					inReplyToCommentsList.add(inReplyToCommentId);
				}
				// START: Logic for abusive comment				
				String description = (String) msg.get(Constants.DESCRIPTION);	
				
				String commentDescription = (String) msg.get(Constants.DESCRIPTION);
				
				StringBuilder modifiedDescrption = new StringBuilder();
				if (StringUtils.isNotBlank(description)) {
					description = description.replaceAll("[\\p{Punct}\\s]+", " ").trim();
					StringTokenizer tkr = new StringTokenizer(description);
					String word = "";
					while (tkr.hasMoreTokens()) {
						word = tkr.nextToken();
						if (StringUtils.isNotBlank(word) && vulgarKeywords.contains(word.toLowerCase())) {
							modifiedDescrption.append(" ").append(getModifiedAbusiveWord(word));
							status_flag = Constants.CommentStatus.ABUSIVE;
						} else {
							modifiedDescrption.append(" ").append(word);
						}
					}
					if (status_flag != 0) {
						msg.put(Constants.STATUS, status_flag);
					}
				}
				
				//Check SPAM comment regex based
				boolean isSpam = executeFilterRule(msg, commentDescription);
				
				//Check SPAM comment spam dictionary based
				checkSpamComment(msg, commentDescription);

				// word.substring(word.length()-2, word.length()-1)
				if(status_flag==Constants.CommentStatus.ABUSIVE)
				msg.put(Constants.MODIFIED_DESCRIPTION, modifiedDescrption.toString().trim());
				else
					msg.put(Constants.MODIFIED_DESCRIPTION,msg.get(Constants.DESCRIPTION));
				// END: Logic for abusive comment
				listMessages.add(msg);
				if(!isSpam){
					// method to push Notification
					log.info("going for checknotification");
					checkForNotification(msg);
				}else{
					log.info("Comment is spam not going for checknotification");
				}
			} else {
				if (eventIn.equalsIgnoreCase(Constants.LIKE)) {					
					likesCountList.add(commentId);				
					//checkForNotification(msg);
					//log.info("sendingLikesNotification"+msg);
				} 
				else if (eventIn.equalsIgnoreCase(Constants.ABUSE)) {
					reportAbuseCountList.add(commentId);
				}

			}
			// Start: Index the data into table
			if (threadRecordCount % batchSize == 0) {
				log.info("batchSize " + batchSize);
				insertData();
			}

			if (System.currentTimeMillis() - startTime >= flushInterval) {
				log.info("Thread " + threadIndex + ", Flushing the Data in commenting indexes..");
				insertData();
			}
			log.info("SUCESSFULLY PROCESSED: ");
			// End: Index the raw data in realtime table
		} catch (Exception e) {
			log.error("An error occurred in IdentificationConsumerRunnableTask.", e);
		}
	}

	private void insertData() {
		String indexName = Indexes.DB_COMMENT;
		resetTime();
		// Index the comments
		if (listMessages.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, listMessages);
			log.info("Thread " + threadIndex + ", Records inserted in db_comment index, size: " + listMessages.size());

			listMessages.clear();
		}

		// Increment reply counter
		if (inReplyToCommentsList.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.DB_COMMENT, MappingTypes.MAPPING_REALTIME,
					inReplyToCommentsList, Constants.REPLY_COUNT, 1);

			log.info("Thread " + threadIndex + ", Reply Count updated in db_comment " + " index, size: "
					+ inReplyToCommentsList.size());
			inReplyToCommentsList.clear();
		}

		// increment like counter
		if (likesCountList.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.DB_COMMENT, MappingTypes.MAPPING_REALTIME,
					likesCountList, Constants.LIKE_COUNT, 1);
			log.info("Thread " + threadIndex + ", Like Count updated in db_comment " + " index, size: "
					+ likesCountList.size());
			likesCountList.clear();
		}

		// increment report abuse counter
		if (reportAbuseCountList.size() > 0) {
			elasticSearchIndexService.incrementCounter(Indexes.DB_COMMENT, MappingTypes.MAPPING_REALTIME,
					reportAbuseCountList, Constants.REPORT_ABUSE_COUNT, 1);
			log.info("Thread " + threadIndex + ", Report Abuse Count updated in db_comment " + " index, size: "
					+ reportAbuseCountList.size());
			reportAbuseCountList.clear();

		}

	}

	private String getModifiedAbusiveWord(String word) {
		return word.substring(0, 1) + word.substring(1, word.length() - 1).replaceAll(".", "*")
				+ word.substring(word.length() - 1, word.length());
	}

	private void getNegativeKeywords() {
		try {
			vulgarKeywords.addAll(Files.readAllLines(Paths.get(config.getProperty("comment.vulgar.keywords")),
					Charset.defaultCharset()));
			log.info("Successfully loaded negative keywords");
		} catch (Exception e) {
			log.error("Could not load negative keywords file", e);
		}

	}

	private void resetTime() {
		startTime = System.currentTimeMillis();
	}
	
	private void checkForNotification(Map<String, Object> msg) {
		if (msg.containsKey(Constants.IS_REPLY)) {
			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {

						commentNotificationHelper.sendNotification(msg);
						log.info("Comment Notification For Reply Submitted");

					return null;
				}

			});
		}
		
		/*if (msg.containsKey(Constants.EVENT)) {
			log.info("records"+msg);
			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					if ((StringUtils.equals((String) msg.get(Constants.EVENT), Constants.LIKE))) {
						commentNotificationHelper.sendNotification(msg);
						log.info("message"+msg);
						log.info("Comment Notification For Like Submitted");
					}
					return null;
				}

			});
		}*/

	}
	
	private void getFilterRules(){
		String path = config.getProperty("comment.filter.rules");
		if(path != null && !path.trim().equals("")){
			filtersRule = GenericUtils.jsonFileReader(path);
		}
	}
	
	private boolean executeFilterRule(Map<String, Object> msg, String description){
		boolean isSpam = false;
		try{
			if(filtersRule.containsKey(Constants.Comment.FILTERS)){
				JSONArray filters = (JSONArray) filtersRule.get(Constants.Comment.FILTERS);

				for(Object filter: filters){
					JSONObject filterRules = (JSONObject) filtersRule.get(filter.toString());

					if(filterRules != null){
						long status_flag = (long) filterRules.get(Constants.STATUS);
						JSONArray rules = (JSONArray) filterRules.get(Constants.Comment.FILTER_RULES);

						for(Object rule: rules){
							Pattern pattern = Pattern.compile(rule.toString());
							Matcher matcher = pattern.matcher(description); 

							if(matcher.find()){
								msg.put(Constants.STATUS, status_flag);
								isSpam = true;
								break;
							}
						}
					}
				}
			}
		}catch(Exception e){
			log.error("Error while checking comments spam with regex");
		}
		return isSpam;
	}
	
	private boolean checkSpamComment(Map<String, Object> msg, String description) {
		boolean isSpam = false;
		int status_flag = Constants.CommentStatus.SPAMS;
		String descriptionLowerCase = description.toLowerCase();
		
		for(Map<String, Object> spamWordMap: spamWordsList){
			if(spamWordMap.containsKey(Constants.Comment.SPAM_WORD)){
				String spamWord = (String)spamWordMap.get(Constants.Comment.SPAM_WORD);
				
				if(!spamWord.equals("") && descriptionLowerCase.contains(spamWord)){
					msg.put(Constants.STATUS, status_flag);
					isSpam=true;
					break;
				}
			}
		}
		
		return isSpam;
	}
	
	@Scheduled(fixedDelay = 300000)
	private void getCommentSpamWords() {
		try {
			List<Map<String, Object>> spamWordList = new ArrayList<Map<String, Object>>();

			BoolQueryBuilder cqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.Comment.SPAM_WORD_ISENABLE, true));
			SearchResponse ser = client.prepareSearch(Indexes.COMMENT_SPAM_WORDS).setTypes(MappingTypes.MAPPING_REALTIME)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).setSize(2000).setQuery(cqb).execute().actionGet();

			SearchHit[] searchHits = ser.getHits().getHits();

			for (SearchHit hit : searchHits) {				
				spamWordList.add(hit.getSource());
			}
			spamWordsList = spamWordList;
			
			log.info("Successfully loaded Spam words");
		} catch (Exception e) {
			log.error("Error while loading Spam words");
		}
	}
}