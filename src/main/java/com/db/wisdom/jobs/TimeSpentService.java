package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.services.QueryExecutorService;
import com.db.common.utils.DateUtil;

public class TimeSpentService {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = null;

	private QueryExecutorService queryExecutorService = new QueryExecutorService();
	
	private static Logger log = LogManager.getLogger(TimeSpentService.class);

	public TimeSpentService() {
		try {
			if (this.client != null) {
				client.close();
			}
			this.client = elasticSearchIndexService.getClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public void timeSpentCron() {
		long startTime = System.currentTimeMillis();
		log.info("START TIME: "+DateUtil.getCurrentDateTime());
		try {
			SearchResponse sr = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(QueryBuilders.matchAllQuery())
					.addAggregation(
							AggregationBuilders.terms("channel").field(Constants.CHANNEL_SLNO)
									.size(20).subAggregation(AggregationBuilders.terms("stories")
											.field(Constants.STORY_ID_FIELD).size(50)))
					.setSize(0).execute().actionGet();
			Terms channelBuckets = sr.getAggregations().get("channel");
			int numThread = 10;
			log.info("Channel count: "+channelBuckets.getBuckets().size());
			ExecutorService executorService = Executors.newFixedThreadPool(numThread);
			for (Terms.Bucket channel : channelBuckets.getBuckets()) {
				List<String> storyIdList = new ArrayList<>();
				Terms storyBuckets = channel.getAggregations().get("stories");
				for (Terms.Bucket story : storyBuckets.getBuckets()) {
					storyIdList.add(story.getKeyAsString());
				}
				log.info("Story count for channel " + channel.getKey() + " is: "
						+ storyBuckets.getBuckets().size());
				executorService.execute(new TimeSpent(storyIdList));

			}		
			
			executorService.shutdown();

			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		log.info("END TIME: "+DateUtil.getCurrentDateTime());
		long endTime = System.currentTimeMillis();
		log.info("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));
	}

	public class TimeSpent extends Thread {
		List<String> stories;

		public TimeSpent(List<String> stories) {
			this.stories = stories;
		}

		public void run() {
			queryExecutorService.calculateTimeSpent(stories,true);
			queryExecutorService.calculateTimeSpent(stories,false);			
		}
	}

	public static void main(String[] args) {
		// if (args.length!=3) {
		// System.err.println(
		// "Usage: java -jar <path/to/RecUserProfileCreator.jar> <NUMBER OF
		// RECORDS TO PROCESSED> <DAY COUNT> <THREAD COUNT>");
		// System.exit(0);
		// }

		TimeSpentService timeSpentService = new TimeSpentService();
		timeSpentService.timeSpentCron();
		System.exit(0);
	}

}
