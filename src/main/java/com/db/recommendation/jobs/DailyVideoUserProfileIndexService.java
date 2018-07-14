package com.db.recommendation.jobs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.google.common.collect.Multimap;

public class DailyVideoUserProfileIndexService {

	private Client client = null;

	/** The Constant CLUSTER_NAME. */
	private static final String CLUSTER_NAME = "cluster.name";

	private int countRecordsToIndex = 0;

	private int dayCountForSummaryCalculation = 7;

	/** The settings. */
	private Settings settings = null;

	/** The host map. */
	private Multimap<String, Integer> hostMap = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(DailyVideoUserProfileIndexService.class);

	private DBConfig config = DBConfig.getInstance();

	private int numThreads = 5;

	public DailyVideoUserProfileIndexService(Integer countRecordsToIndex, Integer dayCountForSummaryCalculation) {
		this.countRecordsToIndex = countRecordsToIndex;
		this.dayCountForSummaryCalculation = dayCountForSummaryCalculation;
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public DailyVideoUserProfileIndexService() {
		initializeClient();
		log.info("Connection initialized with ElasticSearch.");
	}

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public void run() {
		long startTime = System.currentTimeMillis();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			String toDateUnderscoreFormat = DateUtil.getPreviousDate();

			String videoIndexName = "video_tracking_"
					+ toDateUnderscoreFormat.substring(0, toDateUnderscoreFormat.lastIndexOf("_"));
			// Start of unique users calculation
			Set<String> userIds = new HashSet<String>();
			int startFrom = 0;
			// String realtimeIndex = "video_tracking_2016_09";
			System.out.println("Searching  in index " + videoIndexName);

			String fromDateUnderscoreFormat = DateUtil.getPreviousDate(toDateUnderscoreFormat,
					-(dayCountForSummaryCalculation - 1));

			String suffixVideoIndexTo = toDateUnderscoreFormat.substring(0, toDateUnderscoreFormat.lastIndexOf("_"));
			String suffixVideoIndexFrom = fromDateUnderscoreFormat.substring(0,
					fromDateUnderscoreFormat.lastIndexOf("_"));
			String fromDate = fromDateUnderscoreFormat.replaceAll("_", "-");
			String toDate = toDateUnderscoreFormat.replaceAll("_", "-");

			List<String> indexes = new ArrayList<String>();
			indexes.add(videoIndexName);
			if (!suffixVideoIndexTo.equals(suffixVideoIndexFrom)) {
				indexes.add("video_tracking_" + suffixVideoIndexFrom);
			}

			String[] indexArray = indexes.toArray(new String[indexes.size()]);

			startFrom = 0;

			BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
			boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(toDate).to(toDate));
			boolQueryBuilder.mustNot(QueryBuilders.matchQuery(Constants.CAT_ID_FIELD, 8));

			SearchResponse scrollResp = client.prepareSearch(videoIndexName).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQueryBuilder).setScroll(new TimeValue(60000))
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setSize(1000).execute().actionGet();

			while (true) {
				startFrom += scrollResp.getHits().getHits().length;
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				System.out.println("index: " + videoIndexName + " total: " + scrollResp.getHits().getTotalHits()
						+ ", Fetched Hits: " + scrollResp.getHits().getHits().length + " , Total fetched : "
						+ startFrom);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					if (!hit.getSource().containsKey(Constants.SESSION_ID_FIELD)) {
						continue;
					}
					try {
						String sessionId = (String) hit.getSource().get(Constants.SESSION_ID_FIELD);
						if (StringUtils.isNotBlank(sessionId)) {
							userIds.add(sessionId);
						}
					} catch (Exception e) {
						log.error("Error getting information", e);
						continue;
					}
				}
				if (countRecordsToIndex > 0 && userIds.size() > countRecordsToIndex) {
					break;
				}
				System.out.println("Unique users: " + userIds.size());
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
			// End of unique users calculation

			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
			ArrayList<String> session_id_lst = new ArrayList<String>();
			session_id_lst.addAll(userIds);
			System.out.println("Total users count:" + session_id_lst.size());
			int lstSize = session_id_lst.size();
			int range_diff = (lstSize / numThreads);
			int start_range = 0;
			int end_range = range_diff;

			System.out.println("List of indexes to calculate stats: " + indexes);
			for (int i = 0; i < numThreads; i++) {
				System.out.println("Thread " + i);
				System.out.println("Start Range: " + start_range);
				System.out.println("End Range: " + end_range);
				executorService.execute(new SummaryDeriveRunnable(i, session_id_lst.subList(start_range, end_range - 1),
						indexArray, fromDate, toDate));
				start_range = end_range;
				end_range = end_range + range_diff;
			}

			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long endTime = System.currentTimeMillis();
			System.out.println("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public class SummaryDeriveRunnable implements Runnable {
		private final List<String> sessionIds;

		private String indexesForSummary[];
		int totalCount = 0;
		int threadIndex = 0;
		String fromDate = null;
		String toDate = null;

		SummaryDeriveRunnable(int threadIndex, List<String> sessionIds, String indexesForSummary[], String fromDate,
				String toDate) {
			this.sessionIds = sessionIds;
			this.indexesForSummary = indexesForSummary;
			this.threadIndex = threadIndex;
			this.fromDate = fromDate;
			this.toDate = toDate;

		}

		@Override
		public void run() {
			System.out.println("Executing thread " + threadIndex);
			long indexedRecordCount = 0;
			List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
			for (String userSessionId : sessionIds) {
				try {
					BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
					boolQueryBuilder.must(QueryBuilders.matchQuery(Constants.SESSION_ID_FIELD, userSessionId))
							.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(fromDate).to(toDate));
					boolQueryBuilder.mustNot(QueryBuilders.matchQuery(Constants.CAT_ID_FIELD, 8));

					SearchResponse sr = client.prepareSearch(indexesForSummary).setTypes(MappingTypes.MAPPING_REALTIME)
							.setTimeout(new TimeValue(2000)).setQuery(boolQueryBuilder)
							.addAggregation(AggregationBuilders.avg("AverageQuartile").field("quartile"))
							.addAggregation(AggregationBuilders.terms("CAT_ID").field("cat_id").size(5)).setSize(25)
							.execute().actionGet();

					int totalStories = 0;

					Avg avg = sr.getAggregations().get("AverageQuartile");

					Map<String, Object> record = new HashMap<String, Object>();
					record.put(Constants.ROWID, userSessionId);
					record.put("avg_quartile", avg.getValue());

					// ------------------------------- CAT ID
					// ---------------------------------//
					Terms result = sr.getAggregations().get("CAT_ID");
					List<Integer> catIdList = new ArrayList<Integer>();
					for (Terms.Bucket entry : result.getBuckets()) {
						if (entry.getKey() != null && !((String) entry.getKeyAsString()).equalsIgnoreCase("0")) {
							catIdList.add(Integer.valueOf((String) entry.getKey()));
							if (catIdList.size() == 5) {
								break;
							}
						}
					}
					record.put("video_cat_id", catIdList);

					if (sr.getHits().getTotalHits() > 0) {
						// Get total number of stories for the user
						totalStories = (int) (sr.getHits().getTotalHits());
						record.put("video_count", totalStories);
					}

					List<Integer> videoIds = new ArrayList<Integer>();

					for (SearchHit searchHit : sr.getHits()) {
						String videoId = (String) searchHit.getSourceAsMap().get(Constants.VIDEO_ID);
						if (StringUtils.isNotBlank(videoId)) {
							videoIds.add(Integer.valueOf((String) searchHit.getSourceAsMap().get(Constants.VIDEO_ID)));
						}
					}

					record.put(Constants.VIDEO_ID, videoIds);

					record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

					indexedRecordCount++;
					System.out.println("User profile build for " + userSessionId);
					listMessages.add(record);
					if (listMessages.size() == 1000) {
						elasticSearchIndexService.indexOrUpdate(Indexes.VIDEO_USER_PROFILE,
								MappingTypes.MAPPING_REALTIME, listMessages);
						listMessages.clear();
						System.out.println("Thread " + threadIndex + ", Records indexed : " + indexedRecordCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
					log.error("Error getting unique visits for device_token_id: " + userSessionId + " exception. ", e);
					System.out.println(
							"Error getting unique visits for device_token_id: " + userSessionId + " exception " + e);
					continue;
				}
			}

			if (listMessages.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(Indexes.VIDEO_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						listMessages);
				listMessages.clear();
				System.out.println("Thread " + threadIndex + ", Records indexed: " + indexedRecordCount);
			}
		}
	}

	public static void main(String[] args) {
		DailyVideoUserProfileIndexService duvi = new DailyVideoUserProfileIndexService(Integer.valueOf(args[0]),
				Integer.valueOf(args[1]));
		duvi.run();
	}

}