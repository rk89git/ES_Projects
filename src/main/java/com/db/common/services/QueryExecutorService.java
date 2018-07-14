package com.db.common.services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.ArticleParams;
import com.db.common.model.FetchRecordsCountByHost;
import com.db.common.model.GenericQuery;
import com.db.common.model.Hours;
import com.db.common.model.PVResponse;
import com.db.common.model.RealtimeRecord;
import com.db.common.model.StatsDate;
import com.db.common.model.UVResponse;
import com.db.common.model.UserFrequencyQuery;
import com.db.common.model.UserHistory;
import com.db.common.model.UserHourlyStats;
import com.db.common.model.UserPersonalizationQuery;
import com.db.common.model.UserPersonalizationStory;
import com.db.common.model.WisdomUvsPvs;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.GenericUtils;
import com.db.common.utils.IndexUtils;
import com.db.common.utils.SelfExpiringHashMap;
import com.db.common.utils.TimerArrayList;
import com.db.notification.v1.model.NotificationQuery;
import com.db.notification.v1.model.PushNotificationHistory;
import com.db.wisdom.model.WisdomQuery;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * @author Hanish Bansal
 *
 */
@Service
public class QueryExecutorService {

	private Client client = null;

	private static Logger log = LogManager.getLogger(QueryExecutorService.class);
	// private static final Logger log = LogManager.getLogger("query_executor");

	private DBConfig config = DBConfig.getInstance();

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	/**
	 * Map Object to auto expire after one hour
	 */

	private SelfExpiringHashMap<Integer, String> flickerTrendingStoriesCache = new SelfExpiringHashMap<>(3600 * 1000);

	private SelfExpiringHashMap<String, Object> avgStoryTimeCache = new SelfExpiringHashMap<>();

	private Map<String, TimerArrayList<Integer>> cacheRHS = new HashMap<String, TimerArrayList<Integer>>();

	private Map<String, TimerArrayList<Integer>> cacheMostTrendingStories = new HashMap<String, TimerArrayList<Integer>>();

	private Map<String, TimerArrayList<Integer>> cacheTopUcbStories = new HashMap<String, TimerArrayList<Integer>>();

	private Map<String, TimerArrayList<Integer>> cacheMicromaxBrowserStories = new HashMap<String, TimerArrayList<Integer>>();

	String[] facebookTrackerArray = { "news-fbo", "news-hf-fbo" };
	
	private String[] excludeEditorIds = {"254","262"};

	public QueryExecutorService() {
		initializeClient();
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	/**
	 * Return history(records of list of unique story ids visited of a user
	 * 
	 * @param sessionId
	 * @return
	 */
	public Map<String, UserHistory> getHistoryOfUser(String sessionId) {
		// sessionId=
		// "26ba8fba-a893-c8b4-a3b8-e047f58c07db";
		// "ebe38fba-66e7-318d-5dee-a4fd7df77e97";
		Map<String, UserHistory> records = new LinkedHashMap<String, UserHistory>();
		int count = 0;
		long startTime = System.currentTimeMillis();
		String queryDate = DateUtil.getCurrentDate();
		outerLoop: for (int i = 0; i < 15; i++) {
			try {
				SearchResponse sr = client.prepareSearch("realtime_" + queryDate).setTypes("realtime")
						.setTimeout(new TimeValue(2000)).setQuery(QueryBuilders.matchQuery("session_id", sessionId))
						.setSize(100).addSort("datetime", SortOrder.DESC).execute().actionGet();
				SearchHit[] searchHits = sr.getHits().getHits();
				for (SearchHit searchHit : searchHits) {
					if (!searchHit.getSourceAsMap().containsKey("storyid")) {
						continue;
					}
					Object obj = searchHit.getSourceAsMap().get("storyid");
					if (obj != null) {
						String storyId = (String.valueOf(obj));// .toString()));
						if (StringUtils.isNotBlank(storyId) & (!storyId.trim().equalsIgnoreCase("0"))) {
							if (!records.containsKey(storyId)) {
								UserHistory userHistory = new UserHistory();
								userHistory.setStory_id(searchHit.getSourceAsMap().get("storyid").toString());
								userHistory.setDatetime(searchHit.getSourceAsMap().get("datetime").toString());
								userHistory.setImage(searchHit.getSourceAsMap().get("image").toString());
								userHistory.setTitle(searchHit.getSourceAsMap().get("title").toString());
								userHistory.setUrl(searchHit.getSourceAsMap().get("url").toString());
								records.put(storyId, userHistory);
								count++;
								if (count == 50) {
									log.info("Maximum count 50 reached till " + queryDate);
									break outerLoop;
								}
							}
						}
					}
					// log.info((searchHit.getFields().get("datetime").getValue().toString()));
					// log.info((searchHit.getFields().get("storyid").getValue().toString()));
				}
				queryDate = DateUtil.getPreviousDate(queryDate);
			} catch (IndexNotFoundException ime) {
				log.error(ime);
				queryDate = DateUtil.getPreviousDate(queryDate);
			} catch (Exception e) {
				log.error(e);
				queryDate = DateUtil.getPreviousDate(queryDate);
			}
		}

		// log.info(records);
		// log.info(records.size());
		long endTime = System.currentTimeMillis();
		log.info("Time to retrieve history: " + (endTime - startTime) / 1000);
		log.info("History of user [" + sessionId + "] , Records size: " + records.size());
		return records;
	}

	public List<Integer> getStoryIdHistoryOfUser(String sessionId, Integer storyCount) {
		List<Integer> records = new ArrayList<Integer>();
		long startTime = System.currentTimeMillis();
		String currentDate = DateUtil.getCurrentDate();
		// String previousDate = DateUtil.getPreviousDate(currentDate);
		long totalHits = 0;

		try {
			SearchResponse sr = client.prepareSearch("realtime_" + currentDate)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
					.setQuery(QueryBuilders.matchQuery("session_id", sessionId)).setSize(storyCount)
					// .addSort("datetime", SortOrder.DESC)
					.execute().actionGet();
			SearchHit[] searchHits = sr.getHits().getHits();
			totalHits = sr.getHits().getTotalHits();

			for (SearchHit searchHit : searchHits) {
				Integer storyId = (Integer) searchHit.getSourceAsMap().get(Constants.STORY_ID_FIELD);
				records.add(storyId);
			}
		} catch (Exception e) {
			log.error("Error occurred in receiving user story-id history.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Story-id history of user [" + sessionId + "] , Total Hits: " + totalHits + ", Result size: "
				+ records.size() + ", Execution Time (seconds): " + (endTime - startTime) / 1000.0);
		return records;
	}

	@SuppressWarnings("unchecked")
	public List<Integer> getUserPersonalizationStories(UserPersonalizationQuery query) throws DBAnalyticsException {

		List<Integer> result = new ArrayList<>(query.getCount());
		List<Integer> readStories = new ArrayList<>();

		long startTime = System.currentTimeMillis();
		int interestBasedCountV1 = 0;
		int interestBasedCountV2 = 0;

		String queryTimeTo = DateUtil.getCurrentDateTime();
		// String queryTimeToDate = queryTimeTo.split("T")[0];
		// String previousDay = DateUtil.getPreviousDate().replaceAll("_", "-");

		List<Integer> oldReadStories = new ArrayList<>();
		try {
			List<String> hostList = new ArrayList<>();
			if (StringUtils.isNotBlank(query.getHosts())) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			GetResponse response = null;

			// Get history for specified user
			if (StringUtils.isNotBlank(query.getSession_id())) {
				response = client.prepareGet().setIndex(Indexes.USER_PERSONALIZATION_STATS).setId(query.getSession_id())
						.execute().actionGet();
				// readStories =
				// getStoryIdHistoryOfUser(query.getSession_id(),
				// 15);
				if (response.isExists() && response.getSource().get(Constants.STORIES) != null) {
					oldReadStories = (List<Integer>) response.getSource().get(Constants.STORIES);
				}
			}

			BoolQueryBuilder boolQueryV2 = new BoolQueryBuilder();

			// Condition-1: If there-1 is no summary of user then return stories
			// with default logic
			//
			if (query.getCount() > 50) {
				// Default Most read logic
				result = fillDefaultStories(result, query);
			} else if (query.getCount() > 30 && !query.getHosts().equals("20,5,6,7")) {
				result = fillDefaultStories(result, query);
			} else if (StringUtils.isBlank(query.getSession_id())) {
				// Default Most read logic
				result = fillDefaultStories(result, query);
			} else if (response == null) {
				// Default Most read logic
				result = fillDefaultStories(result, query);
			} else if (!response.isExists() || !response.getSource().containsKey(Constants.WEIGHTAGE_STATS_FIELD)) {
				// Default Most read logic
				result = fillDefaultStories(result, query);
			} else {
				// List<CIDWeightage> weightagesList = new
				// ArrayList<CIDWeightage>();
				// WeightageQueryResult recordList = gson.fromJson(
				// (String)
				// response.getSource().get(Constants.WEIGHTAGE_STATS_FIELD),
				// WeightageQueryResult.class);
				// weightagesList = recordList.getWeightages();
				List<Integer> catIds = (List<Integer>) response.getSource().get(Constants.CAT_ID_FIELD);

				Integer countTotalStoryBrowsed = 0;
				if (response.getSource().get(Constants.STORY_COUNT) != null) {
					countTotalStoryBrowsed = (Integer) response.getSource().get(Constants.STORY_COUNT);
				}

				// START: (Condition-2) Logic to give default RHS recommendation
				// if Number of stories<5 AND categories count <2
				if (catIds.isEmpty()) {
					result = fillDefaultStories(result, query);
				} else if (countTotalStoryBrowsed < query.getStoryCountQualifier() && catIds.size() < 2) {

					// END: Logic to give default RHS recommendation if Number
					// of stories<5 and categories count <2
				} else {

					fillViralStories(result, query, readStories);
					interestBasedCountV1 = result.size();
					// System.out.println(weightagesList);
					int personalizationCount = query.getCount() / 3;
					String v1QueryTimeFrom = DateUtil.addHoursToCurrentTime(-2);
					// BoolQueryBuilder boolQueryWithBoost = new
					// BoolQueryBuilder();

					// List<Integer> pcatIds = new ArrayList<Integer>();

					boolQueryV2.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
							.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds))
							.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, readStories))
							.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(v1QueryTimeFrom)
									.to(queryTimeTo))
							.mustNot(QueryBuilders.termsQuery(Constants.UID, excludeEditorIds))
							/*
							 * Change to keep too old stories from coming up.
							 */
							.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME)
									.gte(DateUtil.addHoursToCurrentTime(-72)))
							.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS));
					// .must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(previousDay)
					// .to(queryTimeToDate));

					// Execute query of logic version v2
					// Fetch stories from story_detail table based on weightage
					SearchResponse srFinalV2 = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
							.setQuery(boolQueryV2).addAggregation(AggregationBuilders.terms("StoryIds")
									.field(Constants.STORY_ID_FIELD).size(personalizationCount))
							.setSize(0).execute().actionGet();
					// }
					// System.out.println(srFinalV2);k
					Terms cidsResultV2 = srFinalV2.getAggregations().get("StoryIds");
					// Create a list of size given in input query
					for (Terms.Bucket entry : cidsResultV2.getBuckets()) {
						result.add(Integer.valueOf(entry.getKeyAsString()));
					}
					interestBasedCountV2 = result.size() - interestBasedCountV1;

					// Condition-4: Fill the stories with default logic
					// if (query.getHosts().equals("15") ||
					// query.getHosts().equals("20,5,6,7")) {
					result.removeAll(oldReadStories);
					// }
					if (result.size() < query.getCount()) {
						// LOGIC-3
						result = fillOtherStories(result, query, readStories);
					}

				}
			}
		} catch (Exception e) {
			log.error("Error occured while retrieving Recommended stories for user [" + query.getSession_id() + "]", e);
			return result;
		}

		// Remove stories user have read already
		result.removeAll(readStories);
		if (query.getHosts().equals("15")) {
			result.removeAll(oldReadStories);
		}
		Set<Integer> primesWithoutDuplicates = new LinkedHashSet<>(result);
		// now let's clear the ArrayList so that we can copy all elements from
		// LinkedHashSet
		result.clear();

		// copying elements but without any duplicates
		result.addAll(primesWithoutDuplicates);

		log.info("Recommended stories for user [" + query.getSession_id() + "], Result Size: " + result.size()
		+ ", Interest Based stories: V1=" + interestBasedCountV1 + ", V2=" + interestBasedCountV2
		+ ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: "
		+ result);
		return result;
	}

	public List<Integer> getAppRecommendationArticles(UserPersonalizationQuery query) throws DBAnalyticsException {
		List<Integer> result = new ArrayList<>(query.getCount());
		List<Integer> readStories = new ArrayList<>();

		long startTime = System.currentTimeMillis();
		int interestBasedCountV1 = 0;
		int interestBasedCountV2 = 0;

		String queryTimeTo = DateUtil.getCurrentDateTime();

		// List<Integer> oldReadStories = new ArrayList<Integer>();
		try {
			List<String> hostList = new ArrayList<>();

			if (StringUtils.isBlank(query.getHosts())) {
				log.warn("Host not found for session id " + query.getSession_id() + " and channel id "
						+ query.getChannel());
				return result;
			}
			hostList = Arrays.asList(query.getHosts().split(","));

			if (StringUtils.isBlank(query.getSession_id())) {
				// Default Most read logic
				result = fillDefaultStories(result, query);
			} else {
				SearchResponse sr = client.prepareSearch(Indexes.APP_USER_PROFILE)
						.setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(QueryBuilders.matchQuery(Constants.DEVICEID, query.getSession_id())).setSize(1)
						.execute().actionGet();
				if (sr.getHits().getHits().length == 0) {
					// Default Most read logic
					result = fillDefaultStories(result, query);

				} else {
					SearchHit searchHit = sr.getHits().getHits()[0];
					@SuppressWarnings("unchecked")
					List<Integer> catIds = (List<Integer>) searchHit.getSource().get(Constants.CAT_ID_FIELD);

					Integer countTotalStoryBrowsed = 0;
					if (searchHit.getSource().get(Constants.STORY_COUNT) != null) {
						countTotalStoryBrowsed = (Integer) searchHit.getSource().get(Constants.STORY_COUNT);
					}
					// if (searchHit.getSource().get(Constants.STORIES) != null)
					// {
					// oldReadStories = (List<Integer>)
					// searchHit.getSource().get(Constants.STORIES);
					// }
					if (catIds == null || catIds.size() == 0) {
						result = fillDefaultStories(result, query);
					} else if (countTotalStoryBrowsed < query.getStoryCountQualifier() && catIds.size() < 2) {
						result = fillDefaultStories(result, query);
						// END: Logic to give default RHS recommendation if
						// Number
						// of stories<5 and categories count <2
					} else {
						BoolQueryBuilder boolQueryV2 = new BoolQueryBuilder();
						fillViralStories(result, query, readStories);
						interestBasedCountV1 = result.size();
						int personalizationCount = query.getCount() - interestBasedCountV1;
						String v1QueryTimeFrom = DateUtil.addHoursToCurrentTime(-2);
						boolQueryV2.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
						.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds))
						// .mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD,
						// oldReadStories))
						.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(v1QueryTimeFrom)
								.to(queryTimeTo));

						// Execute query of logic version v2
						SearchResponse srFinalV2 = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
								.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
								.setQuery(boolQueryV2).addAggregation(AggregationBuilders.terms("StoryIds")
										.field(Constants.STORY_ID_FIELD).size(personalizationCount))
								.setSize(0).execute().actionGet();
						Terms cidsResultV2 = srFinalV2.getAggregations().get("StoryIds");
						// Create a list of size given in input query
						for (Terms.Bucket entry : cidsResultV2.getBuckets()) {
							result.add(Integer.valueOf(entry.getKeyAsString()));
						}
						interestBasedCountV2 = result.size() - interestBasedCountV1;
						// result.removeAll(oldReadStories);
						if (result.size() < query.getCount()) {
							// LOGIC-3
							result = fillOtherStories(result, query, readStories);
						}
					}
				}
			}

		} catch (Exception e) {
			log.error("Error occured while retrieving APP Recommended stories for user [" + query.getSession_id() + "]",
					e);
			return result;
		}

		// Remove stories user have read already
		// result.removeAll(readStories);
		Set<Integer> primesWithoutDuplicates = new LinkedHashSet<>(result);
		// now let's clear the ArrayList so that we can copy all elements from
		// LinkedHashSet
		result.clear();

		// copying elements but without any duplicates
		result.addAll(primesWithoutDuplicates);

		log.info("APP Recommended stories for user [" + query.getSession_id() + "], Result Size: " + result.size()
		+ ", Interest Based stories: V1=" + interestBasedCountV1 + ", V2=" + interestBasedCountV2
		+ ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: "
		+ result);
		return result;
	}

	private List<Integer> fillDefaultStories(List<Integer> result, UserPersonalizationQuery query) {
		query.setLogicVersion("v1");
		if (cacheRHS.get(query.getHosts()) == null || cacheRHS.get(query.getHosts()).isEmpty()) {
			fillCacheStories(query);
		}
		result.addAll(cacheRHS.get(query.getHosts()));
		return result;
	}

	private List<Integer> fillViralStories(List<Integer> result, UserPersonalizationQuery query,
			List<Integer> readStories) {
		if (cacheRHS.get(query.getHosts()) == null || cacheRHS.get(query.getHosts()).isEmpty()) {
			fillCacheStories(query);
		}

		for (Integer storyId : cacheRHS.get(query.getHosts())) {
			if (!readStories.contains(storyId)) {
				result.add(storyId);
			}
			if (result.size() == 2)
				break;
		}
		return result;
	}

	private List<Integer> fillOtherStories(List<Integer> result, UserPersonalizationQuery query,
			List<Integer> readStories) {
		if (cacheRHS.get(query.getHosts()) == null || cacheRHS.get(query.getHosts()).isEmpty()) {
			fillCacheStories(query);
		}

		for (Integer storyId : cacheRHS.get(query.getHosts())) {
			if (!readStories.contains(storyId) && !result.contains(storyId)) {
				result.add(storyId);
			}

			if (result.size() == query.getCount())
				break;
		}
		return result;
	}

	private synchronized void fillCacheStories(UserPersonalizationQuery query) {
		if (cacheRHS.get(query.getHosts()) == null || cacheRHS.get(query.getHosts()).isEmpty()) {
			TimerArrayList<Integer> timerArrayList = new TimerArrayList<>();
			log.info("Filling Stories with new values. Host:" + query.getHosts());
			timerArrayList.addAll(fillWAPStories(query));
			cacheRHS.put(query.getHosts(), timerArrayList);
		}
	}

	private List<Integer> fillWAPStories(UserPersonalizationQuery query) {

		// TreeMap<Long, Integer> clicksStoryMap=new TreeMap<Long, Integer>();
		TreeMultimap<Long, Integer> clicksStoryMap = TreeMultimap.create(Ordering.natural().reverse(),
				Ordering.natural());
		List<Integer> tempResult = new ArrayList<>();
		int modifiedStoryCount = 4;
		int trackerStoryCount = 9;
		query.setLogicVersion("v1");
		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-10);

		String queryTimeToDate = queryTimeTo.split("T")[0];
		String queryTimeFromDate = queryTimeFrom.split("T")[0];
		String previousDay = DateUtil.getPreviousDate().replaceAll("_", "-");

		List<String> indexes = new ArrayList<>();
		indexes.add("realtime_" + DateUtil.getCurrentDate());
		if (!queryTimeToDate.equals(queryTimeFromDate)) {
			indexes.add("realtime_" + queryTimeFromDate.replaceAll("-", "_"));
		}

		String[] indexArray = indexes.toArray(new String[indexes.size()]);

		List<String> hostList = Arrays.asList(query.getHosts().split(","));

		// If there is list of hosts
		TermsQueryBuilder hostQueryBuilder = QueryBuilders.termsQuery(Constants.HOST, hostList);

		BoolQueryBuilder boolQueryV1 = new BoolQueryBuilder();
		List<String> trackers = Arrays.asList(facebookTrackerArray);

		if (query.getHosts().equals("15")) {
			// tempResult.addAll(getNewLogic0517Stories(query));
			boolQueryV1.should(QueryBuilders.matchQuery(Constants.TRACKER, "news-ucb"));
			// return tempResult;
		} else {
			for (String tracker : trackers) {
				boolQueryV1.should(QueryBuilders.matchQuery(Constants.TRACKER, tracker));
			}
		}

		BoolQueryBuilder boolQueryFacebookTracker = new BoolQueryBuilder();
		boolQueryFacebookTracker.must(boolQueryV1).must(hostQueryBuilder)
				.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo))
				.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(previousDay).to(queryTimeToDate))
				.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS))
				.mustNot(QueryBuilders.termsQuery(Constants.UID, excludeEditorIds));

		SearchResponse srFinalV1 = client.prepareSearch(indexArray)
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryFacebookTracker)
				.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
						// .order(Order.aggregation("StoryPVS", false))
						.size(modifiedStoryCount)
						// .subAggregation(AggregationBuilders.sum("StoryPVS").field(Constants.PVS))
						).setSize(0).execute().actionGet();
		Terms cidsResultV1 = srFinalV1.getAggregations().get("StoryIds");
		// Create a list of size given in input query
		for (Terms.Bucket entry : cidsResultV1.getBuckets()) {
			clicksStoryMap.put(entry.getDocCount(), Integer.valueOf(entry.getKeyAsString()));
			tempResult.add(Integer.valueOf(entry.getKeyAsString()));
		}

		String tracker = "news-hf";

		BoolQueryBuilder boolQueryTracker = new BoolQueryBuilder();
		boolQueryTracker.must(QueryBuilders.matchQuery(Constants.TRACKER, tracker)).must(hostQueryBuilder)
				.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, clicksStoryMap.values()))
				.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo))
				.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(previousDay).to(queryTimeToDate))
				.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS))
				.mustNot(QueryBuilders.termsQuery(Constants.UID, excludeEditorIds));

		SearchResponse trackerSearchResponse = client.prepareSearch(indexArray)
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryTracker)
				.addAggregation(
						AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD).size(trackerStoryCount))
				.setSize(0).execute().actionGet();
		Terms cidsResult = trackerSearchResponse.getAggregations().get("StoryIds");

		for (Terms.Bucket entry : cidsResult.getBuckets()) {
			clicksStoryMap.put(entry.getDocCount(), Integer.valueOf(entry.getKeyAsString()));
			tempResult.add(Integer.valueOf(entry.getKeyAsString()));
		}

		// LoGIC-3
		if (query.getCount() - clicksStoryMap.size() > 0) {
			BoolQueryBuilder boolQueryMostRead = new BoolQueryBuilder();
			boolQueryMostRead.must(hostQueryBuilder)
					.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, clicksStoryMap.values()))
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo))
					.should(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(previousDay)
							.to(queryTimeToDate))
					.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS))
					.mustNot(QueryBuilders.termsQuery(Constants.UID, excludeEditorIds));

			// boolQueryMostRead.should(QueryBuilders.boolQuery().must(hostQueryBuilder)
			// .mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD,
			// clicksStoryMap.values()))
			// .must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo)))
			// .must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(previousDay).to(queryTimeToDate));

			// String includes[]={"story_modifiedtime","story_pubtime", "title",
			// "url"};
			SearchResponse mostReadSearchResponse = client.prepareSearch(indexArray)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryMostRead)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
							.size(query.getCount() - clicksStoryMap.size())
							// .subAggregation(AggregationBuilders.topHits("source").setFetchSource(includes,new
							// String[]{}).setSize(1))
							).setSize(0).execute().actionGet();
			// log.info(mostReadSearchResponse);
			Terms mostReadcidsResult = mostReadSearchResponse.getAggregations().get("StoryIds");

			for (Terms.Bucket entry : mostReadcidsResult.getBuckets()) {
				clicksStoryMap.put(entry.getDocCount(), Integer.valueOf(entry.getKeyAsString()));
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}
		}

		if (!query.getHosts().equals("15")) {
			tempResult.clear();
			for (Long entry : clicksStoryMap.keySet()) {
				for (Integer storyid : clicksStoryMap.get(entry)) {
					tempResult.add(storyid);
				}
			}
		}
		// Collections.reverse(tempResult);
		return tempResult;
	}

	private Set<Integer> getNewLogic0517Stories(UserPersonalizationQuery query) {
		Set<Integer> tempResult = new HashSet<Integer>();

		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);

		String queryTimeToDate = queryTimeTo.split("T")[0];
		String queryTimeFromDate = queryTimeFrom.split("T")[0];

		List<String> indexes = new ArrayList<String>();
		indexes.add("realtime_" + DateUtil.getCurrentDate());
		if (!queryTimeToDate.equals(queryTimeFromDate)) {
			indexes.add("realtime_" + queryTimeFromDate.replaceAll("-", "_"));
		}

		String[] indexArray = indexes.toArray(new String[indexes.size()]);

		List<String> hostList = Arrays.asList(query.getHosts().split(","));

		// If there is list of hosts
		TermsQueryBuilder hostQueryBuilder = QueryBuilders.termsQuery(Constants.HOST, hostList);

		BoolQueryBuilder boolQueryFacebookTracker = new BoolQueryBuilder();
		boolQueryFacebookTracker.must(hostQueryBuilder)
		.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

		SearchResponse srFinalV1 = client.prepareSearch(indexArray)
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryFacebookTracker)
				.addAggregation(AggregationBuilders.terms("TOP_PEOPLE").field(Constants.PEOPLE).size(5).subAggregation(
						AggregationBuilders.terms("TOP_STORY_ID").field(Constants.STORY_ID_FIELD).size(2)))
				.addAggregation(AggregationBuilders.terms("TOP_EVENT").field(Constants.EVENT).size(5).subAggregation(
						AggregationBuilders.terms("TOP_STORY_ID").field(Constants.STORY_ID_FIELD).size(2)))
				.addAggregation(
						AggregationBuilders.terms("TOP_ORGANIZATION")
						.field(Constants.ORGANIZATION).size(5).subAggregation(AggregationBuilders
								.terms("TOP_STORY_ID").field(Constants.STORY_ID_FIELD).size(2)))
				.setSize(0).execute().actionGet();

		Terms peopleTerms = srFinalV1.getAggregations().get("TOP_PEOPLE");
		for (Terms.Bucket peopleTermBucket : peopleTerms.getBuckets()) {
			if (StringUtils.isNotBlank(peopleTermBucket.getKeyAsString())) {
				Terms storyIdTerms = peopleTermBucket.getAggregations().get("TOP_STORY_ID");
				for (Terms.Bucket storyIdBucket : storyIdTerms.getBuckets()) {
					tempResult.add(Integer.valueOf(storyIdBucket.getKeyAsString()));
				}
			}
		}

		int peopleStoryCount = tempResult.size();
		Terms eventTerms = srFinalV1.getAggregations().get("TOP_EVENT");
		for (Terms.Bucket peopleTermBucket : eventTerms.getBuckets()) {
			if (StringUtils.isNotBlank(peopleTermBucket.getKeyAsString())) {
				Terms storyIdTerms = peopleTermBucket.getAggregations().get("TOP_STORY_ID");
				for (Terms.Bucket storyIdBucket : storyIdTerms.getBuckets()) {
					tempResult.add(Integer.valueOf(storyIdBucket.getKeyAsString()));
				}
			}
		}

		int eventStoryCount = tempResult.size() - peopleStoryCount;

		Terms organizationTerms = srFinalV1.getAggregations().get("TOP_ORGANIZATION");
		for (Terms.Bucket peopleTermBucket : organizationTerms.getBuckets()) {
			if (StringUtils.isNotBlank(peopleTermBucket.getKeyAsString())) {
				Terms storyIdTerms = peopleTermBucket.getAggregations().get("TOP_STORY_ID");
				for (Terms.Bucket storyIdBucket : storyIdTerms.getBuckets()) {
					tempResult.add(Integer.valueOf((String) storyIdBucket.getKeyAsString()));
				}
			}
		}

		int organizationStoryCount = tempResult.size() - peopleStoryCount - eventStoryCount;

		// LoGIC-3
		if (query.getCount() - tempResult.size() > 0) {
			BoolQueryBuilder boolQueryMostRead = new BoolQueryBuilder();
			boolQueryMostRead.must(hostQueryBuilder)
			.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, tempResult))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

			SearchResponse mostReadSearchResponse = client.prepareSearch(indexArray)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
					.setQuery(boolQueryMostRead).addAggregation(AggregationBuilders.terms("StoryIds")
							.field(Constants.STORY_ID_FIELD).size(query.getCount() - tempResult.size()))
					.setSize(0).execute().actionGet();
			Terms mostReadcidsResult = mostReadSearchResponse.getAggregations().get("StoryIds");

			for (Terms.Bucket entry : mostReadcidsResult.getBuckets()) {
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}
		}

		// MultiGetRequest multiGetRequest = new MultiGetRequest();
		// for (Integer story : tempResult) {
		// multiGetRequest.add(Indexes.STORY_UNIQUE_DETAIL,
		// MappingTypes.MAPPING_REALTIME, String.valueOf(story));
		// }
		//
		// MultiGetResponse multiGetResponse =
		// client.multiGet(multiGetRequest).actionGet();
		// for (MultiGetItemResponse multiGetItemResponse :
		// multiGetResponse.getResponses()) {
		// Map<String, Object> storyObject =
		// multiGetItemResponse.getResponse().getSourceAsMap();
		// System.out.println("-----------------------------");
		// System.out.println(storyObject.get(Constants.STORY_ID_FIELD));
		// System.out.println(storyObject.get(Constants.TITLE));
		// System.out.println(storyObject.get(Constants.URL));
		// }
		log.info("NLP tags based new logic. Result Size: " + tempResult.size() + "; People Stories: " + peopleStoryCount
				+ "; Event stories: " + eventStoryCount + "; Organization stories: " + organizationStoryCount);
		return tempResult;
	}

	public List<UserPersonalizationStory> getStoryDetails(List<Integer> stories) {
		List<UserPersonalizationStory> result = new ArrayList<UserPersonalizationStory>();
		Map<String, UserPersonalizationStory> resultMap = new LinkedHashMap<String, UserPersonalizationStory>();

		try {
			// Multi Get request based on story ids
			MultiGetRequest multiGetRequest = new MultiGetRequest();
			for (Integer story : stories) {
				multiGetRequest.add(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME, String.valueOf(story));
			}
			if (stories.size() > 0) {

				MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();
				for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
					Map<String, Object> storyObject = multiGetItemResponse.getResponse().getSourceAsMap();

					UserPersonalizationStory story = new UserPersonalizationStory();

					if (storyObject.get(Constants.STORY_PUBLISH_TIME) != null) {
						story.setStory_pubtime(storyObject.get(Constants.STORY_PUBLISH_TIME).toString());
					}

					story.setStoryid(storyObject.get(Constants.STORY_ID_FIELD));
					// Checks to skip stories
					if (StringUtils.isBlank((String) storyObject.get(Constants.TITLE))) {
						continue;
					}
					if (StringUtils.isBlank((String) storyObject.get(Constants.URL))) {
						continue;
					}
					if (StringUtils.isBlank((String) storyObject.get(Constants.IMAGE))) {
						continue;
					}

					story.setTitle((String) storyObject.get(Constants.TITLE));
					story.setUrl((String) storyObject.get(Constants.URL));
					if (storyObject.get(Constants.PVS) != null) {
						story.setPvs(Long.valueOf(storyObject.get(Constants.PVS).toString()));
					}

					if (storyObject.containsKey(Constants.IMAGE)) {
						story.setImage((String) storyObject.get(Constants.IMAGE));
					}

					if (storyObject.containsKey(Constants.CAT_ID_FIELD)) {
						story.setCat_id(storyObject.get(Constants.CAT_ID_FIELD));
					}
					if (storyObject.containsKey(Constants.PARENT_CAT_ID_FIELD)) {
						story.setPcat_id(storyObject.get(Constants.PARENT_CAT_ID_FIELD));
					}
					if (storyObject.containsKey(Constants.STORY_ATTRIBUTE)) {
						story.setStory_attribute(storyObject.get(Constants.STORY_ATTRIBUTE));
					}
					if (storyObject.get(Constants.DIMENSION) != null) {
						story.setDimension(storyObject.get(Constants.DIMENSION));

						String dimension = story.getDimension().toString();
						Map<Integer, Integer> dimension2 = GenericUtils.getImageResizeDimensions(dimension, "301x147");
						for (Integer key : dimension2.keySet()) {
							story.setWidth(key);
							story.setHeight(dimension2.get(key));
						}
					}
					// Add keyword
					// if (query.getLogicVersion().equalsIgnoreCase("v2")) {
					// story.setCategoryName(categories.get(story.getCat_id()));
					// if (Integer.valueOf(story.getPcat_id().toString()) ==
					// 8342) {
					// story.setCategoryName("MON-PERS");
					// } else if (Integer.valueOf((String)
					// story.getPcat_id().toString()) == 5707) {
					// story.setCategoryName("GADGETS");
					// }
					// }
					// Logic of duplicate stories as per Manish's feedback
					if (!resultMap.containsKey(story.getImage())) {
						resultMap.put(story.getImage(), story);
					} else {
						// resultMap.remove(story.getImage());
					}

					// result.add(story);

					// log.info(storyObject);
				}

			}
		} catch (Exception e) {
			log.error("Error in getting story details.", e);
		}
		result.addAll(resultMap.values());
		return result;
	}

	private Map<String, Object> getMap(Object o) throws IllegalArgumentException, IllegalAccessException {
		Map<String, Object> result = new HashMap<String, Object>();
		Field[] declaredFields = o.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			result.put(field.getName(), field.get(o));
		}
		return result;
	}

	private String getIndex(String strDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = sdf.parse(strDate);
		cal.setTime(date);
		int yyyy = cal.get(Calendar.YEAR);
		int mm = cal.get(Calendar.MONTH) + 1;
		int dd = cal.get(Calendar.DAY_OF_MONTH);
		return "realtime_" + String.valueOf(yyyy) + "_" + String.valueOf("0" + mm) + "_" + String.valueOf(dd);
	}

	/**
	 * Get list of indexes between two dates.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public List<String> getIndexes(String startDate, String endDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date dateFrom = sdf.parse(startDate);
		Date dateTo = sdf.parse(endDate);
		if (dateFrom.compareTo(dateTo) > 0) {
			throw new DBAnalyticsException("Failure: End date can not be before start date.");
		}
		Date currDate = (Date) dateFrom.clone();
		List<String> list = new ArrayList<String>();
		while (currDate.compareTo(dateTo) <= 0) {
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
			list.add("realtime_" + dateFormat.format(currDate));
			log.info(list);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(currDate);
			calNext.add(Calendar.DATE, 1);
			currDate = calNext.getTime();
		}
		return list; // "realtime_"+String.valueOf(yyyy) + "_" +
		// String.valueOf("0"+mm) + "_" + String.valueOf(dd);
	}

	public Map<String, Long> getSessionBucketForUsers(GenericQuery genericQuery) {
		Map<String, Long> result = new LinkedHashMap<String, Long>();
		// long startTime = System.currentTimeMillis();
		// String queryDate = DateUtil.getCurrentDate();

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		String hosts = genericQuery.getHosts();
		// Add multiple hosts match conditions to query
		if (StringUtils.isNotBlank(hosts)) {
			String[] hostArray = hosts.split(",");
			BoolQueryBuilder hostQueryBuilder = new BoolQueryBuilder();
			if (hostArray.length > 0) {
				for (String host : hostArray) {
					hostQueryBuilder.should(QueryBuilders.matchQuery(Constants.HOST, host));
				}
			}
			boolQuery.must(hostQueryBuilder);
		}

		// Add start date and end date match conditions to query
		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(Constants.SESSION_TIMESTAMP);
		if (StringUtils.isNotBlank(genericQuery.getStartDate())) {
			rangeQueryBuilder.from(genericQuery.getStartDate());
		}
		if (StringUtils.isNotBlank(genericQuery.getEndDate())) {
			rangeQueryBuilder.to(genericQuery.getEndDate());
		}
		if (StringUtils.isNotBlank(genericQuery.getStartDate()) || StringUtils.isNotBlank(genericQuery.getEndDate())) {
			boolQuery.must(rangeQueryBuilder);
		}

		// Prepare search request and execute it
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.USER_PROFILE_DAYWISE)
				.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000)).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.terms("GROUP_BY_SESSION_COUNT").field(Constants.SESSION_COUNT)
						.order(Terms.Order.term(true)));

		SearchResponse sr = searchRequestBuilder.execute().actionGet();

		long totalUsers = sr.getHits().getTotalHits();

		Terms groupBySessionCount = sr.getAggregations().get("GROUP_BY_SESSION_COUNT");
		long usersWithLessThan3Sessions = 0;
		for (Terms.Bucket entry : groupBySessionCount.getBuckets()) {
			if (entry.getKeyAsString().equals("1") || entry.getKeyAsString().equals("2")
					|| entry.getKeyAsString().equals("3")) {
				result.put(entry.getKeyAsString(), entry.getDocCount());
				usersWithLessThan3Sessions = usersWithLessThan3Sessions + entry.getDocCount();
			} else if (Integer.valueOf(entry.getKeyAsString()) > 3) {
				break;
			}
		}
		result.put(">3", totalUsers - usersWithLessThan3Sessions);
		long endTime = System.currentTimeMillis();

		// System.out.println(
		// "INFO: Total time to find Session Bucket for users (Seconds): " +
		// ((endTime - startTime) / 1000.0));
		return result;
	}

	/**
	 * Gives time of last visit of a user in last 30 days
	 * 
	 * @param sessionId
	 * @return
	 */
	public String getLastVisitOfUser(String sessionId) {
		long startTime = System.currentTimeMillis();
		String queryDate = DateUtil.getCurrentDate();
		String result = null;
		outerLoop: for (int i = 0; i < 30; i++) {
			try {
				SearchResponse sr = client.prepareSearch("realtime_" + queryDate)
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
						.setQuery(QueryBuilders.matchQuery("session_id", sessionId))
						.addAggregation(AggregationBuilders.dateHistogram("visits_over_time").field("datetime")
								.interval(1800000)
								.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
						.setSize(0).execute().actionGet();

				Histogram dateHistogram = sr.getAggregations().get("visits_over_time");
				if (dateHistogram.getBuckets().size() > 0) {
					Histogram.Bucket b = dateHistogram.getBuckets().get(0);
					log.info("Last visit got till " + queryDate);

					result = b.getKeyAsString();
					break outerLoop;
				}
				queryDate = DateUtil.getPreviousDate(queryDate);
			} catch (IndexNotFoundException ime) {
				log.error(ime);
				queryDate = DateUtil.getPreviousDate(queryDate);
			} catch (Exception e) {
				log.error(e);
				queryDate = DateUtil.getPreviousDate(queryDate);
			}
		}
		long endTime = System.currentTimeMillis();
		log.info("INFO: Total time to find last visit of a user (Seconds): " + ((endTime - startTime) / 1000.0));
		if (result == null) {
			throw new DBAnalyticsException("No visit found for user [" + sessionId + "] in last 30 days.");
		}
		return result;
	}

	public void dailyUserVisitsCounter(String userVisitsIndex) {
		// ElasticSearchIndexService elasticSearchIndexService = new
		// ElasticSearchIndexService();
		String date = DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		if (date == null || "".equalsIgnoreCase(date.trim())) {
			throw new DBAnalyticsException("Please provide a date");
		}
		// Shrey
		date = date.replace("_", "-");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date indexDate = (Date) sdf.parse(date);
			String strIndexDate = sdf.format(new Date(indexDate.getTime()));
			Set<String> session_id_set = new HashSet<String>();
			int startFrom = 0;
			int numRec = 1000;
			long total = 0;
			String realtime_index = "realtime_" + strIndexDate.replace("-", "_");
			// log.info("searching in index of date " + strIndexDate
			// + " strIndexDate: " + realtime_index);
			log.info("searching  in index of date " + strIndexDate + " strIndexDate: " + realtime_index);
			startFrom = 0;
			do {

				SearchResponse sr = client.prepareSearch(realtime_index)
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(5000))
						.setFrom(startFrom).setSize(numRec).execute().actionGet();
				total = sr.getHits().getTotalHits();// getHits().length;
				if (sr.getHits().getHits().length > 0) {
					log.info("index: " + sr.getHits().getHits()[0].getIndex() + " total: " + total + ", Fetched Hits: "
							+ sr.getHits().getHits().length + " , Total fetched : " + startFrom);
					// log.info("index: " +
					// sr.getHits().getHits()[0].getIndex() + " total: " + total
					// +", Fetched Hits: "+sr.getHits().getHits().length +
					// " , Total fetched : "+startFrom);
					startFrom += numRec;
					for (SearchHit hit : sr.getHits().getHits()) {
						try {
							String session_id = hit.getSourceAsMap().get("session_id").toString();
							session_id_set.add(session_id);
						} catch (Exception e) {
							log.error("Error getting session id");
							continue;
						}
					}
				}
				// log.info(session_id_set.size());
				log.info(session_id_set.size());

			} while (startFrom <= total);

			// new date format for story_modifiedtime
			// Map<String,Object> user_unique_visit = new
			// HashMap<String,Object>();
			List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
			for (String session_id : session_id_set) {
				try {
					Map<String, Object> record = new HashMap<String, Object>();
					record.put(Constants.ROWID, session_id);
					SearchResponse sr = client.prepareSearch(realtime_index).setTypes("realtime")
							.setTimeout(new TimeValue(2000))
							.setQuery(QueryBuilders.matchQuery("session_id", session_id))
							.addAggregation(AggregationBuilders.dateHistogram("visits_over_time")
									.field("story_modifiedtime").interval(1800000))
							.setSize(0).execute().actionGet();
					// AggregationBuilders.terms("visits_over_time").field("visits_over_time")

					InternalDateHistogram dateHistogram = sr.getAggregations().get("visits_over_time");
					int visits = dateHistogram.getBuckets().size();
					// log.info(visits);
					record.put(date, visits);
					listMessages.add(record);
					if (listMessages.size() == 1000) {
						elasticSearchIndexService.indexOrUpdate(userVisitsIndex, "realtime", listMessages);
						listMessages.clear();
					}
				} catch (Exception e) {
					log.error("Error getting unique visits for session id: " + session_id + " exception " + e);
					// log.info("Error getting unique visits for
					// session id: "
					// + session_id + " exception " + e);
					continue;
				}

			}

			if (listMessages.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(userVisitsIndex, "realtime", listMessages);
				listMessages.clear();
			}

		} catch (ParseException e) {
			log.error(e);
			throw new DBAnalyticsException("Invalid date format." + e.getMessage());
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	/**
	 * { "query": { "filtered" : { "filter" : { "and" : [ { "missing" : {
	 * "field" : "2015-08-18" } }, { "missing" : { "field" : "2015-08-19" } } ]
	 * } } } }
	 * 
	 * @param noOfDays
	 * @return
	 */
	@Deprecated
	public List<String> getAbsentVisitors(int noOfDays) {
		/*
		 * String prevDate =
		 * DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		 * 
		 * // BoolQueryBuilder boolQuery = new BoolQueryBuilder(); // for(int
		 * i=0; i<noOfDays;i++){ //
		 * boolQuery.must(QueryBuilders.matchQuery(prevDate.replace("_","-"), //
		 * 0)); // prevDate = DateUtil.getPreviousDate(prevDate); // }
		 * 
		 * List<String> sessionIds = new ArrayList<String>(); // for
		 * (Map.Entry<String, String> entry : fields.entrySet()){ //
		 * boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), //
		 * entry.getValue())); // } AndFilterBuilder myFilters =
		 * FilterBuilders.andFilter(); for (int i = 0; i < noOfDays; i++) {
		 * myFilters.add(FilterBuilders.missingFilter(prevDate.replace("_",
		 * "-"))); prevDate = DateUtil.getPreviousDate(prevDate); }
		 * 
		 * SearchResponse sr = client.prepareSearch("userdailyvisits")
		 * .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
		 * myFilters)).setSize(20000) .execute().actionGet();
		 * 
		 * // if (sr.getHits().getHits() != null &&
		 * sr.getHits().getHits().length > 0) { for (SearchHit hit :
		 * sr.getHits().getHits()) { String session_id = hit.getId();//
		 * getSource().get("session_id").toString(); log.info(session_id);
		 * sessionIds.add(session_id); } } log.info(sessionIds.size()); return
		 * sessionIds;
		 */
		return null;
	}

	/**
	 * { "script_fields": { "my_field": { "script" :
	 * "((doc['2015-08-19'].value+doc['2015-08-16'].value+doc['2015-08-20'].value+doc['2015-08-21'].value+doc['2015-08-22'].value+doc['2015-08-23'].value)>1)==true"
	 * } },"size":1000 }
	 */
	@Deprecated
	public List<String> getUsersVisitIdConditionalAggs(UserFrequencyQuery ufq) {
		/*
		 * int noOfDays = ufq.getDayCount(); String operator =
		 * ufq.getOperator(); int aggsCount = ufq.getAggsCount();
		 * 
		 * if (noOfDays < 1 || "".equalsIgnoreCase(operator.trim()) || aggsCount
		 * < 1) { throw new
		 * DBAnalyticsException("Invalid Request : No of days: " + noOfDays +
		 * " Operator: " + operator + " Aggregation: " + aggsCount); } String
		 * prevDate =
		 * DateUtil.getPreviousDate(DateUtil.getPreviousDate(DateUtil.
		 * getCurrentDate())); List<String> sessionIds = new
		 * ArrayList<String>(); StringBuilder scriptSB = new StringBuilder();
		 * StringBuilder columnSB = new StringBuilder(); scriptSB.append("((");
		 * for (int i = 0; i < noOfDays; i++) { columnSB.append("doc['" +
		 * prevDate.replace("_", "-") + "'].value+"); prevDate =
		 * DateUtil.getPreviousDate(prevDate); }
		 * scriptSB.append(replaceLastPlus(columnSB.toString(), '+'));
		 * scriptSB.append(")"); scriptSB.append(operator);
		 * scriptSB.append(aggsCount); scriptSB.append(")");
		 * 
		 * SearchResponse sr =
		 * client.prepareSearch("userdailyvisits").setQuery(QueryBuilders.
		 * matchAllQuery()) .addScriptField("script",
		 * scriptSB.toString()).setSize(20000).execute().actionGet(); // if
		 * (sr.getHits().getHits() != null && sr.getHits().getHits().length > 0)
		 * { for (SearchHit hit : sr.getHits().getHits()) { //
		 * log.info(hit.fields().get("script").getValue().toString()); if
		 * (Boolean.valueOf(hit.fields().get("script").getValue().toString())) {
		 * String session_id = hit.getId();//
		 * getSource().get("session_id").toString(); log.info(session_id);
		 * sessionIds.add(session_id); } } } log.info(sessionIds.size()); return
		 * sessionIds;
		 */
		return null;
	}

	private String replaceLastPlus(String str, char c) {
		if (str.charAt(str.length() - 1) == c) {
			// str = str.replace(str.substring(str.length()-1), "");
			str = str.substring(0, str.lastIndexOf(c)) + "";
			return str;
		} else {
			return str;
		}
	}

	/**
	 * 
	 * @param noOfDays
	 * @return
	 */
	public List<String> getVisitorsFrequency(int noOfDays, int conditionalCount, String operator) {
		String prevDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate());

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (int i = 0; i < noOfDays; i++) {
			boolQuery.must(QueryBuilders.matchQuery(prevDate.replace("_", "-"), 0));
			prevDate = DateUtil.getPreviousDate(prevDate);
		}

		List<String> sessionIds = new ArrayList<String>();
		// for (Map.Entry<String, String> entry : fields.entrySet()){
		// boolQuery.must(QueryBuilders.matchQuery(entry.getKey(),
		// entry.getValue()));
		// }
		//
		SearchResponse sr = client.prepareSearch("userdailyvisits").setQuery(boolQuery).setSize(20000).execute()
				.actionGet();
		if (sr.getHits().getHits() != null && sr.getHits().getHits().length > 0) {
			for (SearchHit hit : sr.getHits().getHits()) {
				String session_id = hit.getSource().get("session_id").toString();
				sessionIds.add(session_id);
			}
		}

		return sessionIds;
	}

	public String getUsersHourlyStatistics(int numDays, String session_id) {
		String prevDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		UserHourlyStats uhs = new UserHourlyStats();
		ArrayList<StatsDate> lstSD = new ArrayList<StatsDate>();
		for (int i = 0; i < numDays; i++) {
			SearchResponse sr = client.prepareSearch("realtime_" + prevDate.replace("-", "_"))
					.setQuery(QueryBuilders.matchQuery("session_id", session_id))
					.addAggregation(
							AggregationBuilders.dateHistogram("number_of_slides").field("datetime").interval(3600000)
							.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
					.setSize(0).execute().actionGet();

			SearchResponse sr1 = client.prepareSearch("realtime_" + prevDate.replace("-", "_"))
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
					.setQuery(QueryBuilders.matchQuery("session_id", session_id))
					.addAggregation(
							AggregationBuilders.dateHistogram("stories_over_time").field("datetime").interval(3600000)
							.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
					.setSize(0).execute().actionGet();

			Histogram uniqueStoryHistogram = sr1.getAggregations().get("stories_over_time");

			Histogram dateHistogram = sr.getAggregations().get("number_of_slides");
			if (dateHistogram.getBuckets().size() > 0 && uniqueStoryHistogram.getBuckets().size() > 0) {
				StatsDate sd = new StatsDate();
				ArrayList<Hours> lstHours = new ArrayList<Hours>();

				for (int j = 0; j < dateHistogram.getBuckets().size(); j++) {
					Hours h = new Hours();
					Histogram.Bucket noOfSlides = dateHistogram.getBuckets().get(j);
					Histogram.Bucket storiesOverTime = null;
					boolean isStoryAlreadyRead = false;
					if (uniqueStoryHistogram.getBuckets().size() > j) {
						storiesOverTime = uniqueStoryHistogram.getBuckets().get(j);
						isStoryAlreadyRead = false;
					} else {
						isStoryAlreadyRead = true;
					}
					log.info("Last visit got till " + prevDate);
					// TODO: Handle below line with new version of ES
					int hour = 0;// noOfSlides.getKeyAsDate().getHourOfDay();
					// h.setHour(hour);
					h.setNumber_of_slides(String.valueOf(noOfSlides.getDocCount()));
					if (!isStoryAlreadyRead) {
						h.setUnique_story_count(String.valueOf(storiesOverTime.getDocCount()));
					} else {
						h.setUnique_story_count(String.valueOf(1));
					}
					lstHours.add(h);
					log.info("hours: " + hour);
					log.info("docCount: " + noOfSlides.getDocCount());
				}
				sd.setHours(lstHours);
				sd.setDate(prevDate);
				lstSD.add(sd);

			}
			prevDate = DateUtil.getPreviousDate(prevDate);
		}
		uhs.setStatsDates(lstSD);
		uhs.setSession_id(session_id);
		Gson gson = new Gson();
		String jsonStr = gson.toJson(uhs);
		return jsonStr;
	}

	public void dailyAppUserTopCatid() {
		String strIndexDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		long startTime = System.currentTimeMillis();
		String date = strIndexDate.replace("_", "-");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			// Start of unique users calculation
			Set<String> device_token_set = new HashSet<String>();
			int startFrom = 0;
			int numRec = 5000;
			long total = 0;
			String realtimeIndex = "realtime_" + strIndexDate;
			log.info("searching  in index of date " + strIndexDate + " strIndexDate: " + realtimeIndex);
			log.info("searching  in index of date " + strIndexDate + " strIndexDate: " + realtimeIndex);
			startFrom = 0;
			do {
				SearchResponse sr = client.prepareSearch(realtimeIndex)
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
						.setQuery(QueryBuilders.matchQuery(Constants.TRACKER, "app")).setTimeout(new TimeValue(5000))
						.setFrom(startFrom).setSize(numRec).execute().actionGet();
				total = sr.getHits().getTotalHits();
				if (sr.getHits().getHits().length > 0) {
					log.info("index: " + sr.getHits().getHits()[0].getIndex() + " total: " + total + ", Fetched Hits: "
							+ sr.getHits().getHits().length + " , Total fetched : " + startFrom);

					startFrom += numRec;
					for (SearchHit hit : sr.getHits().getHits()) {
						try {
							String device_token = hit.getSource().get("device_token").toString();
							if (org.apache.commons.lang.StringUtils.isNotBlank(device_token)) {
								device_token_set.add(device_token);
							}
						} catch (Exception e) {
							log.error("Error getting session id");
							continue;
						}
					}
				}
				log.info("Unique App Users: " + device_token_set.size());

			} while (startFrom <= total);

			// End of unique users calculation

			List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
			// Date of before 7 days
			Date dateNDaysBefore = new Date(((Date) sdf.parse(date)).getTime() - 6 * 24 * 3600 * 1000l);

			String[] indexes = IndexUtils.getIndexes(sdf.format(dateNDaysBefore), date);
			long indexedRecordCount = 0;
			for (String device_token_id : device_token_set) {
				try {
					// TODO: Change mapping type to
					// MAPPING_REALTIME_UNIQUE_USER_STORY and index name to
					// realtime indexes
					SearchResponse sr = client.prepareSearch(indexes)
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
							.setQuery(QueryBuilders.matchQuery("device_token", device_token_id))
							.addAggregation(AggregationBuilders.terms("PCAT_ID").field("pcat_id").size(5))
							.addAggregation(AggregationBuilders.terms("CAT_ID").field("cat_id").size(5))
							// .addAggregation(AggregationBuilders.dateHistogram("daily_visits").field("datetime")
							// .interval(86400000)
							// .order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
							// .addAggregation(AggregationBuilders.dateHistogram("visits_over_time").field("datetime")
							// .interval(1800000)
							// .order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC)
							// .format("HH:mm"))
							.setSize(1).execute().actionGet();

					int totalStories = 0;

					Terms result = sr.getAggregations().get("CAT_ID");

					Map<String, Object> record = new HashMap<String, Object>();
					record.put(Constants.ROWID, device_token_id);

					// ------------------------------- CAT ID
					// ---------------------------------//
					List<Integer> catIdList = new ArrayList<Integer>();
					for (Terms.Bucket entry : result.getBuckets()) {
						if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
							catIdList.add(Integer.valueOf(entry.getKeyAsString()));
							if (catIdList.size() == 5) {
								break;
							}
						}
					}
					record.put("cat_id", catIdList);

					if (sr.getHits().getTotalHits() > 0) {
						// Get total number of stories for the user
						totalStories = (int) (sr.getHits().getTotalHits());
						record.put("story_count", totalStories);
					}
					// put host detail in record
					if (sr.getHits().getHits().length > 0) {
						// Get total number of stories for the user
						for (SearchHit hit : sr.getHits().getHits()) {
							try {
								String host = hit.getSourceAsMap().get("host").toString();
								record.put("host", Integer.valueOf(host));
								if (StringUtils.isNotBlank((String) hit.getSourceAsMap().get(Constants.CITY))) {
									record.put(Constants.CITY, hit.getSourceAsMap().get(Constants.CITY));
								}
								if (StringUtils.isNotBlank((String) hit.getSourceAsMap().get(Constants.STATE))) {
									record.put(Constants.STATE, hit.getSourceAsMap().get(Constants.STATE));
								}
								if (StringUtils.isNotBlank((String) hit.getSourceAsMap().get(Constants.COUNTRY))) {
									record.put(Constants.COUNTRY, hit.getSourceAsMap().get(Constants.COUNTRY));
								}
								break;
							} catch (Exception e) {
								log.error("Error getting user profile detail. ", e);
								e.printStackTrace();
								continue;
							}
						}
					}

					/*
					 * // Calculate avg stories // ***** LOGIC for avg_stories
					 * per day and user visit time **** InternalDateHistogram
					 * dateHistogram = sr.getAggregations().get("daily_visits");
					 * int totalUserVisits = dateHistogram.getBuckets().size();
					 * int avgStories = 0; // If there is not cat_id summary
					 * then skip that user if (totalUserVisits == 0) {
					 * log.error("No visit found for session id: " +
					 * device_token_id); } else { avgStories = totalStories /
					 * totalUserVisits; }
					 * 
					 * record.put("avg_stories", avgStories);
					 * 
					 * // Calculate min and max time of user visit
					 * ArrayList<Integer> lstTime = new ArrayList<Integer>();
					 * InternalDateHistogram dateHistogram1 =
					 * sr.getAggregations().get("visits_over_time"); if
					 * (dateHistogram1.getBuckets().size() > 0) { for
					 * (DateHistogram.Bucket buck : dateHistogram1.getBuckets())
					 * {
					 * lstTime.add(Integer.valueOf(buck.getKeyAsText().toString(
					 * ).replace(":", ""))); } }
					 * 
					 * record.put("user_first_visit", Collections.min(lstTime));
					 * record.put("user_last_visit", Collections.max(lstTime));
					 */

					// ------------------------------- PCAT ID
					// ---------------------------------//
					Terms result1 = sr.getAggregations().get("PCAT_ID");
					List<Integer> pCatIdList = new ArrayList<Integer>();
					for (Terms.Bucket entry : result1.getBuckets()) {
						if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
							pCatIdList.add(Integer.valueOf(entry.getKeyAsString()));
							if (pCatIdList.size() == 5) {
								break;
							}
						}
					}

					record.put("pcat_id", pCatIdList);

					indexedRecordCount++;
					listMessages.add(record);
					if (listMessages.size() == 1000) {
						elasticSearchIndexService.indexOrUpdate("app_user_top_catid", "realtime", listMessages);
						listMessages.clear();
						log.info("Records indexed in app_user_top_catid index : " + indexedRecordCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
					log.error("Error getting unique visits for device_token_id: " + device_token_id + " exception. ",
							e);
					log.info("Error getting unique visits for device_token_id: " + device_token_id + " exception " + e);
					continue;
				}
			}

			if (listMessages.size() > 0) {
				elasticSearchIndexService.indexOrUpdate("app_user_top_catid", "realtime", listMessages);
				listMessages.clear();
				log.info("Records indexed in app_user_top_catid index : " + indexedRecordCount);
			}
			log.info("Summary of app users completed.");
			long endTime = System.currentTimeMillis();
			log.info("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));

		} catch (ParseException e) {
			e.printStackTrace();
			log.error(e);
			throw new DBAnalyticsException("Invalid date format." + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
			throw new DBAnalyticsException("Error getting details for last date." + e.getMessage());
		}
	}

	/**
	 * This api will return list of pushed notification history record matching
	 * the criteria i.e. hosts and date range.
	 * 
	 * 
	 * @param genericQuery
	 *            query object having different parameters.
	 *            <p>
	 *            hosts : comma separated string of host id. <br/>
	 *            startDate : start datetime to filter the records. <br/>
	 *            endDate : end datetime to filter the records. <br/>
	 *            from : number from where to fetch the records. It is used for
	 *            pagination. Default to 0. <br/>
	 *            count : size of result. Default to 100.
	 *            </p>
	 * @return
	 */
	public List<PushNotificationHistory> getNotificationHistory(GenericQuery genericQuery) {
		List<PushNotificationHistory> notificationHistories = new ArrayList<PushNotificationHistory>();
		try {
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			String hosts = genericQuery.getHosts();
			// Add multiple hosts match conditions to query
			if (StringUtils.isNotBlank(hosts)) {
				String[] hostArray = hosts.split(",");
				BoolQueryBuilder hostQueryBuilder = new BoolQueryBuilder();
				if (hostArray.length > 0) {
					for (String host : hostArray) {
						hostQueryBuilder.should(QueryBuilders.matchQuery(Constants.HOST, host));
					}
				}
				boolQuery.must(hostQueryBuilder);
			}

			// Add start date and end date match conditions to query
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD);
			if (StringUtils.isNotBlank(genericQuery.getStartDate())) {
				rangeQueryBuilder.from(genericQuery.getStartDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getEndDate())) {
				rangeQueryBuilder.to(genericQuery.getEndDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getStartDate())
					|| StringUtils.isNotBlank(genericQuery.getEndDate())) {
				boolQuery.must(rangeQueryBuilder);
			}
			if (StringUtils.isNotBlank(genericQuery.getEditorName())) {
				boolQuery.must(QueryBuilders.wildcardQuery("editorName", "*" + genericQuery.getEditorName() + "*"));
			}

			if (StringUtils.isNotBlank(genericQuery.getEditorId())) {
				boolQuery.must(QueryBuilders.matchQuery("editorId", genericQuery.getEditorId()));
			}

			if (!genericQuery.getInclude().isEmpty()) {
				Set<Map.Entry<String, List<String>>> includeEntrySet = genericQuery.getInclude().entrySet();
				for (Map.Entry<String, List<String>> includeEntry : includeEntrySet) {
					if (includeEntry.getKey().startsWith("target"))
						boolQuery.must(QueryBuilders.termsQuery(includeEntry.getKey(),
								trimAndLowerCase(includeEntry.getValue())));
					else
						boolQuery.must(QueryBuilders.termsQuery(includeEntry.getKey(), includeEntry.getValue()));

				}

			}

			if (!genericQuery.getExclude().isEmpty()) {
				Set<Map.Entry<String, List<String>>> excludeEntrySet = genericQuery.getExclude().entrySet();
				for (Map.Entry<String, List<String>> excludeEntry : excludeEntrySet) {
					if (excludeEntry.getKey().startsWith("target"))
						boolQuery.mustNot(QueryBuilders.termsQuery(excludeEntry.getKey(),
								trimAndLowerCase(excludeEntry.getValue())));
					else
						boolQuery.mustNot(QueryBuilders.termsQuery(excludeEntry.getKey(), excludeEntry.getValue()));

				}

			}

			// Prepare search request and execute it
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.PUSH_NOTIFICATION_HISTORY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
					.setFrom(genericQuery.getFrom()).setQuery(boolQuery).setSize(genericQuery.getCount())
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC);

			if (StringUtils.isNotBlank(genericQuery.getQueryMode())) {
				if (genericQuery.getQueryMode().equalsIgnoreCase("month-wise")) {
					searchRequestBuilder.addAggregation(AggregationBuilders.dateHistogram("datetimeAgg")
							.field("datetime").dateHistogramInterval(DateHistogramInterval.MONTH).format("M")
							.subAggregation(AggregationBuilders.sum("TotalSuccess").field(Constants.SUCCESS))
							.subAggregation(AggregationBuilders.sum("TotalFailure").field(Constants.FAILURE)));
				} else if (genericQuery.getQueryMode().equalsIgnoreCase("day-wise")) {
					searchRequestBuilder.addAggregation(AggregationBuilders.dateHistogram("datetimeAgg")
							.field("datetime").dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							.subAggregation(AggregationBuilders.sum("TotalSuccess").field(Constants.SUCCESS))
							.subAggregation(AggregationBuilders.sum("TotalFailure").field(Constants.FAILURE)));
					// {
					// "aggs": {
					// "articles_over_time": {
					// "date_histogram": {
					// "field": "datetime",
					// "interval": "month",
					// "format" : "yyyy-MM-dd"
					// },
					// "aggs":{"successtotal" : { "sum" : { "field" : "host" }
					// }}
					// }
					// }
					// }

				}
				// Set size 0 if query mode is month-wise or day-wise
				searchRequestBuilder.setSize(0);
			}

			SearchResponse sr = searchRequestBuilder.execute().actionGet();
			SearchHit[] searchHits = sr.getHits().getHits();

			for (SearchHit searchHit : searchHits) {
				String jsonSource = searchHit.getSourceAsString();
				PushNotificationHistory pushNotificationHistory = gson.fromJson(jsonSource,
						PushNotificationHistory.class);
				notificationHistories.add(pushNotificationHistory);
			}

			// Get aggregation result
			if (sr.getAggregations() != null) {
				Histogram dateHistogram = sr.getAggregations().get("datetimeAgg");
				for (Histogram.Bucket bucket : dateHistogram.getBuckets()) {
					PushNotificationHistory pushNotificationHistory = new PushNotificationHistory();
					pushNotificationHistory.setKey(bucket.getKeyAsString());
					Sum successAggregation = bucket.getAggregations().get("TotalSuccess");
					long success = (long) successAggregation.getValue();
					pushNotificationHistory.setSuccess(success);
					Sum failureAggregation = bucket.getAggregations().get("TotalFailure");
					long failure = (long) failureAggregation.getValue();
					pushNotificationHistory.setFailure(failure);
					notificationHistories.add(pushNotificationHistory);
				}
			}

			log.info("Notification logs result size " + notificationHistories.size());
		} catch (Exception e) {
			log.error("Error while retrieving notification history.", e);
		}
		return notificationHistories;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getNotificationSubscribers(GenericQuery genericQuery) {
		Map<String, Object> notificationSubscribers = new HashMap<>();
		Map<String, Object> notificationSubscribersData = new HashMap<>();
		try {
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			if (!genericQuery.getInclude().isEmpty()) {
				Set<Map.Entry<String, List<String>>> includeEntrySet = genericQuery.getInclude().entrySet();
				for (Map.Entry<String, List<String>> includeEntry : includeEntrySet) {
					boolQuery.must(QueryBuilders.termsQuery(includeEntry.getKey(), includeEntry.getValue()));
				}
			}
			if (!genericQuery.getExclude().isEmpty()) {
				Set<Map.Entry<String, List<String>>> excludeEntrySet = genericQuery.getExclude().entrySet();
				for (Map.Entry<String, List<String>> excludeEntry : excludeEntrySet) {
					boolQuery.mustNot(QueryBuilders.termsQuery(excludeEntry.getKey(), excludeEntry.getValue()));
				}

			}

			boolQuery.must(QueryBuilders.termsQuery(Constants.NOTIFICATION_STATUS, new int[] {
					Constants.NOTIFICATION_ON, Constants.NOTIFICATION_UN_REGISTERED, Constants.NOTIFICATION_OFF }));
			boolQuery.must(QueryBuilders.queryStringQuery("device_token:*"));
			// Prepare search request and execute it
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.USER_PERSONALIZATION_STATS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000)).setQuery(boolQuery)
					.setSize(0);

			searchRequestBuilder.addAggregation(
					AggregationBuilders.terms(Constants.NOTIFICATION_STATUS).field(Constants.NOTIFICATION_STATUS));
			SearchResponse sr = searchRequestBuilder.execute().actionGet();
			Terms notificationStatus = sr.getAggregations().get(Constants.NOTIFICATION_STATUS);
			List<Terms.Bucket> notificationBuckets = (List<Bucket>) notificationStatus.getBuckets();

			Map<String, Long> subscriberCounts = new HashMap<>();
			subscriberCounts.put("active", 0l);
			subscriberCounts.put("inActive", 0l);
			for (Terms.Bucket notificationBucket : notificationBuckets) {
				String key = notificationBucket.getKeyAsString();
				switch (key) {
				case "0":
					subscriberCounts.put("inActive", notificationBucket.getDocCount());
					break;
				case "1":
					subscriberCounts.put("active", notificationBucket.getDocCount());
					break;
				case "2":
					subscriberCounts.put("inActive",
							subscriberCounts.get("inActive") + notificationBucket.getDocCount());
				}
			}
			subscriberCounts.put("total", subscriberCounts.get("active") + subscriberCounts.get("inActive"));
			notificationSubscribers.put("subscriberCount", subscriberCounts);

			// getting day-wise unsubscribers
			if (genericQuery.getQueryMode() != null && genericQuery.getQueryMode().equalsIgnoreCase("day-wise")) {
				{
					Map<String, Map<String, Long>> unSubscriberMap = new LinkedHashMap<>();
					BoolQueryBuilder unSubscriberDateQuery = QueryBuilders.boolQuery()
							.must(QueryBuilders.rangeQuery(Constants.NotificationConstants.UN_SUBSCRIBED_AT)
									.gte(genericQuery.getStartDate()).lte(genericQuery.getEndDate()));

					unSubscriberDateQuery.must(boolQuery);
					SearchResponse unSubscriberResponse = client.prepareSearch(Indexes.USER_PERSONALIZATION_STATS)
							.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
							.setQuery(unSubscriberDateQuery).setSize(0)
							.addAggregation(AggregationBuilders
									.dateHistogram(Constants.NotificationConstants.UN_SUBSCRIBED_AT)
									.field(Constants.NotificationConstants.UN_SUBSCRIBED_AT).minDocCount(0)
									.dateHistogramInterval(DateHistogramInterval.DAY).order(Histogram.Order.KEY_ASC)
									.subAggregation(AggregationBuilders
											.terms(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD)
											.field(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD)))
							.execute().actionGet();

					Histogram unSubscriberHistogram = unSubscriberResponse.getAggregations()
							.get(Constants.NotificationConstants.UN_SUBSCRIBED_AT);

					List<Histogram.Bucket> unSubscriberHistogramBuckets = (List<Histogram.Bucket>) unSubscriberHistogram
							.getBuckets();

					for (Histogram.Bucket bucket : unSubscriberHistogramBuckets) {
						Map<String, Long> browserMap = new HashMap<>();
						Terms browserAggregation = bucket.getAggregations()
								.get(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD);
						List<Terms.Bucket> browserBuckets = (List<Bucket>) browserAggregation.getBuckets();
						for (Terms.Bucket browserBucket : browserBuckets) {
							browserMap.put(browserBucket.getKeyAsString(), browserBucket.getDocCount());
							browserMap.put("total", browserMap.getOrDefault("total", 0l) + browserBucket.getDocCount());
						}
						unSubscriberMap.put(bucket.getKeyAsString(), browserMap);
					}
					notificationSubscribersData.put("unSubscriber", unSubscriberMap);
				}

				// getting day-wise sbscribers
				{
					Map<String, Map<String, Long>> subscriberMap = new LinkedHashMap<>();
					BoolQueryBuilder subscriberDateQuery = QueryBuilders.boolQuery()
							.must(QueryBuilders.rangeQuery(Constants.NotificationConstants.SUBSCRIBED_AT)
									.gte(genericQuery.getStartDate()).lte(genericQuery.getEndDate()));

					subscriberDateQuery.must(boolQuery);
					SearchResponse subscriberResponse = client.prepareSearch(Indexes.USER_PERSONALIZATION_STATS)
							.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
							.setQuery(subscriberDateQuery).setSize(0)
							.addAggregation(AggregationBuilders
									.dateHistogram(Constants.NotificationConstants.SUBSCRIBED_AT)
									.field(Constants.NotificationConstants.SUBSCRIBED_AT).minDocCount(0)
									.dateHistogramInterval(DateHistogramInterval.DAY).order(Histogram.Order.KEY_ASC)
									.subAggregation(AggregationBuilders
											.terms(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD)
											.field(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD)))
							.execute().actionGet();

					Histogram subscriberHistogram = subscriberResponse.getAggregations()
							.get(Constants.NotificationConstants.SUBSCRIBED_AT);

					List<Histogram.Bucket> subscriberHistogramBuckets = (List<Histogram.Bucket>) subscriberHistogram
							.getBuckets();

					for (Histogram.Bucket bucket : subscriberHistogramBuckets) {
						Map<String, Long> browserMap = new HashMap<>();
						Terms browserAggregation = bucket.getAggregations()
								.get(Constants.NotificationConstants.BrowserNotificationConstants.BROWSER_FIELD);
						List<Terms.Bucket> browserBuckets = (List<Bucket>) browserAggregation.getBuckets();
						for (Terms.Bucket browserBucket : browserBuckets) {
							browserMap.put(browserBucket.getKeyAsString(), browserBucket.getDocCount());
							browserMap.put("total", browserMap.getOrDefault("total", 0l) + browserBucket.getDocCount());
						}
						subscriberMap.put(bucket.getKeyAsString(), browserMap);
					}
					notificationSubscribersData.put("subscriber", subscriberMap);
				}
			}

			notificationSubscribers.put("data", notificationSubscribersData);

		} catch (Exception e) {
			log.error("Error while retrieving subscribers.", e);
		}
		return notificationSubscribers;
	}

	public List<RealtimeRecord> getReadLaterData(String sessionId, String host) {
		List<RealtimeRecord> records = new ArrayList<RealtimeRecord>();
		try {
			SearchResponse sr = client.prepareSearch(Indexes.USER_READ_LATER_DATA)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
					.setQuery(QueryBuilders.matchQuery("session_id", sessionId)).setSize(50)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute().actionGet();
			SearchHit[] searchHits = sr.getHits().getHits();

			for (SearchHit searchHit : searchHits) {
				String jsonSource = searchHit.getSourceAsString();
				RealtimeRecord realtimeRecord = gson.fromJson(jsonSource, RealtimeRecord.class);
				records.add(realtimeRecord);
			}
		} catch (Exception e) {
			log.error("Error while retrieving read later (Wish-List) data.", e);
		}
		return records;
	}

	List<String> trimAndLowerCase(List<String> list) {
		List<String> temp = new ArrayList<>();
		for (String s : list) {
			temp.add(s.trim().toLowerCase());
		}
		return temp;
	}

	public List<Integer> getMostViewedArticles(ArticleParams articleParams) {
		long startTime = System.currentTimeMillis();

		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);

		String pubTimeFrom = DateUtil.addHoursToCurrentTime(-48);
		// Prepare query dynamically based on run time parameters
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		if (!StringUtils.isBlank(articleParams.getHosts())) {
			String[] hosts = articleParams.getHosts().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hosts));
		}

		if (!StringUtils.isBlank(articleParams.getCatId())) {
			String[] catIds = articleParams.getCatId().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds));
		}

		if (!StringUtils.isBlank(articleParams.getpCatId())) {
			String[] pCatIds = articleParams.getpCatId().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.PARENT_CAT_ID_FIELD, pCatIds));
		}

		if (!articleParams.getTracker().isEmpty()) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.TRACKER, articleParams.getTracker()));
		}

		if (!articleParams.getExcludeTracker().isEmpty()) {
			boolQuery.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, articleParams.getExcludeTracker()));
		}

		/*
		 * Exclude automation stories.
		 */
		boolQuery.mustNot(QueryBuilders.termsQuery("uid", excludeEditorIds));
		boolQuery.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS));
		
		boolQuery.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(pubTimeFrom).to(queryTimeTo));
		boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

		int size = articleParams.getCount();

		SearchResponse sr = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD).size(size)
						// .order(Order.aggregation("StoryPVS", false)).size(size)
						// .subAggregation(AggregationBuilders.sum("StoryPVS").field(Constants.PVS))
						).execute().actionGet();

		Terms result = sr.getAggregations().get("StoryIds");

		ArrayList<Integer> tempResult = new ArrayList<>();
		// Create a list of size given in input query
		for (Terms.Bucket entry : result.getBuckets()) {
			tempResult.add(Integer.valueOf((String) entry.getKeyAsString()));
		}

		if (!StringUtils.isBlank(articleParams.getExcludeStories())) {
			for (String story : articleParams.getExcludeStories().split(",")) {
				tempResult.remove(Integer.valueOf(story));
			}
		}

		log.info("Result size of most viewed stories: " + tempResult.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result:" + tempResult);
		return tempResult;
	}

	public String getCohortGroup(String sessionId) {
		GetResponse response = client.prepareGet().setIndex("user_personalization_stats").setId(sessionId).execute()
				.actionGet();
		if (!response.isExists()) {
			return "A_0";
		} else {
			if (!response.getFields().containsKey(Constants.STORY_COUNT)) {
				return "A_0";
			}
			Long storyCount = (Long) response.getField(Constants.STORY_COUNT).getValue();
			if (storyCount < 5) {
				return "A_" + storyCount;
			} else if (storyCount < 10) {
				return "B_" + storyCount;
			} else if (storyCount < 15) {
				return "C_" + storyCount;
			} else if (storyCount < 20) {
				return "D_" + storyCount;
			} else {
				return "E_" + storyCount;
			}
		}
	}

	public String getUVsPvsForWisdom() {
		long startTime = System.currentTimeMillis();
		String queryDate = DateUtil.getCurrentDate();

		Map<Integer, WisdomUvsPvs> hostUvPvMap = new HashMap<Integer, WisdomUvsPvs>();

		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from("now-5m").to("now"))
		.filter(QueryBuilders.existsQuery(Constants.HOST));

		// Prepare search request for active uvs last 5 min and execute it
		SearchResponse sr = client.prepareSearch("realtime_" + queryDate)
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
				.setQuery(boolQueryBuilder).setSize(0)
				.addAggregation(AggregationBuilders.terms("group_by_hosts").field(Constants.HOST).subAggregation(
						AggregationBuilders.cardinality("unique_session_id").field(Constants.SESSION_ID_FIELD)))
				.execute().actionGet();

		Terms result = sr.getAggregations().get("group_by_hosts");

		for (Terms.Bucket entry : result.getBuckets()) {
			if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
				Cardinality cardinality = entry.getAggregations().get("unique_session_id");
				if (hostUvPvMap.containsKey(Integer.valueOf(entry.getKeyAsString()))) {
					WisdomUvsPvs wisdomuvpv = hostUvPvMap.get(Integer.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setActiveuv(cardinality.getValue());
				} else {
					WisdomUvsPvs wisdomuvpv = new WisdomUvsPvs();
					wisdomuvpv.setHost(String.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setActiveuv(cardinality.getValue());
					hostUvPvMap.put(Integer.valueOf(entry.getKeyAsString()), wisdomuvpv);
				}

			}
		}

		BoolQueryBuilder boolQueryBuilder11 = new BoolQueryBuilder();
		boolQueryBuilder11.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from("now-1m").to("now"))
		.filter(QueryBuilders.existsQuery(Constants.HOST));

		// Prepare search request for active pvs last 1 min and execute it
		SearchResponse sr1 = client.prepareSearch("realtime_" + queryDate).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(boolQueryBuilder11).setSize(0)
				.addAggregation(AggregationBuilders.terms("group_by_hosts").field("host")).execute().actionGet();
		// add the pvs result to model class
		Terms result1 = sr1.getAggregations().get("group_by_hosts");
		for (Terms.Bucket entry : result1.getBuckets()) {
			if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
				if (hostUvPvMap.containsKey(Integer.valueOf(entry.getKeyAsString()))) {
					WisdomUvsPvs wisdomuvpv = hostUvPvMap.get(Integer.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setActivepv(entry.getDocCount());
				} else {
					WisdomUvsPvs wisdomuvpv = new WisdomUvsPvs();
					wisdomuvpv.setHost(String.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setActivepv(entry.getDocCount());
					hostUvPvMap.put(Integer.valueOf(entry.getKeyAsString()), wisdomuvpv);
				}

			}
		}

		// Total PVs
		SearchResponse sr3 = client.prepareSearch("realtime_" + queryDate).setTypes(MappingTypes.MAPPING_REALTIME)
				.addAggregation(AggregationBuilders.terms("group_by_hosts").field("host")).setSize(0).execute()
				.actionGet();
		Terms result3 = sr3.getAggregations().get("group_by_hosts");
		for (Terms.Bucket entry : result3.getBuckets()) {
			if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
				if (hostUvPvMap.containsKey(Integer.valueOf(entry.getKeyAsString()))) {
					WisdomUvsPvs wisdomuvpv = hostUvPvMap.get(Integer.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setPvs(entry.getDocCount());
				} else {
					WisdomUvsPvs wisdomuvpv = new WisdomUvsPvs();
					wisdomuvpv.setHost(String.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setPvs(entry.getDocCount());
					hostUvPvMap.put(Integer.valueOf(entry.getKeyAsString()), wisdomuvpv);
				}

			}
		}

		// Total unique visitors (UVs)
		SearchResponse sr2 = client.prepareSearch("realtime_" + queryDate)
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000)).setSize(0)
				.addAggregation(AggregationBuilders.terms("group_by_hosts").field(Constants.HOST).subAggregation(
						AggregationBuilders.cardinality("unique_session_id").field(Constants.SESSION_ID_FIELD)))
				.addAggregation(
						AggregationBuilders.terms("group_by_hosts_stories").field(Constants.HOST).subAggregation(
								AggregationBuilders.cardinality("unique_story_id").field(Constants.STORY_ID_FIELD)))
				.execute().actionGet();

		// Add the UVs result to model class
		Terms result2 = sr2.getAggregations().get("group_by_hosts");

		for (Terms.Bucket entry : result2.getBuckets()) {
			if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
				Cardinality cardinality = entry.getAggregations().get("unique_session_id");
				if (hostUvPvMap.containsKey(Integer.valueOf(entry.getKeyAsString()))) {
					WisdomUvsPvs wisdomuvpv = hostUvPvMap.get(Integer.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setUvs(cardinality.getValue());
				} else {
					WisdomUvsPvs wisdomuvpv = new WisdomUvsPvs();
					wisdomuvpv.setHost(String.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setUvs(cardinality.getValue());
					hostUvPvMap.put(Integer.valueOf(entry.getKeyAsString()), wisdomuvpv);
				}

			}
		}

		// For pageDepth and unique page depth
		// SearchResponse sr4 = client.prepareSearch("realtime_" + queryDate)
		// .setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new
		// TimeValue(2000)).setSize(0)
		// .addAggregation(AggregationBuilders.terms("group_by_hosts").field(Constants.HOST).subAggregation(
		// AggregationBuilders.cardinality("unique_story_id").field(Constants.STORY_ID_FIELD)))
		// .execute().actionGet();
		//
		Terms result4 = sr2.getAggregations().get("group_by_hosts_stories");

		for (Terms.Bucket entry : result4.getBuckets()) {
			if (entry.getKey() != null && !"0".equalsIgnoreCase(entry.getKeyAsString())) {
				Cardinality cardinality = entry.getAggregations().get("unique_story_id");
				if (hostUvPvMap.containsKey(Integer.valueOf(entry.getKeyAsString()))) {
					WisdomUvsPvs wisdomuvpv = hostUvPvMap.get(Integer.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setStoryCount(cardinality.getValue());
				} else {
					WisdomUvsPvs wisdomuvpv = new WisdomUvsPvs();
					wisdomuvpv.setHost(String.valueOf(entry.getKeyAsString()));
					wisdomuvpv.setStoryCount(cardinality.getValue());
					hostUvPvMap.put(Integer.valueOf(entry.getKeyAsString()), wisdomuvpv);
				}

			}
		}

		List<WisdomUvsPvs> lstWisdomUvPv = new ArrayList<WisdomUvsPvs>();
		for (Map.Entry<Integer, WisdomUvsPvs> hosts : hostUvPvMap.entrySet()) {
			WisdomUvsPvs wup = hosts.getValue();
			double pageDepth = 0.0;
			if (wup.getPvs() != 0 && wup.getUvs() != 0) {
				pageDepth = Double.valueOf(wup.getPvs()) / Double.valueOf(wup.getUvs());
			}
			double uniquePageDepth = 0.0;
			if (wup.getStoryCount() != 0 && wup.getUvs() != 0) {
				uniquePageDepth = Double.valueOf(wup.getStoryCount()) / Double.valueOf(wup.getUvs());
			}
			wup.setPgdepth(pageDepth);
			wup.setPgdepth_story(uniquePageDepth);
			lstWisdomUvPv.add(wup);
			log.info(gson.toJson(wup));
		}
		log.info(gson.toJson(lstWisdomUvPv));
		log.info("Service getUVsPvsForWisdom - Response time(seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return gson.toJson(lstWisdomUvPv);

	}

	public List<PVResponse> getPvDetails(GenericQuery query) {
		List<PVResponse> pvResponses = new ArrayList<PVResponse>();
		long startTime = System.currentTimeMillis();

		String queryDate = DateUtil.getCurrentDate();
		DateHistogramAggregationBuilder dateHistogramBuilder = AggregationBuilders.dateHistogram("pvs")
				.field("datetime")
				.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC);
		if (query.getIntervalFormat().equalsIgnoreCase("minute")) {
			dateHistogramBuilder.dateHistogramInterval(DateHistogramInterval.MINUTE);
		} else if (query.getIntervalFormat().equalsIgnoreCase("hour")) {
			dateHistogramBuilder.dateHistogramInterval(DateHistogramInterval.HOUR);
		}

		SearchRequestBuilder srb = null;
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		boolQuery.must(QueryBuilders.matchQuery(Constants.HOST, query.getHosts()));
		boolQuery.must(
				QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(query.getStartDate()).to(query.getEndDate()));
		srb = client.prepareSearch("realtime_" + queryDate).setTypes(MappingTypes.MAPPING_REALTIME)
				.setTimeout(new TimeValue(2000)).setQuery(boolQuery).setSize(0).addAggregation(dateHistogramBuilder);

		log.info(srb);
		SearchResponse sr = srb.execute().actionGet();

		Histogram dateHistogram = sr.getAggregations().get("pvs");
		for (Histogram.Bucket bucket : dateHistogram.getBuckets()) {
			PVResponse pvResponse = new PVResponse();
			pvResponse.setDatetime(bucket.getKeyAsString());
			pvResponse.setPvs(bucket.getDocCount());
			pvResponses.add(pvResponse);
		}
		log.info(pvResponses);
		log.info("Service getPvDetails - Response time(seconds): " + (System.currentTimeMillis() - startTime) / 1000.0);
		return pvResponses;
	}

	/**
	 * 
	 * @param query
	 * @return
	 */
	public List<UVResponse> getUvDetails(GenericQuery query) {
		long startTime = System.currentTimeMillis();
		List<UVResponse> uvResponses = new ArrayList<UVResponse>();

		String queryDate = DateUtil.getCurrentDate();
		DateHistogramAggregationBuilder dateHistogramBuilder = AggregationBuilders.dateHistogram("uv-datetime")
				.field("datetime")
				.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC);
		if (query.getIntervalFormat().equalsIgnoreCase("minute")) {
			dateHistogramBuilder.dateHistogramInterval(DateHistogramInterval.MINUTE);
		} else if (query.getIntervalFormat().equalsIgnoreCase("hour")) {
			dateHistogramBuilder.dateHistogramInterval(DateHistogramInterval.HOUR);
		}

		SearchRequestBuilder srb = null;
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		boolQuery.must(QueryBuilders.matchQuery(Constants.HOST, query.getHosts()));
		boolQuery.must(
				QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(query.getStartDate()).to(query.getEndDate()));
		srb = client.prepareSearch("realtime_" + queryDate).setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
				.setTimeout(new TimeValue(2000)).setQuery(boolQuery).setSize(0)
				.addAggregation(dateHistogramBuilder.subAggregation(
						AggregationBuilders.cardinality("unique_session_id").field(Constants.SESSION_ID_FIELD)));

		SearchResponse sr = srb.execute().actionGet();
		// log.info(sr);
		Histogram dateHistogram = sr.getAggregations().get("uv-datetime");
		for (Histogram.Bucket bucket : dateHistogram.getBuckets()) {
			Cardinality cardinality = bucket.getAggregations().get("unique_session_id");
			// log.info(cardinality.getValue());
			UVResponse uvResponse = new UVResponse();
			uvResponse.setDatetime(bucket.getKeyAsString());
			uvResponse.setUvs(cardinality.getValue());
			uvResponses.add(uvResponse);
		}
		log.info(uvResponses);
		log.info("Service getUvDetails - Response time(seconds): " + (System.currentTimeMillis() - startTime) / 1000.0);
		return uvResponses;
	}

	public void esToHadoopGenerateDataForHDFS(String prevdate) {
		// String date = DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		String date = prevdate;
		if (date == null || "".equalsIgnoreCase(date.trim())) {
			throw new DBAnalyticsException("Please provide a date");
		}
		try {
			int numRec = 1000;
			String realtime_index = "realtime_" + date;
			System.out.println("searching  in index of date " + date + " strIndexDate: " + realtime_index);
			FileWriter fout = new FileWriter(config.getProperty("tmp.location.hdfs.path") + realtime_index, true);
			BufferedWriter bout = new BufferedWriter(fout);

			SearchResponse scrollResp = client.prepareSearch(realtime_index).setTypes(MappingTypes.MAPPING_REALTIME)
					// .setSearchType(SearchType.SCAN)
					.addSort("_doc", SortOrder.ASC).setScroll(new TimeValue(60000))
					.setQuery(QueryBuilders.matchAllQuery()).setSize(numRec).execute().actionGet(); // 100
			// hits
			// per
			long startTime = System.currentTimeMillis();
			// Scroll until no hits are returned
			long count = 0;
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						String record = hit.getSourceAsString();
						bout.write(record + '\n');
						count++;
					} catch (Exception e) {
						log.error("Error getting session id");
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				System.out.println("Number of records processed " + count);
				log.info("Number of records processed: " + count);
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
			bout.close();
			fout.close();

			long endTime = System.currentTimeMillis();
			log.info("-----------------------------Data Summary Derived----------------------------------");
			System.out.println("INFO: Total time to process the records (Minutes): " + ((endTime - startTime) / (1000 * 60)));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ElasticsearchException
	 * @throws NumberFormatException
	 */

	public String getRecord(String indexName, String mappingType, String key) {
		GetResponse response = client.prepareGet().setIndex(indexName).setType(mappingType).setId(key).execute()
				.actionGet();
		if (response.isExists()) {
			return response.getSourceAsString();
		} else {
			throw new DBAnalyticsException("No record found for key: " + key);
		}
	}

	public List<Map<String, Object>> getWebUserPersonalizationStories(UserPersonalizationQuery query) {
		List<Integer> storiesResponse = getUserPersonalizationStories(query);
		List<Map<String, Object>> result = new ArrayList<>();
		// If there is no story that return blank result.
		if (storiesResponse.isEmpty()) {
			return result;
		}
		MultiGetRequest multiGetRequest = new MultiGetRequest();
		for (Integer story : storiesResponse) {
			multiGetRequest.add(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME, String.valueOf(story));
		}

		MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();
		for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
			Map<String, Object> storyObject = multiGetItemResponse.getResponse().getSourceAsMap();
			Map<String, Object> storyDetail = new HashMap<>();
			storyDetail.put(Constants.STORY_ID_FIELD, storyObject.get(Constants.STORY_ID_FIELD));
			storyDetail.put((Constants.PVS), storyObject.get(Constants.PVS));
			result.add(storyDetail);
		}
		return result;
	}

	public List<Integer> getFlickerTrendingStories(UserPersonalizationQuery query) {
		List<Integer> result = new ArrayList<Integer>();
		long startTime = System.currentTimeMillis();
		// START: Fill cache
		if (flickerTrendingStoriesCache.size() == 0) {
			log.info("Filling flickerTrendingStoriesCache with new values.");

			String queryTimeTo = DateUtil.getCurrentDateTime();
			String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
			List<String> hostList = new ArrayList<String>();
			if (query.getHosts() != null) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			BoolQueryBuilder trackerQueryBuilder = new BoolQueryBuilder();
			if (StringUtils.isNotBlank(query.getTrackers())) {
				List<String> trackers = Arrays.asList(query.getTrackers().split(","));
				for (String tracker : trackers) {
					trackerQueryBuilder.should(QueryBuilders.matchQuery(Constants.TRACKER, tracker));
				}
			}

			BoolQueryBuilder boolQueryMain = new BoolQueryBuilder();
			boolQueryMain.must(trackerQueryBuilder).must(QueryBuilders.termsQuery(Constants.HOST, hostList))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

			SearchResponse srFinalMain = client.prepareSearch(Indexes.STORY_DETAIL).setQuery(boolQueryMain)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
							.order(Order.aggregation("StoryPVS", false)).size(query.getCount())
							.subAggregation(AggregationBuilders.sum("StoryPVS").field(Constants.PVS)))
					.setSize(0).execute().actionGet();
			Terms cidsResultV1 = srFinalMain.getAggregations().get("StoryIds");
			// Create a list of size given in input query
			for (Terms.Bucket entry : cidsResultV1.getBuckets()) {
				flickerTrendingStoriesCache.put(Integer.valueOf(entry.getKeyAsString()), "");
			}

		}
		// END: Fill cache
		result.addAll(flickerTrendingStoriesCache.keySet());
		log.info("Result size of flicker trending stories: " + result.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		// + ", Result: "
		// + result);

		return result;
	}

	public List<Integer> getMostTrendingStories(UserPersonalizationQuery query) {
		List<Integer> result = new ArrayList<Integer>();
		long startTime = System.currentTimeMillis();
		// START: Fill cache
		if (cacheMostTrendingStories.get(query.getHosts()) == null
				|| cacheMostTrendingStories.get(query.getHosts()).size() == 0) {
			log.info("Filling MostTrendingStories for host: " + query.getHosts());

			String queryTimeTo = DateUtil.getCurrentDateTime();
			String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
			List<String> hostList = new ArrayList<String>();
			if (query.getHosts() != null) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			BoolQueryBuilder boolQueryMain = new BoolQueryBuilder();
			boolQueryMain.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

			if (query.getpCatId() != 0) {
				boolQueryMain.must(QueryBuilders.matchQuery(Constants.PARENT_CAT_ID_FIELD, query.getpCatId()));
			}
			SearchResponse srFinalMain = client.prepareSearch(Indexes.STORY_DETAIL).setQuery(boolQueryMain)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
							.order(Order.aggregation("StoryPVS", false)).size(query.getCount())
							.subAggregation(AggregationBuilders.sum("StoryPVS").field(Constants.PVS)))
					.setSize(0).execute().actionGet();
			Terms cidsResultV1 = srFinalMain.getAggregations().get("StoryIds");
			TimerArrayList<Integer> tempResult = new TimerArrayList<Integer>();
			// Create a list of size given in input query
			for (Terms.Bucket entry : cidsResultV1.getBuckets()) {
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}
			cacheMostTrendingStories.put(query.getHosts(), tempResult);
		}
		// END: Fill cache
		result.addAll(cacheMostTrendingStories.get(query.getHosts()));
		log.info("Result size of most trending stories: " + result.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " + result);
		return result;
	}

	public List<Integer> getTopUCBStories(UserPersonalizationQuery query) {
		List<Integer> result = new ArrayList<Integer>();
		long startTime = System.currentTimeMillis();
		// START: Fill cache
		if (cacheTopUcbStories.get(query.getHosts()) == null || cacheTopUcbStories.get(query.getHosts()).size() == 0) {
			log.info("Filling TopUcbStories for host: " + query.getHosts());

			String queryTimeTo = DateUtil.getCurrentDateTime();
			String queryTimeFrom = DateUtil.addHoursToCurrentTime(-1);
			List<String> hostList = new ArrayList<String>();
			if (query.getHosts() != null) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			BoolQueryBuilder boolQueryMain = new BoolQueryBuilder();
			boolQueryMain.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo))
			.must(QueryBuilders.termQuery(Constants.TRACKER, Constants.NEWS_UCB));

			SearchResponse srFinalMain = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryMain)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
							.size(query.getCount()))
					.setSize(0).execute().actionGet();
			Terms cidsResultV1 = srFinalMain.getAggregations().get("StoryIds");
			TimerArrayList<Integer> tempResult = new TimerArrayList<Integer>();
			// Create a list of size given in input query
			for (Terms.Bucket entry : cidsResultV1.getBuckets()) {
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}
			cacheTopUcbStories.put(query.getHosts(), tempResult);
		}
		// END: Fill cache
		result.addAll(cacheTopUcbStories.get(query.getHosts()));
		log.info("Result size of Top Ucb Stories: " + result.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " + result);
		return result;
	}

	public List<Integer> getMicromaxFeedStories(UserPersonalizationQuery query) {
		List<Integer> result = new ArrayList<Integer>();
		long startTime = System.currentTimeMillis();
		// START: Fill cache
		if (cacheMicromaxBrowserStories.get(query.getHosts()) == null
				|| cacheMicromaxBrowserStories.get(query.getHosts()).size() == 0) {
			log.info("Filling getMicromaxFeedStories for host: " + query.getHosts());

			String queryTimeTo = DateUtil.getCurrentDateTime();
			String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
			String queryTimeToDate = queryTimeTo.split("T")[0];
			String queryTimeFromDate = queryTimeFrom.split("T")[0];

			List<String> indexes = new ArrayList<String>();
			indexes.add("realtime_" + DateUtil.getCurrentDate());
			if (!queryTimeToDate.equals(queryTimeFromDate)) {
				indexes.add("realtime_" + queryTimeFromDate.replaceAll("-", "_"));
			}

			String[] indexArray = indexes.toArray(new String[indexes.size()]);

			List<String> hostList = new ArrayList<String>();
			if (query.getHosts() != null) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			String tracker = "news-hf";

			BoolQueryBuilder boolQueryFlickerTracker = new BoolQueryBuilder();
			boolQueryFlickerTracker.must(QueryBuilders.matchQuery(Constants.TRACKER, tracker))
			.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

			SearchResponse trackerSearchResponse = client.prepareSearch(indexArray)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryFlickerTracker)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD).size(10))
					.setSize(0).execute().actionGet();

			Terms cidsResultV1 = trackerSearchResponse.getAggregations().get("StoryIds");

			TimerArrayList<Integer> tempResult = new TimerArrayList<Integer>();
			// Create a list of size given in input query
			for (Terms.Bucket entry : cidsResultV1.getBuckets()) {
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}

			BoolQueryBuilder boolQueryMostViewed = new BoolQueryBuilder();
			boolQueryMostViewed.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
			.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, tempResult))
			.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));
			cacheMicromaxBrowserStories.put(query.getHosts(), tempResult);

			SearchResponse mostViewedSearchResponse = client.prepareSearch(indexArray)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryMostViewed)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD).size(10))
					.setSize(0).execute().actionGet();

			Terms cidsResultMostViewed = mostViewedSearchResponse.getAggregations().get("StoryIds");

			// Create a list of size given in input query
			for (Terms.Bucket entry : cidsResultMostViewed.getBuckets()) {
				tempResult.add(Integer.valueOf(entry.getKeyAsString()));
			}

		}
		// END: Fill cache
		result.addAll(cacheMicromaxBrowserStories.get(query.getHosts()));
		log.info("Result size of micromax stories: " + result.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " + result);
		return result;
	}

	@SuppressWarnings("unchecked")
	public Set<String> getVideoRecommendation(UserPersonalizationQuery query) {
		// List<Integer> result = new ArrayList<Integer>(8);
		Set<String> topvideo = new HashSet<String>();
		Integer video_count = 0;

		long startTime = System.currentTimeMillis();

		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
		String currentDate = DateUtil.getCurrentDate();
		String suffixVideoIndex = currentDate.substring(0, currentDate.lastIndexOf("_"));

		// Check Session ID history in last 7 days for video
		// recommendation,don't recommend video which was viewed
		// by them in last 7 days
		GetResponse response = client.prepareGet().setIndex(Indexes.VIDEO_USER_PROFILE)
				.setType(MappingTypes.MAPPING_REALTIME).setId(query.getSession_id()).execute().actionGet();
		if (!response.isExists()) {
			// Do Nothing and return blank array
		} else {
			List<Integer> viewedLastWeek = new ArrayList<Integer>();
			List<Integer> viewedCurrentDay = new ArrayList<Integer>();
			video_count = (Integer) response.getSource().get(Constants.VIDEO_COUNT);
			viewedLastWeek = (List<Integer>) response.getSource().get(Constants.VIDEO_ID);
			if (video_count != null) {
				// Don't recommend video which they read today
				viewedCurrentDay = getVideoHistoryUser(query.getSession_id(), 20, query.getChannel());
				BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
						.must(QueryBuilders.termQuery("channel", query.getChannel()))
						.mustNot(QueryBuilders.termQuery(Constants.CAT_ID_FIELD, 8))
						.mustNot(QueryBuilders.termsQuery(Constants.VIDEO_ID, viewedLastWeek))
						.mustNot(QueryBuilders.termsQuery(Constants.VIDEO_ID, viewedCurrentDay))
						.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

				int resultSize = 10;
				if (video_count >= 6) {
					resultSize = 10;
				} else if (video_count == 4 || video_count == 5) {
					resultSize = 8;
				} else {
					resultSize = 5;
				}
				// Trending Videos in last 4 hours

				SearchResponse sr = client.prepareSearch("video_tracking_" + suffixVideoIndex)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQueryBuilder)
						.addAggregation(AggregationBuilders.terms("TopVideo").field(Constants.VIDEO_ID)).setSize(0)
						.execute().actionGet();

				Terms videoresult = sr.getAggregations().get("TopVideo");

				for (Terms.Bucket entry : videoresult.getBuckets()) {
					if (entry.getKey() != null) {
						topvideo.add(entry.getKeyAsString());
						if (topvideo.size() == resultSize) {
							break;
						}
					}
				}
			} else {
				log.info("User [" + query.getSession_id() + "] don't view any videos.");
			}

		}
		log.info("Recommended videos for user [" + query.getSession_id() + "], Result Size: " + topvideo.size()
		+ ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: "
		+ topvideo);
		return topvideo;
	}

	private List<Integer> getVideoHistoryUser(String sessionId, Integer videoCount, String channel) {
		long startTime = System.currentTimeMillis();
		List<Integer> videoIds = new ArrayList<Integer>();
		String currentDate = DateUtil.getCurrentDate();
		String suffixVideoIndex = currentDate.substring(0, currentDate.lastIndexOf("_"));
		String fromDate = currentDate.replaceAll("_", "-");
		// Video viewed in a day
		SearchResponse sr = client.prepareSearch("video_tracking_" + suffixVideoIndex)
				.setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(
						QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(Constants.SESSION_ID_FIELD, sessionId))
						.must(QueryBuilders.termQuery("channel", channel))
						.mustNot(QueryBuilders.termQuery(Constants.CAT_ID_FIELD, 8))
						.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(fromDate).to(fromDate)))
				.setSize(videoCount).execute().actionGet();
		long totalHits = sr.getHits().getTotalHits();
		for (SearchHit hit : sr.getHits()) {
			String id = (String) hit.getSource().get(Constants.VIDEO_ID);
			if (StringUtils.isNotBlank(id)) {
				videoIds.add(Integer.valueOf(id));
			}
		}
		log.info("Video history of user [" + sessionId + "] , Total Hits: " + totalHits + ", Result size: "
				+ videoIds.size() + ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0);
		return videoIds;
	}

	public List<Map<String, Object>> getAvgTimeSpent(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> avgTimeSpentResults = new ArrayList<>();
		List<String> inputStoryIdtList = new LinkedList<String>(Arrays.asList(query.getStoryid().split(",")));
		List<String> responseStoryIdList = new ArrayList<>();

		/*
		 * if (storyArr.length > 10) { throw new DBAnalyticsException(
		 * "Please pass storyids less than 10"); }
		 */
		/*for (String storyid : storyIdList) {
			GetResponse res = client.prepareGet(Indexes.STORY_DETAIL_DAYWISE, MappingTypes.MAPPING_REALTIME,
					storyid + "_" + DateUtil.getCurrentDate()).execute().actionGet();
			if (res == null || !res.isExists()) {
				log.info("Calculating time spent for story id: " + storyid);
				storyIdToCalculate.add(storyid);
			} else {
				log.info("Getting time spent from index for story id: " + storyid);
				avgTimeSpentResults.add(res.getSource());
			}

		}*/

		//	inputStoryIdtList = Arrays.asList(query.getStoryid().split(","));

		BoolQueryBuilder bqb = new BoolQueryBuilder();
		if(query.getStartDate()!=null&&query.getEndDate()!=null){
			bqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate()).lte(query.getEndDate()));
		}
		bqb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, inputStoryIdtList));

		SearchResponse res = client.prepareSearch(Indexes.STORY_DETAIL_DAYWISE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(bqb).addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(10)
						.subAggregation(AggregationBuilders.sum(Constants.ACTIVE_USERS).field(Constants.ACTIVE_USERS))
						.subAggregation(AggregationBuilders.sum(Constants.USER_COUNT).field(Constants.USER_COUNT))
						.subAggregation(AggregationBuilders.sum(Constants.TIME_SPENT).field(Constants.TIME_SPENT)))
				.execute().actionGet();		

		Terms storiesAgg = res.getAggregations().get("stories");
		for(Terms.Bucket storiesBucket:storiesAgg.getBuckets()) {

			long active_users =0;
			long user_count = 0;
			long time_spent = 0;
			Map<String, Object> storiesMap = new HashMap<>();

			String storyid = storiesBucket.getKeyAsString();
			storiesMap.put("storyid", storyid);
			responseStoryIdList.add(storyid);

			Sum	activeUsers = storiesBucket.getAggregations().get(Constants.ACTIVE_USERS);
			active_users = new Double(activeUsers.getValue()).longValue();
			storiesMap.put("active_users", active_users);

			Sum	userCount = storiesBucket.getAggregations().get(Constants.USER_COUNT);
			user_count = new Double(userCount.getValue()).longValue();
			storiesMap.put("user_count", user_count);

			Sum	timeSpent = storiesBucket.getAggregations().get(Constants.TIME_SPENT);
			time_spent = new Double(timeSpent.getValue()).longValue();

			storiesMap.put("time_spent", time_spent);	


			avgTimeSpentResults.add(storiesMap);		
		}		
		inputStoryIdtList.removeAll(responseStoryIdList);		
		avgTimeSpentResults.addAll(calculateTimeSpent(inputStoryIdtList, false));
		System.out.println("Total time for " + avgTimeSpentResults.size() + " stories : "+ (System.currentTimeMillis() - startTime) / 1000);
		for(Map<String, Object> map:avgTimeSpentResults)
		{
			if(map.get("active_users") instanceof Integer){
				if((Integer)map.get("active_users")!=0){
					map.put("avgTimeSpent", (Integer)map.get("time_spent")/(Integer)map.get("active_users"));
				}
				else{
					map.put("avgTimeSpent", 0);
				}
			}
			else if(map.get("active_users") instanceof Long){
				if((Long)map.get("active_users")!= 0) {
					map.put("avgTimeSpent", (Long) map.get("time_spent") / (Long) map.get("active_users"));
				} else {
					map.put("avgTimeSpent", 0);
				}
			}
		}
		return avgTimeSpentResults;
	}

	/*
	 * public List<Map<String, Object>> getAvgTimeSpent(WisdomQuery query, Boolean
	 * app) { long startTime = System.currentTimeMillis(); List<Map<String, Object>>
	 * avgTimeSpentResults = new ArrayList<>(); List<String> storyIdList =
	 * Arrays.asList(storyids.split(",")); List<String> storyIdToCalculate = new
	 * ArrayList<>();
	 * 
	 * if (storyArr.length > 10) { throw new DBAnalyticsException(
	 * "Please pass storyids less than 10"); }
	 * 
	 * if (app) { for (String storyid : storyIdList) { Map<String, Object> storyMap
	 * = new HashMap<>(); GetResponse res =
	 * client.prepareGet(Indexes.STORY_DETAIL_DAYWISE,
	 * MappingTypes.MAPPING_REALTIME, storyid + "_" + DateUtil.getCurrentDate() +
	 * "_app").execute().actionGet(); if (res == null || !res.isExists()) {
	 * log.info("Calculating time spent for story id: " + storyid);
	 * storyIdToCalculate.add(storyid); } else {
	 * log.info("Getting time spent from index for story id: " + storyid);
	 * storyMap.put("storyid", storyid); storyMap.put("avgTimeSpent",
	 * res.getSource().get("avg_time_spent").toString());
	 * avgTimeSpentResults.add(storyMap); }
	 * 
	 * } avgTimeSpentResults.addAll(calculateTimeSpent(storyIdToCalculate, true)); }
	 * else { avgTimeSpentResults = getAvgTimeSpent(storyids); }
	 * System.out.println("Total time for " + avgTimeSpentResults.size() +
	 * " stories : " + (System.currentTimeMillis() - startTime) / 1000); return
	 * avgTimeSpentResults; }
	 */

	public Set<String> getSources(GenericQuery genericQuery) {
		long startTime = System.currentTimeMillis();
		String indexNameofAppSummary = Indexes.APP_USER_PROFILE;
		Set<String> topValues = getTopUniqueValues(indexNameofAppSummary, MappingTypes.MAPPING_REALTIME,
				Constants.NotificationConstants.SOURCE_FIELD, genericQuery.getCount()).keySet();
		topValues.remove(String.valueOf(""));
		log.info(" Total time getSources API : " + (System.currentTimeMillis() - startTime) / 1000);
		return topValues;
	}

	public Map<String, Long> getVideoPartners(GenericQuery genericQuery) {
		Instant startTime = Instant.now();
		String indexName = Indexes.USER_PERSONALIZATION_STATS;
		Map<String, Long> topValues = getTopUniqueValues(indexName, MappingTypes.MAPPING_REALTIME,
				Constants.NotificationConstants.PARTNER_FIELD, genericQuery.getCount());
		topValues.remove(String.valueOf(""));
		log.info(" Total time getPartner API : " + Duration.between(startTime, Instant.now()).toMillis() / 1000);
		return topValues;
	}

	public Map<String, Long> getFollowTags(GenericQuery genericQuery) {
		Instant startTime = Instant.now();
		String indexName = Indexes.USER_PERSONALIZATION_STATS;
		Map<String, Long> topValues = getTopUniqueValues(indexName, MappingTypes.MAPPING_REALTIME,
				Constants.NotificationConstants.FOLLOW_TAG_FIELD, genericQuery.getCount());
		topValues.remove(String.valueOf(""));
		log.info(" Total time getFollowTags API : " + Duration.between(startTime, Instant.now()).toMillis() / 1000);
		return topValues;
	}

	/**
	 *
	 * @param indexes
	 * @param types
	 * @param field
	 * @param size
	 * @return this function returns unique terms for a field
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Long> getTopUniqueValues(String indexes, String types, String field, int size) {
		Map<String, Long> topValues = new HashMap<>();
		SearchResponse response = client.prepareSearch().setIndices(indexes.split(String.valueOf(",")))
				.setTypes(types.split(String.valueOf(","))).setSize(0)
				.addAggregation(AggregationBuilders.terms("topValues").field(field).size(size)).execute().actionGet();
		Terms topValuesAggr = response.getAggregations().get("topValues");
		List<Terms.Bucket> topValuesBuckets = (List<Bucket>) topValuesAggr.getBuckets();
		topValues = topValuesBuckets.stream().collect(Collectors.toMap(Terms.Bucket::getKeyAsString,
				Terms.Bucket::getDocCount, (oldValue, newValue) -> newValue, LinkedHashMap::new));
		return topValues;
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> calculateTimeSpent(List<String> storyIds, Boolean app) {
		List<Map<String, Object>> records = new ArrayList<>();
		for (String storyid : storyIds) {
			long start = System.currentTimeMillis();
			Map<String, Object> storyMap = new HashMap<>();
			Double activeTimeSpent = 0.0; // time spent of users who read more den one slide
			Double totalTimeSpent = 0.0;
			Double avgTimeSpent = 0.0;
			String id = "";

			if (avgStoryTimeCache.containsKey(storyid)) {
				storyMap = (Map<String, Object>) avgStoryTimeCache.get(storyid);
				long end = System.currentTimeMillis();
				long time = end - start;
				log.info("Getting result from cache for story: " + storyid + "; Average Time Spent: " + totalTimeSpent
						+ " ; Execution Time:(Seconds) " + time / 1000.0);
				// System.out.println("Getting result from cache for story: " +
				// storyid);
			} else {
				ArrayList<Double> timeList = new ArrayList<>();
				String indexName = "realtime_" + DateUtil.getCurrentDate();
				BoolQueryBuilder bqb = QueryBuilders.boolQuery()
						.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, storyid));
				if (app) {
					id = storyid + "_" + DateUtil.getCurrentDate() + "_app";
					bqb.must(QueryBuilders.termQuery(Constants.TRACKER, Constants.APP));
				} else {
					id = storyid + "_" + DateUtil.getCurrentDate();
					bqb.mustNot(QueryBuilders.termQuery(Constants.TRACKER, Constants.APP));
				}
				SearchResponse sr = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(bqb).setSize(1)
						.addAggregation(AggregationBuilders.terms("user").field(Constants.SESSION_ID_FIELD).size(5000)
								.subAggregation(AggregationBuilders.dateHistogram("interval")
										.field(Constants.DATE_TIME_FIELD).interval(1800000)
										.subAggregation(AggregationBuilders.scriptedMetric("time")
												.initScript(new Script("params._agg.time=[]"))
												.mapScript(new Script("params._agg.time.add(doc['datetime'].value);"))
												.combineScript(new Script(
														" def all = [];for (a in params._agg.time) {all.add(a);} return all;"))
												.reduceScript(new Script(
														"def min = 0; def max = 0; for (a in params._aggs) {if(a==null)continue;for(i in a){if (min == 0 || i < min) { min = i;} if (max == 0 || i > max) { max = i; }}} return (max-min)/1000.0")))))
						.execute().actionGet();

				int totalPageCount = 0;

				if (sr.getHits().getHits().length > 0) {
					if (!StringUtils.isBlank(sr.getHits().getHits()[0].getSource().get(Constants.PGTOTAL).toString()))
						totalPageCount = Integer
								.parseInt(sr.getHits().getHits()[0].getSource().get(Constants.PGTOTAL).toString());
				}
				Terms userTerms = sr.getAggregations().get("user");
				for (Terms.Bucket userBucket : userTerms.getBuckets()) {
					Histogram interval = userBucket.getAggregations().get("interval");
					ScriptedMetric time = interval.getBuckets().get(0).getAggregations().get("time");
					Double timeS = (Double) time.aggregation();
					if (timeS > 0.0)
						timeList.add(timeS);
				}

				int totalUsers = userTerms.getBuckets().size();
				long activeUsers = timeList.size();
				totalTimeSpent = sum(timeList);

				/*
				 * if (!timeList.isEmpty()) { activeTimeSpent = avg(timeList); } int
				 * remainingUsers = totalUsers - activeUsers; if (totalPageCount != 0) { Double
				 * timePerPage = activeTimeSpent / totalPageCount; totalTimeSpent =
				 * (activeTimeSpent * activeUsers) + (timePerPage * remainingUsers);
				 * avgTimeSpent = (totalTimeSpent) / totalUsers; } else { totalTimeSpent =
				 * activeTimeSpent * activeUsers; avgTimeSpent = activeTimeSpent; }
				 * 
				 * totalTimeSpent = activeTimeSpent * activeUsers;
				 */
				storyMap.put("storyid", storyid);
				storyMap.put("user_count", totalUsers);
				storyMap.put("active_users", activeUsers);
				storyMap.put("time_spent", totalTimeSpent.longValue());
				storyMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				storyMap.put(Constants.ROWID, id);
				if (totalTimeSpent != 0)
					elasticSearchIndexService.indexOrUpdate(Indexes.STORY_DETAIL_DAYWISE, MappingTypes.MAPPING_REALTIME,
							storyMap);
				avgStoryTimeCache.put(storyid, storyMap, 600 * 1000);
				long end = System.currentTimeMillis();
				long time = end - start;
				log.info("Story: " + storyid + "; No. of slides:" + totalPageCount + "; Total Users:" + totalUsers
						+ "; Active Users:" + activeUsers + "; Active user time spent:" + activeTimeSpent
						+ "; Average Time Spent: " + totalTimeSpent + " ; Execution Time:(Seconds) " + time / 1000.0);
				/*
				 * System.out.println("Story: " + storyid + "; No. of slides:" + totalPageCount
				 * + "; Total Users:" + totalUsers + "; Active Users:" + activeUsers +
				 * "; Active user time spent:" + activeTimeSpent + "; Average Time Spent: " +
				 * totalTimeSpent + " ; Execution Time:(Seconds) " + time / 1000.0);
				 */
			}
			records.add(storyMap);
		}
		return records;
	}

	private Double avg(List<Double> list) {
		Double sum = 0.0;
		for (Double i : list) {
			sum += i;
		}
		return sum / list.size();
	}

	private Double sum(List<Double> list) {
		Double sum = 0.0;
		for (Double i : list) {
			sum += i;
		}
		return sum;
	}

	private List<String> loadEnglishStopWords() {
		List<String> list = new ArrayList<String>();
		try {
			BufferedReader in = new BufferedReader(
					new FileReader("F:\\sts-workspace\\db-analytics\\src\\main\\resources\\english-stop-words.txt"));
			String str;

			while ((str = in.readLine()) != null) {
				list.add(str);
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public List<RealtimeRecord> getKeywordRecommendation(GenericQuery genericQuery) {
		List<RealtimeRecord> records = new ArrayList<RealtimeRecord>();

		String storyid = genericQuery.getStoryid();

		String storyDetail = getRecord(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME, storyid);
		RealtimeRecord record = gson.fromJson(storyDetail, RealtimeRecord.class);
		log.info("Input Story Title:" + record.getTitle());
		log.info("Input URL:" + record.getUrl());

		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-96);
		List<String> englishStopWords = loadEnglishStopWords();
		try {

			BoolQueryBuilder boolFilter = new BoolQueryBuilder()
					.must(QueryBuilders.rangeQuery(Constants.STORY_MODIFIED_TIME).from(queryTimeFrom).to(queryTimeTo))
					.must(QueryBuilders.termQuery(Constants.HOST, record.getHost()))
					.must(QueryBuilders.termQuery(Constants.PARENT_CAT_ID_FIELD, record.getPcat_id()))
					.mustNot(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, storyid));

			List<String> nlpNouns = (List<String>) record.getNlp_nouns();
			nlpNouns.removeAll(englishStopWords);
			log.info("Nouns of input story:" + nlpNouns);

			BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(boolFilter)
					.must(QueryBuilders.matchQuery(Constants.TITLE, record.getTitle()));

			SearchResponse sr = client.prepareSearch(Indexes.STORY_UNIQUE_DETAIL)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000)).setQuery(queryBuilder)
					.setSize(4).execute().actionGet();
			SearchHit[] searchHits = sr.getHits().getHits();
			log.info("Recommendation Articles:");
			for (SearchHit searchHit : searchHits) {
				String jsonSource = searchHit.getSourceAsString();
				RealtimeRecord realtimeRecord = gson.fromJson(jsonSource, RealtimeRecord.class);
				log.info(realtimeRecord.getUrl());
				log.info(realtimeRecord.getTitle());
				records.add(realtimeRecord);
			}
		} catch (Exception e) {
			log.error("Error while retrieving read later (Wish-List) data.", e);
		}
		log.info(records);
		return records;
	}

	public Set<Integer> getRealtimePersonalizedRecommendation(UserPersonalizationQuery query)
			throws DBAnalyticsException {

		Set<Integer> result = new HashSet<>();

		long startTime = System.currentTimeMillis();
		int interestBasedCountV1 = 0;
		int interestBasedCountV2 = 0;
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
		String queryTimeTo = DateUtil.getCurrentDateTime();
		int latestArticleCount = 2;
		int mostViewedArticleCount = 5;

		try {
			List<String> hostList = new ArrayList<>();
			if (StringUtils.isNotBlank(query.getHosts())) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			// Get history for specified user
			if (StringUtils.isNotBlank(query.getSession_id())) {
				Set<Integer> catIds = new HashSet<>();
				if (query.getCat_id() != null && query.getCat_id().size() > 0) {
					catIds.addAll(query.getCat_id());
				} else {
					SearchResponse sr = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
							.setQuery(QueryBuilders.matchQuery("session_id", query.getSession_id())).setSize(20)
							.execute().actionGet();
					SearchHit[] searchHits = sr.getHits().getHits();

					Set<Integer> readStories = new HashSet<>();
					for (SearchHit searchHit : searchHits) {
						catIds.add((Integer) searchHit.getSource().get(Constants.CAT_ID_FIELD));
						readStories
								.add(Integer.valueOf(searchHit.getSource().get(Constants.STORY_ID_FIELD).toString()));
					}
				}

				if (catIds.size() > 0) {
					BoolQueryBuilder latestArticleQuery = new BoolQueryBuilder();
					latestArticleQuery.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
							.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds))
							// .mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD,
							// readStories))
							.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(queryTimeFrom)
									.to(queryTimeTo));

					SearchResponse srFinalV1 = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(latestArticleQuery)
							.addSort(Constants.STORY_PUBLISH_TIME, SortOrder.DESC).setSize(latestArticleCount).execute()
							.actionGet();

					for (SearchHit searchHit : srFinalV1.getHits().getHits()) {
						result.add(Integer.valueOf(searchHit.getSource().get(Constants.STORY_ID_FIELD).toString()));
					}

					interestBasedCountV1 = result.size();

					BoolQueryBuilder mostViewedArticleQuery = new BoolQueryBuilder();
					mostViewedArticleQuery.must(QueryBuilders.termsQuery(Constants.HOST, hostList))
							.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds))
							.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, result)).must(QueryBuilders
									.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

					SearchResponse srFinalV2 = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(mostViewedArticleQuery)
							.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
									.size(mostViewedArticleCount))
							.setSize(0).execute().actionGet();

					Terms cidsResultV2 = srFinalV2.getAggregations().get("StoryIds");
					// Create a list of size given in input query
					for (Terms.Bucket entry : cidsResultV2.getBuckets()) {
						result.add(Integer.valueOf(entry.getKeyAsString()));
					}
					interestBasedCountV2 = result.size() - interestBasedCountV1;

				}
			}
		} catch (Exception e) {
			log.error("Error occured while retrieving Realtime Recommended HP stories for user ["
					+ query.getSession_id() + "]", e);
			return result;
		}
		log.info("Realtime Recommended HP stories for user [" + query.getSession_id() + "], Result Size: "
				+ result.size() + ", V1=" + interestBasedCountV1 + ", V2=" + interestBasedCountV2
				+ ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: "
				+ result);
		return result;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set<Integer> getRealtimePersonalizedRecommendationV2(UserPersonalizationQuery query)
			throws DBAnalyticsException {

		Set<Integer> result = new LinkedHashSet<>();

		long startTime = System.currentTimeMillis();
		int interestBasedCountV1 = 0;
		int interestBasedCountV2 = 0;
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);
		/*
		 * Changed from 72 hours to 48 hours as of 19 June 2018.
		 */
		String publishTimeFrom = DateUtil.addHoursToCurrentTime(-48);
		String queryTimeTo = DateUtil.getCurrentDateTime();
		// int latestArticleCount = 2;
		int mostViewedArticleCount = 10;
		if (query.getInCount() != 0) {
			mostViewedArticleCount = query.getInCount();
		}

		try {
			List<String> hostList = new ArrayList<>();
			if (StringUtils.isNotBlank(query.getHosts())) {
				hostList = Arrays.asList(query.getHosts().split(","));
			}

			// Get history for specified user
			if (StringUtils.isNotBlank(query.getSession_id())) {
				Set<Integer> catIds = new HashSet<>();
				Set<String> people = new HashSet<>();
				Set<String> events = new HashSet<>();
				Set<String> organizations = new HashSet<>();

				SearchResponse sr = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
						.setQuery(QueryBuilders.matchQuery("session_id", query.getSession_id())).setSize(20).execute()
						.actionGet();
				SearchHit[] searchHits = sr.getHits().getHits();

				Set<Integer> readStories = new HashSet<>();
				for (SearchHit searchHit : searchHits) {
					catIds.add((Integer) searchHit.getSource().get(Constants.CAT_ID_FIELD));
					if (searchHit.getSource().get(Constants.PEOPLE) != null
							&& searchHit.getSource().get(Constants.PEOPLE) instanceof List) {
						people.addAll((List) searchHit.getSource().get(Constants.PEOPLE));
					}

					if (searchHit.getSource().get(Constants.EVENT) != null
							&& searchHit.getSource().get(Constants.EVENT) instanceof List) {
						events.addAll((List) searchHit.getSource().get(Constants.EVENT));
					}

					if (searchHit.getSource().get(Constants.ORGANIZATION) != null
							&& searchHit.getSource().get(Constants.ORGANIZATION) instanceof List) {
						organizations.addAll((List) searchHit.getSource().get(Constants.ORGANIZATION));
					}

					readStories.add(Integer.valueOf(searchHit.getSource().get(Constants.STORY_ID_FIELD).toString()));
				}

				if (!catIds.isEmpty()) {

					BoolQueryBuilder nlpTagsQuery = new BoolQueryBuilder();
					nlpTagsQuery.should(QueryBuilders.termsQuery(Constants.PEOPLE, people))
							.should(QueryBuilders.termsQuery(Constants.ORGANIZATION, organizations))
							.should(QueryBuilders.termsQuery(Constants.EVENT, events))
							.should(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds));

					BoolQueryBuilder mostViewedArticleQuery = new BoolQueryBuilder();
					mostViewedArticleQuery.must(QueryBuilders.termsQuery(Constants.HOST, hostList)).must(nlpTagsQuery)
							.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, readStories))
							.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom)
									.to(queryTimeTo))
							.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(publishTimeFrom)
									.to(queryTimeTo))
							.mustNot(QueryBuilders.termsQuery(Constants.UID, excludeEditorIds))
							.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS));

					SearchResponse srFinalV2 = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(mostViewedArticleQuery)
							.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID_FIELD)
									.size(mostViewedArticleCount))
							.setSize(0).execute().actionGet();

					Terms cidsResultV2 = srFinalV2.getAggregations().get("StoryIds");
					// Create a list of size given in input query
					for (Terms.Bucket entry : cidsResultV2.getBuckets()) {
						result.add(Integer.valueOf(entry.getKeyAsString()));
					}
					interestBasedCountV1 = result.size();

				}
			}

			if (interestBasedCountV1 < mostViewedArticleCount) {
				List<Integer> tempResult = getUserPersonalizationStories(query);
				for (Integer integer : tempResult) {
					result.add(integer);
					if (result.size() == mostViewedArticleCount) {
						break;
					}
				}

				interestBasedCountV2 = result.size() - interestBasedCountV1;
			}
		} catch (Exception e) {
			log.error("Error occured while retrieving Realtime Recommended HP stories for user ["
					+ query.getSession_id() + "]", e);
			return result;
		}
		log.info("Realtime Recommended HP stories for user [" + query.getSession_id() + "], Result Size: "
				+ result.size() + ", V1 (Current day personalizaed stories) =" + interestBasedCountV1 + ", V2="
				+ interestBasedCountV2 + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " + result);
		return result;
	}

	/*
	 * Get 2 latest stories of last 4 hours & 3 most viewed of last 4 hours
	 */
	public Map<String, List<Integer>> getCityWiseStories(ArticleParams articleParams) {
		long startTime = System.currentTimeMillis();
		int size = 2;
		int mvsize = 3;

		if (articleParams.getInCount() != 0) {
			mvsize = articleParams.getInCount() - size;
		}

		Map<String, List<Integer>> finalResult = new HashMap<>();

		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);

		// Prepare query dynamically based on run time parameters
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (!StringUtils.isBlank(articleParams.getHosts())) {
			String[] hosts = articleParams.getHosts().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hosts));
		}

		if (!StringUtils.isBlank(articleParams.getCatId())) {
			String[] catIds = articleParams.getCatId().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, catIds));
		}

		boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));
		String includes[] = { Constants.STORY_ID_FIELD };
		SearchResponse sr = client.prepareSearch("realtime_" + DateUtil.getCurrentDate())
				.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.terms("CATEGORY").field(Constants.CAT_ID_FIELD)
						.subAggregation(AggregationBuilders.terms("LATEST_STORIES").field(Constants.STORY_PUBLISH_TIME)
								.size(size).order(Terms.Order.term(false))
								.subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(includes, new String[] {}).size(1)))
						.subAggregation(
								AggregationBuilders.terms("MOST_VIEWED").field(Constants.STORY_ID_FIELD).size(mvsize)))

				.execute().actionGet();

		Terms categoryTermsAgg = sr.getAggregations().get("CATEGORY");
		for (Terms.Bucket categoryBucket : categoryTermsAgg.getBuckets()) {
			ArrayList<Integer> tempResult = new ArrayList<>();
			// Fill latest stories
			Terms latestStoriesTermsAgg = categoryBucket.getAggregations().get("LATEST_STORIES");
			for (Terms.Bucket latestStoryTermAgg : latestStoriesTermsAgg.getBuckets()) {
				TopHits topHits = latestStoryTermAgg.getAggregations().get("top");
				if (topHits.getHits().getHits()[0].getSource().get(Constants.STORY_ID_FIELD) != null) {
					tempResult.add(Integer.valueOf(
							topHits.getHits().getHits()[0].getSource().get(Constants.STORY_ID_FIELD).toString()));
				}
			}
			// Fill most viewed stories
			Terms storyIdAgg = categoryBucket.getAggregations().get("MOST_VIEWED");
			for (Terms.Bucket mostViewedStoryBucket : storyIdAgg.getBuckets()) {
				if (!tempResult.contains(Integer.valueOf(mostViewedStoryBucket.getKeyAsString()))) {
					tempResult.add(Integer.valueOf(mostViewedStoryBucket.getKeyAsString()));
				}

			}
			finalResult.put(categoryBucket.getKeyAsString(), tempResult);
		}
		log.info("City wise stories. Execution time(Sec): " + (System.currentTimeMillis() - startTime) / 1000.0
				+ "; Result: " + finalResult);
		return finalResult;
	}

	public List<Integer> getFacebookTrendingArticles(ArticleParams articleParams) {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder topicBoolQuery = new BoolQueryBuilder();
		String topicList = articleParams.getTopic().toLowerCase();
		topicList = topicList.replaceAll("and", "");
		topicList = topicList.replaceAll("or", "");
		topicList = topicList.replaceAll("the", "");

		// String topicArray[] = articleParams.getTopic().split(",");

		topicBoolQuery.must(
				QueryBuilders.multiMatchQuery(topicList, "people.text", "event.text", "organization.text", "keywords"));
		/*
		 * for (String topic : topicArray) { if (StringUtils.isNotBlank(topic)) {
		 * topicBoolQuery .should(QueryBuilders.multiMatchQuery(topic, "people",
		 * "event", "organization")); //
		 * topicBoolQuery.should(QueryBuilders.multiMatchQuery(topic, //
		 * Constants.PEOPLE, Constants.EVENT, // Constants.ORGANIZATION,
		 * Constants.LOCATION)); } }
		 */

		String queryTimeTo = DateUtil.getCurrentDateTime();
		// String queryTimeFrom = DateUtil.addHoursToCurrentTime(-4);

		// Prepare query dynamically based on run time parameters
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (!StringUtils.isBlank(articleParams.getHosts())) {
			String[] hosts = articleParams.getHosts().split(",");
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hosts));
		}

		boolQuery.must(topicBoolQuery);
		// boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));
		// Datetime of before 3 days.
		String storyPubDateFrom = DateUtil.addHoursToCurrentTime(-48);
		boolQuery.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(storyPubDateFrom).to(queryTimeTo));
		/*
		 * Exclude Automation Stories
		 */
		boolQuery.mustNot(QueryBuilders.termsQuery("uid", excludeEditorIds));

		/*
		 * Exclude stories from social.
		 */
		boolQuery.mustNot(QueryBuilders.termsQuery(Constants.UID, Constants.SOCIAL_EDITOR_UIDS));

		SearchResponse sr = client.prepareSearch(Indexes.STORY_UNIQUE_DETAIL).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(boolQuery).setSize(articleParams.getCount()).execute().actionGet();
		SearchHit[] searchhits = sr.getHits().getHits();
		ArrayList<Integer> tempResult = new ArrayList<>();
		for (SearchHit extractTopic : searchhits) {
			tempResult.add(Integer.valueOf(extractTopic.getId()));
		}

		log.info("Result size of facebook trending articles: " + tempResult.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result:" + tempResult);
		return tempResult;
	}

	public Long getRecordsCountByHost(FetchRecordsCountByHost query) {
		Long totalRecords = 0l;
		String dateValue = query.getDateValue();
		List<Integer> pageNumberList = query.getPageNumber();
		String trackerValue = query.getTrackerValue();
		Integer host = query.getHost();

		BoolQueryBuilder boolQueryMain = new BoolQueryBuilder();

		try {

			boolQueryMain.must(QueryBuilders.termQuery(Constants.HOST, host))
					.must(QueryBuilders.termsQuery("pgno", pageNumberList))
					// .must(QueryBuilders.prefixQuery(Constants.TRACKER,
					// trackerValue));
					.must(QueryBuilders.wildcardQuery(Constants.TRACKER, trackerValue));

			SearchResponse sr = client.prepareSearch("realtime_" + dateValue).setTypes(MappingTypes.MAPPING_REALTIME)
					.setTimeout(new TimeValue(2000)).setQuery(boolQueryMain).execute().actionGet();

			totalRecords = sr.getHits().getTotalHits();
		} catch (Exception e) {
			log.error(e);
		}
		return totalRecords;
	}

	public List<String> getHigestCTRArticles(ArticleParams articleParams) {
		long startTime = System.currentTimeMillis();
		int size = 2;
		if (articleParams.getInCount() != 0) {
			size = articleParams.getInCount();
		}
		BoolQueryBuilder storiesIdBoolQuery = new BoolQueryBuilder();
		String InputStoriesId[] = articleParams.getInputStoriesId().split(",");

		storiesIdBoolQuery.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, InputStoriesId));

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (!StringUtils.isBlank(articleParams.getHosts())) {
			// String[] hosts = articleParams.getHosts().split(",");
			// Commented below condition to get CTR of mix WEB + Mobile
			// boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hosts));
		}
		boolQuery.must(storiesIdBoolQuery);
		SearchResponse sr = client.prepareSearch(Indexes.IDENTIFICATION_STORY_DETAIL)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery)
				.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
						.order(Order.aggregation("views", false))
						.subAggregation(AggregationBuilders.sum("impressions").field(Constants.IMPRESSIONS))
						.subAggregation(AggregationBuilders.sum("views").field(Constants.VIEWS)))
				.setSize(0).execute().actionGet();

		Terms aggregationagg = sr.getAggregations().get("storyid");
		ArrayList<String> eodStories = new ArrayList<>();
		Map<Float, String> tempResult = new TreeMap<>();

		for (Terms.Bucket mostViewedStoryBucket : aggregationagg.getBuckets()) {
			Sum impressions = mostViewedStoryBucket.getAggregations().get("impressions");
			Sum views = mostViewedStoryBucket.getAggregations().get("views");
			String gk = mostViewedStoryBucket.getKeyAsString();
			Float impressionCount = (float) impressions.getValue();
			Float viewCount = (float) views.getValue();
			Float CTR = viewCount / impressionCount;
			tempResult.put(CTR, gk);
		}
		NavigableMap<Float, String> nmap = ((TreeMap<Float, String>) tempResult).descendingMap();
		for (int i = 0; i < size; i++) {
			Float key = (Float) nmap.keySet().toArray()[i];
			String value = nmap.get(key);
			eodStories.add(value);
		}
		log.info("Result size of getHigestCTRArticles: " + eodStories.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result:" + eodStories);
		return eodStories;
	}

	public List<String> getMaxPVSstories(WisdomQuery query) {

		long startTime = System.currentTimeMillis();
		ArrayList<String> storyListresult = new ArrayList<>();
		try {
			BoolQueryBuilder bqb = new BoolQueryBuilder();
			String pubTimefrom = DateUtil.addHoursToCurrentTime(-12);
			String dateTimefrom = DateUtil.addHoursToCurrentTime(-4);
			String pubTimeTo = DateUtil.getCurrentDateTime();

			if (query.getHost() != null) {
				List<Integer> hosts = query.getHost();
				bqb.must(QueryBuilders.termsQuery(Constants.HOST, hosts));
			}

			bqb.must(QueryBuilders.rangeQuery(Constants.STORY_MODIFIED_TIME).from(pubTimefrom).to(pubTimeTo));
			bqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(dateTimefrom)
					.to(DateUtil.getCurrentDateTime()));

			String indexname = "realtime_" + DateUtil.getCurrentDate();
			SearchResponse ser = client.prepareSearch(indexname).setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb)
					.setSize(query.getCount())
					.addAggregation(
							AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD).size(query.getCount()))
					.setSize(0).execute().actionGet();

			Terms storyAgg = ser.getAggregations().get("storyid");
			for (Terms.Bucket storyBucket : storyAgg.getBuckets()) {
				storyListresult.add(storyBucket.getKeyAsString());
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getMaximumPVS.", e);
		}
		log.info("Retrieving getMaximumPVS; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + "size" + storyListresult.size());

		return storyListresult;
	}

	public Map<String, Object> getNotificationList(NotificationQuery notificationQuery) {
		Map<String, Object> response = new HashMap<>();
		QueryBuilder queryBuilder = notificationQuery.getQueryBuilder();
		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse(Indexes.NOTIFICATIONS,
				MappingTypes.MAPPING_REALTIME, queryBuilder, notificationQuery.getFrom(), notificationQuery.getSize(),
				notificationQuery.getSort(), notificationQuery.getIncludeFields(), notificationQuery.getExcludeFields(),
				null, null);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		List<Map<String, Object>> data = Arrays.stream(searchHits)
				.map(searchHitFields -> searchHitFields.getSourceAsMap()).collect(Collectors.toList());
		response.put("count", searchResponse.getHits().getTotalHits());
		response.put("data", data);
		return response;
	}

	public static void main(String[] args) throws Exception {
		// UserPersonalizationQuery query = new UserPersonalizationQuery();
		// CommentQuery dbc = new CommentQuery();
		QueryExecutorService qes = new QueryExecutorService();
		WisdomQuery wquery = new WisdomQuery();
		/*
		 * qes.esToHadoopGenerateDataForHDFS(args[0]); System.exit(1);
		 * 
		 * ArrayList<Integer> host = new ArrayList<>(); host.add(5);
		 * 
		 * // query.setSession_id("430c23aa-2afd-8efe-1396-f90a810fb0cc");
		 * query.setSession_id("577abadc-742d-2cef-b0af-4fa6f14bf610");
		 * query.setHosts("15"); // query.setCount(15); query.setFirstTimeFlag(1);
		 * query.setInCount(2);
		 * 
		 * ArticleParams qe = new ArticleParams(); List<String> tracker = new
		 * ArrayList<>(); tracker.add("news-hf"); // tracker.add("news-ht"); //
		 * qe.setTopic("Neymar,T.R. Zeliang,Mumbai,Venezuela,San //
		 * Antonio,Texas,Venkaiah Naidu,Thor: Ragnarok,Pranab Mukherjee,India // vs.
		 * England‬"); qe.setHosts("15"); // query.setSession_id("hanish"); //
		 * qe.setCount(10); qe.setCatId("6724"); // qe.setTracker(tracker); //
		 * qe.setExcludeTracker(tracker); // qe.setExcludeStories("119857727"); //
		 * qe.setInputStoriesId(
		 * "119090827,119399874,119026058,119164866,119136594,119083072,119135769,118976364,119008878"
		 * ); // qe.setCount(4); qe.setInCount(6); // wqes.setHost(host); //
		 * wqes.setCount(6); //query.setStoryid("120799811"); dbc.setPost_id("0001");
		 * dbc.setChannel_slno("512"); dbc.setFrom(0); dbc.setSize(10); //
		 * dbc.setId("9c92644af02dee6ef8b6856716351d37");
		 */ // System.out.println(qes.calculateTimeSpent(Arrays.asList("120799811"),
			// false));
		// qes.getDbComments(dbc);
		// wquery.setStartDate("2017-12-07");
		// wquery.setEndDate("2017-12-07");
		wquery.setStoryid("121285499");
		System.out.println(qes.getAvgTimeSpent(wquery));

	}

}
