package com.db.recommendation.jobs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;

public class DailyUserVisitsIndexService {

	private Client client = null;

	private int countRecordsToIndex = 0;

	private int dayCountForSummaryCalculation = 7;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(DailyUserVisitsIndexService.class);

	private int numThreads = 6;

	public DailyUserVisitsIndexService(Integer countRecordsToIndex, Integer dayCountForSummaryCalculation,
			Integer numThreads) {
		this.countRecordsToIndex = countRecordsToIndex;
		this.dayCountForSummaryCalculation = dayCountForSummaryCalculation;
		this.numThreads = numThreads;
		try {

			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public DailyUserVisitsIndexService() {
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public void initiateTasks() {
		long startTime = System.currentTimeMillis();
		String date = DateUtil.getPreviousDate(DateUtil.getCurrentDate());

		date = date.replace("_", "-");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			// Start of unique users calculation
			Date indexDate = (Date) sdf.parse(date);
			String strIndexDate = sdf.format(new Date(indexDate.getTime()));
			Set<String> session_id_set = new HashSet<String>();
			int startFrom = 0;
			int numRec = 1000;
			String realtime_index = "realtime_" + strIndexDate.replace("-", "_");
			// log.info("searching in index of date " + strIndexDate
			// + " strIndexDate: " + realtime_index);
			log.info("Index name to identify unique users: " + realtime_index);
			startFrom = 0;


			SearchResponse scrollResp = client.prepareSearch(realtime_index)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setScroll(new TimeValue(60000))
					.setQuery(QueryBuilders.boolQuery()

					.mustNot(QueryBuilders.termsQuery(Constants.TRACKER,"news-ucb",Constants.APP)))
					.setSize(numRec).execute().actionGet();

			while (true) {
				startFrom += scrollResp.getHits().getHits().length;
				log.info("Hit size to process : " + scrollResp.getHits().getHits().length);
				log.info("Index: " + realtime_index + " total: " + scrollResp.getHits().getTotalHits()
						+ ", Fetched Hits: " + scrollResp.getHits().getHits().length + " , Total fetched : "
						+ startFrom);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						String session_id = (String)hit.getSource().get("session_id");
						
						if(StringUtils.isNotBlank(session_id)){
							session_id_set.add(session_id);
						}
					} catch (Exception e) {
						log.error("Error getting session id");
						continue;
					}
				}
				if (countRecordsToIndex > 0 && session_id_set.size() > countRecordsToIndex) {
					break;
				}
				log.info("Unique users: " + session_id_set.size());
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
			// End of unique users calculation

			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
			ArrayList<String> session_id_lst = new ArrayList<String>();
			session_id_lst.addAll(session_id_set);
			log.info("-----------------------------------------------");
			log.info("Unique users calculation completed. Count: " + session_id_lst.size());
			log.info("Total Execution time(Minutes)to derive unique users : "
					+ (System.currentTimeMillis() - startTime) / (1000 * 60));
			int lstSize = session_id_lst.size();
			int range_diff = (lstSize / numThreads);
			int start_range = 0;
			int end_range = range_diff;

			Date dateNDaysBefore = new Date(
					((Date) sdf.parse(date)).getTime() - (dayCountForSummaryCalculation - 1) * 24 * 3600 * 1000l);
			String startDate = sdf.format(dateNDaysBefore);
			String endDate = date;

			String[] indexes = IndexUtils.getIndexes(startDate, endDate);
			log.info("List of indexes to calculate stats: " + indexes);
			for (int i = 0; i < numThreads; i++) {
				log.info("Thread " + i);
				log.info("Start Range: " + start_range);
				log.info("End Range: " + end_range);
				executorService
						.execute(new SummaryDeriveRunnable(i, session_id_lst.subList(start_range, end_range - 1)));
				start_range = end_range;
				end_range = end_range + range_diff;
			}
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long endTime = System.currentTimeMillis();
			log.info("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));
		} catch (ParseException e) {
			log.error(e);
			throw new DBAnalyticsException("Invalid date format." + e.getMessage());
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public class SummaryDeriveRunnable implements Runnable {
		private final List<String> sessionIds;

		private String indexNamePrevDate;
		private String prevDateIndexSuffix = DateUtil.getPreviousDate();
		private String prevDate = prevDateIndexSuffix.replaceAll("_", "-");
		private String startDate = DateUtil.getPreviousDate(DateUtil.getPreviousDate(), -dayCountForSummaryCalculation)
				.replaceAll("_", "-");

		int threadIndex = 0;

		int batchSize = 1000;

		SummaryDeriveRunnable(int threadIndex, List<String> sessionIds) {
			this.sessionIds = sessionIds;
			this.threadIndex = threadIndex;
		}

		@Override
		public void run() {
			log.info("Executing thread " + threadIndex);
			try {
				indexNamePrevDate = "realtime_" + DateUtil.getPreviousDate();
				List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
				AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
				long dayWiseThreadRecordCount = 1;
				long startTimeDayWiseSummary = System.currentTimeMillis();

				// START: Derive user summary day_wise
				sessionid_loop: for (String session_id : sessionIds) {
					dayWiseThreadRecordCount = dayWiseThreadAtomicInt.getAndIncrement();
					try {

						// ------------- For getting top 5 PCat_ID
						SearchResponse sr = client.prepareSearch(indexNamePrevDate)
								.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
								.setTimeout(new TimeValue(2000))
								.setQuery(QueryBuilders.boolQuery()
										.must(QueryBuilders.matchQuery(Constants.SESSION_ID_FIELD, session_id))
										.mustNot(QueryBuilders.matchQuery(Constants.TRACKER, Constants.APP)))
								.setSize(20).addSort(Constants.DATE_TIME_FIELD, SortOrder.ASC)
								.addAggregation(AggregationBuilders.terms("TopPCID")
										.field(Constants.PARENT_CAT_ID_FIELD).size(5))
								.addAggregation(
										AggregationBuilders.terms("TopCID").field(Constants.CAT_ID_FIELD).size(5))
								.addAggregation(AggregationBuilders.cardinality("SESSION_COUNT_AGG")
										.field(Constants.SESS_ID_FIELD))
								.addAggregation(AggregationBuilders.terms("TopSection")
										.field(Constants.SECTION).size(6))
								.addAggregation(AggregationBuilders.terms("TopSuperCat")
										.field(Constants.SUPER_CAT_ID).size(6))
								.addAggregation(AggregationBuilders.terms("EVENT")
										.field(Constants.EVENT).size(7))
								.addAggregation(AggregationBuilders.terms("LOCATION")
										.field(Constants.LOCATION).size(7))
								.addAggregation(AggregationBuilders.terms("ORGANIZATION")
										.field(Constants.ORGANIZATION).size(7))
								.execute().actionGet();

						long storiesViewed = sr.getHits().getTotalHits();

						Map<String, Object> record = new HashMap<>();

						// Get top 5 pcat_id
						Terms pcidsResult = sr.getAggregations().get("TopPCID");
						List<Integer> pcatIds = new ArrayList<>();
						for (Terms.Bucket entry : pcidsResult.getBuckets()) {
							if (entry.getKey() != null && !( entry.getKeyAsString()).equalsIgnoreCase("0")) {
								pcatIds.add(Integer.valueOf( entry.getKeyAsString()));
							}
						}
						record.put(Constants.PARENT_CAT_ID_FIELD, pcatIds);

						// Get top 5 cat_id
						Terms cidsResult = sr.getAggregations().get("TopCID");
						List<Integer> catIds = new ArrayList<>();
						for (Terms.Bucket entry : cidsResult.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).equalsIgnoreCase("0")) {
								catIds.add(Integer.valueOf(entry.getKeyAsString()));
							}
						}
						record.put(Constants.CAT_ID_FIELD, catIds);
						
						// Get top 5 section
						Terms sectionsResult = sr.getAggregations().get("TopSection");
						List<String> sections = new ArrayList<>();
						for (Terms.Bucket entry : sectionsResult.getBuckets()) {
							if (StringUtils.isNotBlank((String)entry.getKey())) {
								sections.add(entry.getKeyAsString());
							}
						}
						record.put(Constants.SECTION, sections);
						
						// Get top 5 super_cat_id
						Terms superCatResults = sr.getAggregations().get("TopSuperCat");
						List<String> superCats = new ArrayList<>();
						for (Terms.Bucket entry : superCatResults.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).equalsIgnoreCase("0")) {
								superCats.add(entry.getKeyAsString());
							}
						}
						record.put(Constants.SUPER_CAT_ID, superCats);
						
						// Get top 5 event
						Terms eventResults = sr.getAggregations().get("EVENT");
						List<String> events = new ArrayList<>();
						for (Terms.Bucket entry : eventResults.getBuckets()) {
							if (StringUtils.isNotBlank((String)entry.getKey())){
								events.add(entry.getKeyAsString());
							}
						}
						record.put(Constants.EVENT, events);
						
						// Get top 5 location
						Terms locationResults = sr.getAggregations().get("LOCATION");
						List<String> locations = new ArrayList<>();
						for (Terms.Bucket entry : locationResults.getBuckets()) {
							if (StringUtils.isNotBlank((String)entry.getKey())){
								locations.add(entry.getKeyAsString());
							}
						}
						record.put(Constants.LOCATION, locations);
						
						// Get top 5 organization
						Terms organizationResults = sr.getAggregations().get("ORGANIZATION");
						List<String> organizations = new ArrayList<>();
						for (Terms.Bucket entry : organizationResults.getBuckets()) {
							if (StringUtils.isNotBlank((String)entry.getKey())){
								organizations.add(entry.getKeyAsString());
							}
						}
						record.put(Constants.ORGANIZATION, organizations);

						// Get session_count of a user
						Cardinality cardinality = sr.getAggregations().get("SESSION_COUNT_AGG");
						long sessionCount = cardinality.getValue();
						record.put(Constants.SESSION_COUNT, sessionCount);

						record.put(Constants.STORY_COUNT, storiesViewed);
						record.put(Constants.ROWID, session_id + "_" + prevDateIndexSuffix);
						record.put(Constants.SESSION_ID_FIELD, session_id);
						record.put(Constants.DATE_TIME_FIELD, prevDate);

						SearchHit[] searchHits = sr.getHits().getHits();

						// Retrieve user location information
						if (searchHits.length > 0) {
							SearchHit hit = searchHits[0];
							if (hit.getSource().get(Constants.CITY) != null) {
								record.put(Constants.CITY, hit.getSource().get(Constants.CITY));
							}
							if (hit.getSource().get(Constants.STATE) != null) {
								record.put(Constants.STATE, hit.getSource().get(Constants.STATE));
							}
							if (hit.getSource().get(Constants.COUNTRY) != null) {
								record.put(Constants.COUNTRY, hit.getSource().get(Constants.COUNTRY));
							}
						}

						// Retrieve stories, hosts and session_timestamp details
						List<Integer> storyIds = new ArrayList<Integer>();
						Set<Integer> hosts = new HashSet<Integer>();
						Map<String, String> sessIdWithTimestamp = new HashMap<>();

						Map<Object, Integer> refPlatformOccuranceMap = new HashMap<>();
						Map<Object, Integer> refHostOccuranceMap = new HashMap<>();

						//Map<Object, Integer> sectionOccuranceMap = new HashMap<>();
						
						Map<Object, Integer> trackerOccuranceMap = new HashMap<>();

						for (SearchHit searchHit : searchHits) {
							Map<String, Object> searchHitRecord = searchHit.getSource();
							// Derive session timestamp of user
							if (sessionCount > 0) {
								String sessId = (String) searchHitRecord.get(Constants.SESS_ID_FIELD);
								if (StringUtils.isNotBlank(sessId) && !sessIdWithTimestamp.containsKey(sessId)) {
									sessIdWithTimestamp.put(sessId,
											(String) searchHitRecord.get(Constants.DATE_TIME_FIELD));
								}
							}
							String refPlatform = (String) searchHitRecord.get(Constants.REF_PLATFORM);
							if (StringUtils.isNotBlank(refPlatform)) {
								if (!refPlatformOccuranceMap.containsKey(refPlatform)) {
									refPlatformOccuranceMap.put(refPlatform, 1);
								} else {
									refPlatformOccuranceMap.put(refPlatform,
											refPlatformOccuranceMap.get(refPlatform) + 1);
								}
							}

							String refHost = (String) searchHitRecord.get(Constants.REF_HOST);
							if (StringUtils.isNotBlank(refHost)) {
								if (!refHostOccuranceMap.containsKey(refHost)) {
									refHostOccuranceMap.put(refHost, 1);
								} else {
									refHostOccuranceMap.put(refHost, refHostOccuranceMap.get(refHost) + 1);
								}
							}

							/*String section = (String) searchHitRecord.get(Constants.SECTION);
							if (!sectionOccuranceMap.containsKey(section)) {
								sectionOccuranceMap.put(section, 1);
							} else {
								sectionOccuranceMap.put(section, sectionOccuranceMap.get(section) + 1);
							}*/

							String tracker = (String) searchHitRecord.get(Constants.TRACKER);
							if (StringUtils.isNotBlank(tracker)) {
								if (!trackerOccuranceMap.containsKey(tracker)) {
									trackerOccuranceMap.put(tracker, 1);
								} else {
									trackerOccuranceMap.put(tracker, trackerOccuranceMap.get(tracker) + 1);
								}
							}
							
							storyIds.add(
									Integer.valueOf((String) searchHit.getSource().get(Constants.STORY_ID_FIELD)));
							hosts.add((Integer) searchHit.getSource().get(Constants.HOST));
						}

						if (refPlatformOccuranceMap.size() > 0) {
							record.put(Constants.REF_PLATFORM, getTop5Element(refPlatformOccuranceMap));
						}
						/*if (sectionOccuranceMap.size() > 0) {
							record.put(Constants.SECTION, getTop5Element(sectionOccuranceMap));
						}*/
						if (refHostOccuranceMap.size() > 0) {
							record.put(Constants.REF_HOST, getTop5Element(refHostOccuranceMap));
						}
						
						if (trackerOccuranceMap.size() > 0) {
							record.put(Constants.TRACKER, getTop5Element(trackerOccuranceMap));
						}
						
						record.put(Constants.SESSION_TIMESTAMP, sessIdWithTimestamp.values());
						record.put(Constants.STORIES, storyIds);
						record.put(Constants.HOST, hosts);

						listWeightage.add(record);
						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
									listWeightage);
							log.info("Thread " + threadIndex + " Day wise summary; Total Users: "
									+ sessionIds.size() + "; Users Processed: " + dayWiseThreadRecordCount);
							listWeightage.clear();
						}
					} catch (Exception e) {
						e.printStackTrace();
						log.info(
								"Error occured in getting profile for session id: " + session_id + " exception " + e);
						continue sessionid_loop;
					}
				}

				// Index remaining data if there after completing loop
				if (listWeightage.size() > 0) {

					elasticSearchIndexService.index(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
							listWeightage);
					log.info("Thread " + threadIndex + " Day wise summary; Total Users: " + sessionIds.size()
							+ "; Users Processed: " + dayWiseThreadRecordCount);
					listWeightage.clear();
				}
				log.info("Total Execution time of Day Wise Summary(Minutes) : "
						+ (System.currentTimeMillis() - startTimeDayWiseSummary) / (1000 * 60));
						// END: Derive user summary day_wise

				/*
				 * =============================================================
				 * User Consolidated summary JOB
				 * =============================================================
				 */
				List<Map<String, Object>> userFinalSummaryList = new ArrayList<Map<String, Object>>();

				AtomicInteger finalSummaryThreadAtomicInt = new AtomicInteger(1);
				long finalSummaryThreadRecordCount = 1;
				List<String> dateList = DateUtil.getDates(startDate, prevDate);
				// Build consolidated profile for given interval
				for (String session_id : sessionIds) {
					try {

						MultiGetRequest multiGetRequest = new MultiGetRequest();
						for (String date : dateList) {
							multiGetRequest.add(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
									session_id + "_" + date);
						}

						finalSummaryThreadRecordCount = finalSummaryThreadAtomicInt.getAndIncrement();

						MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();

						Set<Integer> storyIds = new HashSet<>();
						Set<Integer> hosts = new HashSet<>();
						long storiesViewed = 0;
						Map<Integer, Integer> categoryOccuranceMap = new HashMap<>();
						Map<Integer, Integer> parentCategoryOccuranceMap = new HashMap<>();
						Map<Object, Integer> sectionOccuranceMap = new HashMap<>();
						Map<Object, Integer> superCatOccuranceMap = new HashMap<>();
						Map<Object, Integer> eventOccuranceMap = new HashMap<>();
						Map<Object, Integer> locationOccuranceMap = new HashMap<>();
						Map<Object, Integer> organizationOccuranceMap = new HashMap<>();

						int sessionCount = 0;
						Map<String, Object> record = new HashMap<>();
						for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
							if (multiGetItemResponse.getResponse().isExists()) {
								Map<String, Object> hitAsMap = multiGetItemResponse.getResponse().getSourceAsMap();
								if (hitAsMap.get(Constants.CITY) != null) {
									record.put(Constants.CITY, hitAsMap.get(Constants.CITY));
								}
								if (hitAsMap.get(Constants.STATE) != null) {
									record.put(Constants.STATE, hitAsMap.get(Constants.STATE));
								}
								if (hitAsMap.get(Constants.COUNTRY) != null) {
									record.put(Constants.COUNTRY, hitAsMap.get(Constants.COUNTRY));
								}
								List<Integer> categoryList = (List<Integer>) hitAsMap.get(Constants.CAT_ID_FIELD);
								if(categoryList != null){
									for (Integer categoryId : categoryList) {
										if (!categoryOccuranceMap.containsKey(categoryId)) {
											categoryOccuranceMap.put(categoryId, 1);
										} else {
											categoryOccuranceMap.put(categoryId, categoryOccuranceMap.get(categoryId) + 1);
										}
									}
								}

								List<Integer> parentCategoryList = (List<Integer>) hitAsMap
										.get(Constants.PARENT_CAT_ID_FIELD);
								
								if(parentCategoryList != null){
									for (Integer pCategoryId : parentCategoryList) {
										if (!parentCategoryOccuranceMap.containsKey(pCategoryId)) {
											parentCategoryOccuranceMap.put(pCategoryId, 1);
										} else {
											parentCategoryOccuranceMap.put(pCategoryId,
													parentCategoryOccuranceMap.get(pCategoryId) + 1);
										}
									}
								}
								
								
								if(hitAsMap.get(Constants.SECTION) != null){
									List<String> sectionList = (List<String>) hitAsMap
											.get(Constants.SECTION);
									for (String section : sectionList) {
										if (!sectionOccuranceMap.containsKey(section)) {
											sectionOccuranceMap.put(section, 1);
										} else {
											sectionOccuranceMap.put(section,
													sectionOccuranceMap.get(section) + 1);
										}
									}
								}
								
								if(hitAsMap.get(Constants.SUPER_CAT_ID) != null){
									List<String> superCatList = (List<String>) hitAsMap
											.get(Constants.SUPER_CAT_ID);
									for (String superCat : superCatList) {
										if (!superCatOccuranceMap.containsKey(superCat)) {
											superCatOccuranceMap.put(superCat, 1);
										} else {
											superCatOccuranceMap.put(superCat,
													superCatOccuranceMap.get(superCat) + 1);
										}
									}
								}
								
								if(hitAsMap.get(Constants.EVENT) != null){
									List<String> eventList = (List<String>) hitAsMap
											.get(Constants.EVENT);
									for (String event : eventList) {
										if (!eventOccuranceMap.containsKey(event)) {
											eventOccuranceMap.put(event, 1);
										} else {
											eventOccuranceMap.put(event,
													eventOccuranceMap.get(event) + 1);
										}
									}
								}
								
								if(hitAsMap.get(Constants.LOCATION) != null){
									List<String> locationList = (List<String>) hitAsMap
											.get(Constants.LOCATION);
									for (String location : locationList) {
										if (!locationOccuranceMap.containsKey(location)) {
											locationOccuranceMap.put(location, 1);
										} else {
											locationOccuranceMap.put(location,
													locationOccuranceMap.get(location) + 1);
										}
									}
								}
								
								if(hitAsMap.get(Constants.ORGANIZATION) != null){
									List<String> organizationList = (List<String>) hitAsMap
											.get(Constants.ORGANIZATION);
									for (String organization : organizationList) {
										if (!organizationOccuranceMap.containsKey(organization)) {
											organizationOccuranceMap.put(organization, 1);
										} else {
											organizationOccuranceMap.put(organization,
													organizationOccuranceMap.get(organization) + 1);
										}
									}
								}
								
								if(hitAsMap.get(Constants.STORY_COUNT) != null){
									storiesViewed = storiesViewed + (Integer) hitAsMap.get(Constants.STORY_COUNT);
								}
								
								if(hitAsMap.get(Constants.STORIES) != null){
									storyIds.addAll((List<Integer>) hitAsMap.get(Constants.STORIES));
								}
								
								if(hitAsMap.get(Constants.HOST) instanceof List){
									hosts.addAll((List<Integer>) hitAsMap.get(Constants.HOST));
								}else if(hitAsMap.get(Constants.HOST) != null){
									hosts.addAll(Arrays.asList(Integer.parseInt(hitAsMap.get(Constants.HOST).toString())));
								}
								
								if (hitAsMap.containsKey(Constants.SESSION_COUNT)) {
									sessionCount = sessionCount + (Integer) hitAsMap.get(Constants.SESSION_COUNT);
								}

							}
						}

						record.put(Constants.CAT_ID_FIELD, getTopCat(categoryOccuranceMap));
						record.put(Constants.PARENT_CAT_ID_FIELD, getTopCat(parentCategoryOccuranceMap));
						record.put(Constants.SECTION, getTop5Element(sectionOccuranceMap));
						record.put(Constants.SUPER_CAT_ID, getTop5Element(superCatOccuranceMap));
						record.put(Constants.EVENT, getTop5Element(eventOccuranceMap));
						record.put(Constants.LOCATION, getTop5Element(locationOccuranceMap));
						record.put(Constants.ORGANIZATION, getTop5Element(organizationOccuranceMap));

						record.put(Constants.STORY_COUNT, storiesViewed);
						record.put(Constants.ROWID, session_id);
						record.put(Constants.DEVICEID, session_id);
						record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						record.put(Constants.STORIES, storyIds);
						record.put(Constants.HOST, hosts);

						record.put(Constants.SESSION_COUNT, sessionCount);
						userFinalSummaryList.add(record);

						if (userFinalSummaryList.size() == batchSize) {
							elasticSearchIndexService.indexOrUpdate(Indexes.USER_PERSONALIZATION_STATS,
									MappingTypes.MAPPING_REALTIME, userFinalSummaryList);

							log.info("Thread " + threadIndex + " consolidated summary; Total Users: "
									+ sessionIds.size() + "; Users Processed: " + finalSummaryThreadRecordCount);
							userFinalSummaryList.clear();
						}

					} catch (Exception e) {
						e.printStackTrace();
						System.err.println("Error occured in consolidating profile for session id: " + session_id
								+ ", Exception " + e.getMessage());
						continue;
					}
				}
				// Index remaining data if there after completing loop
				if (userFinalSummaryList.size() > 0) {
					elasticSearchIndexService.indexOrUpdate(Indexes.USER_PERSONALIZATION_STATS,
							MappingTypes.MAPPING_REALTIME, userFinalSummaryList);
					log.info("Thread " + threadIndex + " consolidated summary; Total Users: "
							+ sessionIds.size() + "; Users Processed: " + finalSummaryThreadRecordCount);
					userFinalSummaryList.clear();
				}

				log.info("User Profiler completed.");

			} catch (Exception e) {
				e.printStackTrace();
				throw new DBAnalyticsException("Error getting personanilzation stats for last date." + e.getMessage());
			}

		}

		/**
		 * Sort map by values and give 5 keys based having highest values
		 * 
		 * @param map
		 * @return
		 */
		@SuppressWarnings("unchecked")
		private List<Integer> getTopCat(Map<Integer, Integer> map) {
			List<Integer> result = new ArrayList<>();
			Object[] a = map.entrySet().toArray();
			Arrays.sort(a, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<Integer, Integer>) o2).getValue()
							.compareTo(((Map.Entry<Integer, Integer>) o1).getValue());
				}
			});
			for (Object e : a) {
				// log.info(((Map.Entry<Integer, Integer>) e).getKey()
				// + " : "
				// + ((Map.Entry<Integer, Integer>) e).getValue());
				result.add(((Map.Entry<Integer, Integer>) e).getKey());
				if (result.size() >= 5) {
					break;
				}
			}
			return result;
		}
		
		private List<Object> getTop5Element(Map<Object, Integer> map) {
			List<Object> result = new ArrayList<>();
			Object[] a = map.entrySet().toArray();
			Arrays.sort(a, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<Object, Integer>) o2).getValue()
							.compareTo(((Map.Entry<Object, Integer>) o1).getValue());
				}
			});
			for (Object e : a) {
				// log.info(((Map.Entry<Integer, Integer>) e).getKey()
				// + " : "
				// + ((Map.Entry<Integer, Integer>) e).getValue());
				result.add(((Map.Entry<Object, Integer>) e).getKey());
				if (result.size() >= 5) {
					break;
				}
			}
			return result;
		}
	}

	public static void main(String[] args) {
		DailyUserVisitsIndexService duvi = new DailyUserVisitsIndexService(Integer.valueOf(args[0]),
				Integer.valueOf(args[1]), Integer.valueOf(args[2]));
		
		duvi.initiateTasks();
	}

}