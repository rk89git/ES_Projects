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

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;

public class RecUserProfileService {

	private Client client = null;

	private int countRecordsToIndex = 0;

	private int dayCountForSummaryCalculation = 7;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(RecUserProfileService.class);

	private int numThreads = 6;

	public RecUserProfileService(Integer countRecordsToIndex, Integer dayCountForSummaryCalculation,
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

	public RecUserProfileService() {
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
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
			String realtime_index = Constants.REC_DAILY_LOG_INDEX_PREFIX + strIndexDate.replace("-", "_");
			// System.out.println("searching in index of date " + strIndexDate
			// + " strIndexDate: " + realtime_index);
			System.out.println("Index name to identify unique users: " + realtime_index);
			startFrom = 0;

			SearchResponse scrollResp = client.prepareSearch(realtime_index)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setScroll(new TimeValue(60000))
					.setQuery(QueryBuilders.matchAllQuery())
					.setSize(numRec).execute().actionGet();

			while (true) {
				startFrom += scrollResp.getHits().getHits().length;
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				System.out.println("Index: " + realtime_index + " total: " + scrollResp.getHits().getTotalHits()
						+ ", Fetched Hits: " + scrollResp.getHits().getHits().length + " , Total fetched : "
						+ startFrom);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						String session_id = (String) hit.getSource().get("session_id");
						session_id_set.add(session_id);
					} catch (Exception e) {
						log.error("Error getting session id");
						continue;
					}
				}
				if (countRecordsToIndex > 0 && session_id_set.size() > countRecordsToIndex) {
					break;
				}
				System.out.println("Unique users: " + session_id_set.size());
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
			System.out.println("-----------------------------------------------");
			System.out.println("Unique users calculation completed. Count: " + session_id_lst.size());
			System.out.println("Total Execution time(Minutes)to derive unique users : "
					+ (System.currentTimeMillis() - startTime) / (1000 * 60));
			int lstSize = session_id_lst.size();
			if (lstSize>0) {
				int range_diff = (lstSize / numThreads);
				int start_range = 0;
				int end_range = range_diff;

				Date dateNDaysBefore = new Date(
						((Date) sdf.parse(date)).getTime() - (dayCountForSummaryCalculation - 1) * 24 * 3600 * 1000l);
				String startDate = sdf.format(dateNDaysBefore);
				String endDate = date;

				String[] indexes = IndexUtils.getIndexes(startDate, endDate);
				System.out.println("List of indexes to calculate stats: " + indexes);
				for (int i = 0; i < numThreads; i++) {
					System.out.println("Thread " + i);
					System.out.println("Start Range: " + start_range);
					System.out.println("End Range: " + end_range);
					executorService
							.execute(new SummaryDeriveRunnable(i, session_id_lst.subList(start_range, end_range - 1)));
					start_range = end_range;
					end_range = end_range + range_diff;
				}
			} else {
				System.out.println("No Record to be processed.");
			}
			
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long endTime = System.currentTimeMillis();
			System.out.println("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));
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

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			System.out.println("Executing thread " + threadIndex);
			try {
				indexNamePrevDate = Constants.REC_DAILY_LOG_INDEX_PREFIX + DateUtil.getPreviousDate();
				List<Map<String, Object>> listDayWiseUserProfile = new ArrayList<Map<String, Object>>();
				AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
				long dayWiseThreadRecordCount = 1;
				long startTimeDayWiseSummary = System.currentTimeMillis();

				// START: Derive user summary day_wise
				sessionid_loop: for (String session_id : sessionIds) {
					dayWiseThreadRecordCount = dayWiseThreadAtomicInt.getAndIncrement();
					try {

						// ------------- For getting top 5 sections
						SearchResponse sr = client.prepareSearch(indexNamePrevDate)
								.setTypes(MappingTypes.MAPPING_REALTIME)
								.setTimeout(new TimeValue(2000))
								.setQuery(QueryBuilders.boolQuery()
										.must(QueryBuilders.matchQuery(Constants.SESSION_ID_FIELD, session_id)))
								.setSize(20)
								.addAggregation(
										AggregationBuilders.terms("TopSection").field(Constants.SECTION).size(5))
								.execute().actionGet();

						long storiesViewed = sr.getHits().getTotalHits();

						Map<String, Object> record = new HashMap<String, Object>();

						// Get top 5 language
						/*Terms topLanguageResult = sr.getAggregations().get("TopLanguage");
						List<String> languageList = new ArrayList<String>();
						for (Terms.Bucket entry : topLanguageResult.getBuckets()) {
							if (entry.getKey() != null) {
								languageList.add(entry.getKey());
							}
						}
						record.put(Constants.LANGUAGE, languageList);*/
						
						// Get top 5 section
						Terms topSectionResult = sr.getAggregations().get("TopSection");
						List<String> sectionList = new ArrayList<String>();
						for (Terms.Bucket entry : topSectionResult.getBuckets()) {
							if (entry.getKey() != null ) {
								sectionList.add((String) entry.getKey());
							}
						}
						record.put(Constants.SECTION, sectionList);

				

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
							if (hit.getSource().get(Constants.LANGUAGE) != null) {
								record.put(Constants.LANGUAGE, hit.getSource().get(Constants.LANGUAGE));
							}
						}

						// Retrieve stories details
						Set<String> storyIds = new HashSet<String>();
						Set<String> brandIds = new HashSet<String>();
						Set<String> brandNames = new HashSet<String>();
						Set<String> deviceTypes = new HashSet<String>();
						
						for (SearchHit searchHit : searchHits) {
							storyIds.add((String) searchHit.getSource().get(Constants.STORY_ID));
							// Retrieve brand id details
							brandIds.add((String) searchHit.getSource().get(Constants.BRAND_ID));
							// Retrieve brand name details
							brandNames.add((String) searchHit.getSource().get(Constants.BRAND_NAME));
							// Retrieve device type
							deviceTypes.add((String) searchHit.getSource().get(Constants.DEVICE_TYPE));
						}
						
						record.put(Constants.STORY_ID, storyIds);
						record.put(Constants.BRAND_ID, brandIds);
						record.put(Constants.BRAND_NAME, brandNames);
						record.put(Constants.DEVICE_TYPE, deviceTypes);

						listDayWiseUserProfile.add(record);
						if (listDayWiseUserProfile.size() == batchSize) {
							elasticSearchIndexService.index(Indexes.RECOMMENDATION_USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
									listDayWiseUserProfile);
							System.out.println("Thread " + threadIndex + " Day wise summary; Total Users: "
									+ sessionIds.size() + "; Users Processed: " + dayWiseThreadRecordCount);
							listDayWiseUserProfile.clear();
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(
								"Error occured in getting profile for session id: " + session_id + " exception " + e);
						continue sessionid_loop;
					}
				}

				// Index remaining data if there after completing loop
				if (listDayWiseUserProfile.size() > 0) {

					elasticSearchIndexService.index(Indexes.RECOMMENDATION_USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
							listDayWiseUserProfile);
					System.out.println("Thread " + threadIndex + " Day wise summary; Total Users: " + sessionIds.size()
							+ "; Users Processed: " + dayWiseThreadRecordCount);
					listDayWiseUserProfile.clear();
				}
				System.out.println("Total Execution time of Day Wise Summary(Minutes) : "
						+ (System.currentTimeMillis() - startTimeDayWiseSummary) / (1000 * 60));
						// END: Derive user summary day_wise

				/*
				 * =============================================================
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
							multiGetRequest.add(Indexes.RECOMMENDATION_USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
									session_id + "_" + date);
						}

						finalSummaryThreadRecordCount = finalSummaryThreadAtomicInt.getAndIncrement();

						MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();

						Set<String> storyIds = new HashSet<String>();
						Set<String> brandIds = new HashSet<String>();
						Set<String> brandNames = new HashSet<String>();
						Set<String> deviceTypes = new HashSet<String>();
						
						long storiesViewed = 0;
						Map<String, Integer> sectionOccuranceMap = new HashMap<>();
						int sessionCount = 0;

						Map<String, Object> record = new HashMap<String, Object>();
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
								if (hitAsMap.get(Constants.LANGUAGE) != null) {
									record.put(Constants.LANGUAGE, hitAsMap.get(Constants.LANGUAGE));
								}
								
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
								storiesViewed = storiesViewed + (Integer) hitAsMap.get(Constants.STORY_COUNT);
								storyIds.addAll((List<String>) hitAsMap.get(Constants.STORY_ID));
								brandIds.addAll((List<String>) hitAsMap.get(Constants.BRAND_ID));
								brandNames.addAll((List<String>) hitAsMap.get(Constants.BRAND_NAME));
								deviceTypes.addAll((List<String>) hitAsMap.get(Constants.DEVICE_TYPE));
							}
						}

						record.put(Constants.SECTION, getTopFiveItems(sectionOccuranceMap));

						record.put(Constants.STORY_COUNT, storiesViewed);
						record.put(Constants.ROWID, session_id);
						record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						record.put(Constants.STORY_ID, storyIds);
						record.put(Constants.BRAND_ID, brandIds);
						record.put(Constants.BRAND_NAME, brandNames);
						record.put(Constants.DEVICE_TYPE, deviceTypes);
						userFinalSummaryList.add(record);

						if (userFinalSummaryList.size() == batchSize) {
							elasticSearchIndexService.indexOrUpdate(Indexes.RECOMMENDATION_USER_PROFILE_STATS,
									MappingTypes.MAPPING_REALTIME, userFinalSummaryList);

							System.out.println("Thread " + threadIndex + " consolidated summary; Total Users: "
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
					elasticSearchIndexService.indexOrUpdate(Indexes.RECOMMENDATION_USER_PROFILE_STATS,
							MappingTypes.MAPPING_REALTIME, userFinalSummaryList);
					System.out.println("Thread " + threadIndex + " consolidated summary; Total Users: "
							+ sessionIds.size() + "; Users Processed: " + finalSummaryThreadRecordCount);
					userFinalSummaryList.clear();
				}

				System.out.println("User Profiler completed.");

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
		private List<String> getTopFiveItems(Map<String, Integer> map) {
			List<String> result = new ArrayList<>();
			Object[] a = map.entrySet().toArray();
			Arrays.sort(a, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<String, Integer>) o2).getValue()
							.compareTo(((Map.Entry<String, Integer>) o1).getValue());
				}
			});
			for (Object e : a) {
				// System.out.println(((Map.Entry<Integer, Integer>) e).getKey()
				// + " : "
				// + ((Map.Entry<Integer, Integer>) e).getValue());
				result.add(((Map.Entry<String, Integer>) e).getKey());
				if (result.size() >= 5) {
					break;
				}
			}
			return result;
		}
	}

	public static void main(String[] args) {
		if (args.length!=3) {
			System.err.println(
					"Usage: java -jar <path/to/RecUserProfileCreator.jar> <NUMBER OF RECORDS TO PROCESSED> <DAY COUNT> <THREAD COUNT>");
			System.exit(0);
		}
//		int usercount=1000;
//		int day=4;
//		int threadnumber=6;
		RecUserProfileService duvi = new RecUserProfileService(Integer.valueOf(args[0]), Integer.valueOf(args[1]),
				Integer.valueOf(args[2]));
		duvi.initiateTasks();
	}

}