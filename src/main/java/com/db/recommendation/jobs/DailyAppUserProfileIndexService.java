package com.db.recommendation.jobs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

public class DailyAppUserProfileIndexService {

	private static Logger log = LogManager.getLogger(DailyAppUserProfileIndexService.class);
	private Client client = null;
	private int countRecordsToIndex = 0;
	private int dayCountForSummaryCalculation = 7;
	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	private int numThreads = 5;

	public DailyAppUserProfileIndexService(Integer countRecordsToIndex, Integer dayCountForSummaryCalculation) {
		this.countRecordsToIndex = countRecordsToIndex;
		this.dayCountForSummaryCalculation = dayCountForSummaryCalculation;
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public DailyAppUserProfileIndexService() {
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

	public void run() {
		String strIndexDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate());
		long startTime = System.currentTimeMillis();
		String date = strIndexDate.replace("_", "-");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			// Start of unique users calculation
			Set<String> deviceIdList = new HashSet<String>();
			int startFrom = 0;
			String realtimeIndex = "realtime_" + strIndexDate;
			log.info("searching  in index of date " + strIndexDate + " strIndexDate: " + realtimeIndex);
			log.info("searching  in index of date " + strIndexDate + " strIndexDate: " + realtimeIndex);
			startFrom = 0;

			SearchResponse scrollResp = client.prepareSearch(realtimeIndex)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
					.setQuery(QueryBuilders.matchQuery(Constants.TRACKER, Constants.APP)).setScroll(new TimeValue(60000))
					//.setSearchType(SearchType.SCAN).
					.addSort("_doc",SortOrder.ASC)
					.setSize(1000).execute()
					.actionGet();

			while (true) {
				startFrom += scrollResp.getHits().getHits().length;
				log.info("Hit size to process : " + scrollResp.getHits().getHits().length);
				log.info("index: " + realtimeIndex + " total: " + scrollResp.getHits().getTotalHits()
						+ ", Fetched Hits: " + scrollResp.getHits().getHits().length + " , Total fetched : "
						+ startFrom);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						String deviceId = (String) hit.getSource().get(Constants.SESSION_ID_FIELD);
						if (org.apache.commons.lang.StringUtils.isNotBlank(deviceId)) {
							deviceIdList.add(deviceId);
						}
					} catch (Exception e) {
						log.error("Error getting information", e);
						continue;
					}
				}
				if (countRecordsToIndex > 0 && deviceIdList.size() > countRecordsToIndex) {
					break;
				}
				log.info("Unique users: " + deviceIdList.size());
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
			session_id_lst.addAll(deviceIdList);
			int lstSize = session_id_lst.size();
			log.info("Total Users to Process:" + lstSize);

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
				executorService.execute(
						new SummaryDeriveRunnable(i, session_id_lst.subList(start_range, end_range - 1), indexes));
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
		int totalCount = 0;
		int threadIndex = 0;
		private String indexesForSummary[];

		SummaryDeriveRunnable(int threadIndex, List<String> sessionIds, String indexesForSummary[]) {
			this.sessionIds = sessionIds;
			this.indexesForSummary = indexesForSummary;
			this.threadIndex = threadIndex;
		}

		@Override
		public void run() {
			log.info("Executing thread " + threadIndex);
			long indexedRecordCount = 0;
			List<Map<String, Object>> listMessages = new ArrayList<Map<String, Object>>();
			for (String deviceId : sessionIds) {
				try {
					SearchResponse sr = client.prepareSearch(indexesForSummary)
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
							.setQuery(QueryBuilders.termQuery(Constants.SESSION_ID_FIELD, deviceId))
							.addAggregation(AggregationBuilders.terms("PCAT_ID").field("pcat_id").size(5))
							.addAggregation(AggregationBuilders.terms("CAT_ID").field("cat_id").size(5))

							.addAggregation(AggregationBuilders.terms("PEOPLE").field("people").size(7))
							.addAggregation(AggregationBuilders.terms("EVENT").field("event").size(7))
							.addAggregation(AggregationBuilders.terms("LOCATION").field("location").size(7))
							.addAggregation(AggregationBuilders.terms("ORGANIZATION").field("organization").size(7))
							.addAggregation(AggregationBuilders.terms("HOST").field("host").size(5))
							// .addAggregation(AggregationBuilders.dateHistogram("daily_visits").field("datetime")
							// .interval(86400000)
							// .order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
							// .addAggregation(AggregationBuilders.dateHistogram("visits_over_time").field("datetime")
							// .interval(1800000)
							// .order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC)
							// .format("HH:mm"))
							.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC)
							.setSize(1).execute().actionGet();

					int totalStories = 0;

					Terms result = sr.getAggregations().get("CAT_ID");

					Map<String, Object> record = new HashMap<String, Object>();
					record.put(Constants.ROWID, deviceId);

					// ------------------------------- CAT ID
					// ---------------------------------//
					List<Integer> catIdList = new ArrayList<Integer>();
					for (Terms.Bucket entry : result.getBuckets()) {
						if (entry.getKey() != null && !(entry.getKeyAsString()).equalsIgnoreCase("0")) {
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
								// String host =
								// hit.sourceAsMap().get("host").toString();
								// record.put(Constants.HOST,
								// Integer.valueOf(host));
								if (StringUtils.isNotBlank((String) hit.getSource().get(Constants.CITY))) {
									record.put(Constants.CITY, hit.getSource().get(Constants.CITY));
								}
								if (StringUtils.isNotBlank((String) hit.getSource().get(Constants.STATE))) {
									record.put(Constants.STATE, hit.getSource().get(Constants.STATE));
								}
								if (StringUtils.isNotBlank((String) hit.getSource().get(Constants.COUNTRY))) {
									record.put(Constants.COUNTRY, hit.getSource().get(Constants.COUNTRY));
								}
								record.put(Constants.DEVICEID, hit.getSource().get(Constants.SESSION_ID_FIELD));
								record.put(Constants.DEVICE_TOKEN, hit.getSource().get(Constants.DEVICE_TOKEN));
								record.put(Constants.APP_VERSION, hit.getSource().get(Constants.APP_VERSION));
								record.put(Constants.NotificationConstants.DAY_NOTIFICATION_COUNTER,0);//resetting counter
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
						if (entry.getKey() != null && !(entry.getKeyAsString()).equalsIgnoreCase("0")) {
							pCatIdList.add(Integer.valueOf(entry.getKeyAsString()));
							if (pCatIdList.size() == 5) {
								break;
							}
						}
					}

					record.put("pcat_id", pCatIdList);
					{
						Terms peoples = sr.getAggregations().get("PEOPLE");
						List<String> peopleList = new ArrayList<>();
						for (Terms.Bucket entry : peoples.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
								peopleList.add(entry.getKeyAsString());
								if (peopleList.size() == 5) {
									break;
								}
							}
						}
						record.put(Constants.PEOPLE, peopleList);
					}
					{
						Terms events = sr.getAggregations().get("EVENT");
						List<String> eventList = new ArrayList<>();
						for (Terms.Bucket entry : events.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
								eventList.add(entry.getKeyAsString());
								if (eventList.size() == 5) {
									break;
								}
							}
						}
						record.put(Constants.EVENT, eventList);
					}
					{
						Terms locations = sr.getAggregations().get("LOCATION");
						List<String> locationList = new ArrayList<>();
						for (Terms.Bucket entry : locations.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
								locationList.add( entry.getKeyAsString());
								if (locationList.size() == 5) {
									break;
								}
							}
						}
						record.put(Constants.LOCATION, locationList);
					}
					{
						Terms organizations = sr.getAggregations().get("ORGANIZATION");
						List<String> organizationList = new ArrayList<>();
						for (Terms.Bucket entry : organizations.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
								organizationList.add( entry.getKeyAsString());
								if (organizationList.size() == 5) {
									break;
								}
							}
						}
						record.put(Constants.ORGANIZATION, organizationList);
					}
					{
						Terms hosts = sr.getAggregations().get("HOST");
						List<Integer> hostList = new ArrayList<>();
						for (Terms.Bucket entry : hosts.getBuckets()) {
							if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
								hostList.add(Integer.valueOf(entry.getKeyAsString()));
								if (hostList.size() == 5) {
									break;
								}
							}
						}
						record.put(Constants.HOST, hostList);
					}

					record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					log.info("User profile build for " + deviceId);
					indexedRecordCount++;
					listMessages.add(record);
					if (listMessages.size() == 1000) {
						// TODO: Uncomment below line after few days. - 23rd May
						// elasticSearchIndexService.indexOrUpdateWithScriptAndAppend(Indexes.APP_USER_PROFILE,
						// MappingTypes.MAPPING_REALTIME, listMessages,
						// Constants.NotificationConstants.HOST_FIELD);
						elasticSearchIndexService.indexOrUpdate(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
								listMessages);
						listMessages.clear();
						log.info("Thread " + threadIndex + ", Records indexed in "+Indexes.APP_USER_PROFILE+" index : "
								+ indexedRecordCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
					log.error("Error getting unique visits for device_token_id: " + deviceId + " exception. ", e);
					log.info(
							"Error getting unique visits for deviPce_token_id: " + deviceId + " exception " + e);
					continue;
				}
			}

			if (listMessages.size() > 0) {
				// TODO: Uncomment below line after few days. - 23rd May
				// elasticSearchIndexService.indexOrUpdateWithScriptAndAppend(Indexes.APP_USER_PROFILE,
				// MappingTypes.MAPPING_REALTIME, listMessages,
				// Constants.NotificationConstants.HOST_FIELD);
				elasticSearchIndexService.indexOrUpdate(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						listMessages);
				listMessages.clear();
				log.info("Thread " + threadIndex + ", Records indexed in "+Indexes.APP_USER_PROFILE+" index : "
						+ indexedRecordCount);
			}
		}
	}

	public static void main(String[] args) {
		DailyAppUserProfileIndexService duvi = new DailyAppUserProfileIndexService(Integer.valueOf(args[0]),
				Integer.valueOf(args[1]));
		duvi.run();
	}
	
}