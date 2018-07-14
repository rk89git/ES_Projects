package com.db.recommendation.jobs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class DailyAppUserProfileIndex2 {

	private static Logger log = LogManager.getLogger(DailyAppUserProfileIndexService.class);
	private Client client = null;
	private int countRecordsToIndex = 0;
	private int dayCountForSummaryCalculation = 7;
	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	private int numThreads = 5;

	public DailyAppUserProfileIndex2(Integer countRecordsToIndex, Integer dayCountForSummaryCalculation) {
		this.countRecordsToIndex = countRecordsToIndex;
		this.dayCountForSummaryCalculation = dayCountForSummaryCalculation;
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public DailyAppUserProfileIndex2() {
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
		try {
			long startTime = System.currentTimeMillis();
			// Start of unique users calculation
			Set<String> deviceIdList = new HashSet<>();
			int startFrom = 0;
			int numRec = 1000;
			
			String date = DateUtil.getPreviousDate();
			String realtimeIndex = "realtime_" + date;
			
			log.info("searching  in index of date " + date + " strIndexDate: " + realtimeIndex);

			SearchResponse scrollResp = client.prepareSearch(realtimeIndex)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
					.setQuery(QueryBuilders.matchQuery(Constants.TRACKER, Constants.APP)).setScroll(new TimeValue(60000))
					.addSort("_doc",SortOrder.ASC)
					.setSize(numRec).execute()
					.actionGet();

			while (true) {
				startFrom += scrollResp.getHits().getHits().length;
				
				log.info("Hit size to process : " + scrollResp.getHits().getHits().length+" for index: " + realtimeIndex + " total: " + scrollResp.getHits().getTotalHits()
						+ ", Fetched Hits: " + scrollResp.getHits().getHits().length + " , Total fetched : "
						+ startFrom);
				
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						String deviceId = (String) hit.getSource().get(Constants.SESSION_ID_FIELD);
						
						if (StringUtils.isNotBlank(deviceId)) {
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
				
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
			// End of unique users calculation
			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
			ArrayList<String> sessionIdLst = new ArrayList<>();
		
			sessionIdLst.addAll(deviceIdList);
			int lstSize = sessionIdLst.size();
			
			log.info("Total Users to Process:" + lstSize);

			int rangeDiff = (lstSize / numThreads);
			int startRange = 0;
			int endRange = rangeDiff;

			for (int i = 0; i < numThreads; i++) {
				log.info("Thread "+i+" Start Range: " + startRange+ "& End Range: "+endRange);
				
				executorService.execute(
						new SummaryDeriveRunnable(i, sessionIdLst.subList(startRange, endRange - 1)));
				
				startRange = endRange;
				endRange = endRange + rangeDiff;
			}
			
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			
			long endTime = System.currentTimeMillis();
			
			log.info("Total Execution time(Minutes) : " + (endTime - startTime) / (1000 * 60));
		}catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}
	}
	
	public class SummaryDeriveRunnable implements Runnable {
		private final List<String> sessionIds;
		
		int totalCount = 0;
		int threadIndex = 0;
		int batchSize = 1000;
		
		private String prevDateIndexSuffix = DateUtil.getPreviousDate();
		private String prevDate = prevDateIndexSuffix.replaceAll("_", "-");
		private String startDate = DateUtil.getPreviousDate(DateUtil.getPreviousDate(), -dayCountForSummaryCalculation)
				.replaceAll("_", "-");

		SummaryDeriveRunnable(int threadIndex, List<String> sessionIds) {
			this.sessionIds = sessionIds;
			this.threadIndex = threadIndex;
		}
		
		@Override
		public void run() {
			try{
				log.info("Executing thread " + threadIndex);
				
				prepareDailyWiseSummary();
				prepareConsolidatedSummary(sessionIds, DateUtil.getDates(startDate, prevDate)); 
			}catch(Exception e){
				log.error("Error Occurred while running daily App user profile index", e);
			}
		}
		
		private void prepareDailyWiseSummary(){
			long dayWiseThreadRecordCount = 1;
			
			AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
			List<Map<String, Object>> listMessages = new ArrayList<>();

			String indexNamePrevDate = "realtime_" + DateUtil.getPreviousDate();

			for (String deviceId : sessionIds) {
				try {
					dayWiseThreadRecordCount = dayWiseThreadAtomicInt.getAndIncrement();

					SearchResponse sr = client.prepareSearch(indexNamePrevDate)
							.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setTimeout(new TimeValue(2000))
							.setQuery(QueryBuilders.termQuery(Constants.SESSION_ID_FIELD, deviceId))
							.addAggregation(AggregationBuilders.terms("PCAT_ID").field(Constants.PARENT_CAT_ID_FIELD).size(5))
							.addAggregation(AggregationBuilders.terms("CAT_ID").field(Constants.CAT_ID_FIELD).size(5))
							.addAggregation(AggregationBuilders.terms("PEOPLE").field(Constants.PEOPLE).size(7))
							.addAggregation(AggregationBuilders.terms("EVENT").field(Constants.EVENT).size(7))
							.addAggregation(AggregationBuilders.terms("LOCATION").field(Constants.LOCATION).size(7))
							.addAggregation(AggregationBuilders.terms("ORGANIZATION").field(Constants.ORGANIZATION).size(7))
							.addAggregation(AggregationBuilders.terms("HOST").field(Constants.HOST).size(5))
							.addAggregation(AggregationBuilders.terms("SECTION").field(Constants.SECTION).size(5))
							.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC)
							.execute().actionGet();

					Map<String, Object> record = new HashMap<>();

					Terms cat = sr.getAggregations().get("CAT_ID");
					record.put(Constants.CAT_ID_FIELD, addTermsResultListInt(cat, 5));

					Terms pCat = sr.getAggregations().get("PCAT_ID");
					record.put(Constants.PARENT_CAT_ID_FIELD, addTermsResultListInt(pCat, 5));

					Terms people = sr.getAggregations().get("PEOPLE");
					record.put(Constants.PEOPLE, addTermsResultListStr(people, 5));

					Terms events = sr.getAggregations().get("EVENT");
					record.put(Constants.EVENT, addTermsResultListStr(events, 5));

					Terms locations = sr.getAggregations().get("LOCATION");
					record.put(Constants.LOCATION, addTermsResultListStr(locations, 5));

					Terms organizations = sr.getAggregations().get("ORGANIZATION");
					record.put(Constants.ORGANIZATION, addTermsResultListStr(organizations, 5));

					Terms hosts = sr.getAggregations().get("HOST");
					record.put(Constants.HOST, addTermsResultListStr(hosts, 5));
					
					Terms sections = sr.getAggregations().get("SECTION");
					record.put(Constants.SECTION, addTermsResultListStr(sections, 5));

					listMessages.add(record);

					record.put(Constants.ROWID, deviceId + "_" + prevDateIndexSuffix);
					record.put(Constants.SESSION_ID_FIELD, deviceId);
					record.put(Constants.DATE_TIME_FIELD, prevDate);
					record.put(Constants.STORY_COUNT, sr.getHits().getTotalHits());

					SearchHit[] searchHits = sr.getHits().getHits();

					// Retrieve user location information
					if (searchHits.length > 0) {
						SearchHit hit = searchHits[0];
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
						record.put(Constants.APP_VERSION, hit.getSource().get(Constants.APP_VERSION));
					}

					List<Integer> storyIds = new ArrayList<>();

					for (SearchHit searchHit : searchHits) {
						storyIds.add(
								Integer.valueOf((String) searchHit.getSource().get(Constants.STORY_ID_FIELD)));
					}

					record.put(Constants.STORIES, storyIds);

					log.info("User profile build for " + deviceId);

					if (listMessages.size() == batchSize) {
						elasticSearchIndexService.indexOrUpdate(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
								listMessages);

						log.info("Thread " + threadIndex + " Day wise summary; Total App Users: "
								+ sessionIds.size() + "; Users Processed: " + dayWiseThreadRecordCount);

						listMessages.clear();
					}

				} catch (Exception e) {
					log.error("Error getting unique visits for device_token_id: " + deviceId + " exception. ", e);
					continue;
				}
			}

			if (!listMessages.isEmpty()) {
				elasticSearchIndexService.indexOrUpdate(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
						listMessages);

				log.info("Thread " + threadIndex + " Day wise summary; Total App Users: "
						+ sessionIds.size() + "; Users Processed: " + dayWiseThreadRecordCount);

				listMessages.clear();
			}
		}
		
		@SuppressWarnings("unchecked")
		private void prepareConsolidatedSummary(List<String> sessionIds, List<String> dateList){
			
			AtomicInteger finalSummaryThreadAtomicInt = new AtomicInteger(1);
			long finalSummaryThreadRecordCount = 1;
			List<Map<String, Object>> userFinalSummaryList = new ArrayList<>();
			
			for (String session_id : sessionIds) {
				try {

					MultiGetRequest multiGetRequest = new MultiGetRequest();
					
					for (String date : dateList) {
						multiGetRequest.add(Indexes.USER_PROFILE_DAYWISE, MappingTypes.MAPPING_REALTIME,
								session_id + "_" + date);
					}
					
					finalSummaryThreadRecordCount = finalSummaryThreadAtomicInt.getAndIncrement();

					MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();

					long storiesViewed = 0;
					Map<Integer, Integer> categoryOccuranceMap = new HashMap<>();
					Map<Integer, Integer> parentCategoryOccuranceMap = new HashMap<>();
					Map<Integer, Integer> hostOccuranceMap = new HashMap<>();
					Map<String, Integer> peopleOccuranceMap = new HashMap<>();
					Map<String, Integer> eventOccuranceMap = new HashMap<>();
					Map<String, Integer> locationOccuranceMap = new HashMap<>();
					Map<String, Integer> organizationOccuranceMap = new HashMap<>();
					Map<String, Integer> sectionOccuranceMap = new HashMap<>();
					Map<String, Object> record = new HashMap<>();

					for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
						if (multiGetItemResponse.getResponse().isExists()) {
							Map<String, Object> hitAsMap = multiGetItemResponse.getResponse().getSourceAsMap();
							
							if (StringUtils.isNotBlank((String) hitAsMap.get(Constants.CITY))) {
								record.put(Constants.CITY, hitAsMap.get(Constants.CITY));
							}
							if (StringUtils.isNotBlank((String) hitAsMap.get(Constants.STATE))) {
								record.put(Constants.STATE, hitAsMap.get(Constants.STATE));
							}
							if (StringUtils.isNotBlank((String) hitAsMap.get(Constants.COUNTRY))) {
								record.put(Constants.COUNTRY, hitAsMap.get(Constants.COUNTRY));
							}
							
							record.put(Constants.DEVICEID, hitAsMap.get(Constants.SESSION_ID_FIELD));
							record.put(Constants.APP_VERSION, hitAsMap.get(Constants.APP_VERSION));
							record.put(Constants.NotificationConstants.DAY_NOTIFICATION_COUNTER,0);
							
							List<Object> categoryList = (List<Object>) hitAsMap.get(Constants.CAT_ID_FIELD);
							updateIntKeyOccurences(categoryList, categoryOccuranceMap);

							List<Object> parentCategoryList = (List<Object>) hitAsMap.get(Constants.PARENT_CAT_ID_FIELD);
							updateIntKeyOccurences(parentCategoryList, parentCategoryOccuranceMap);
							
							List<String> peopleList = (List<String>) hitAsMap.get(Constants.PEOPLE);
							updateStrKeyOccurences(peopleList, peopleOccuranceMap);
							
							List<String> eventList = (List<String>) hitAsMap.get(Constants.EVENT);
							updateStrKeyOccurences(eventList, eventOccuranceMap);
							
							List<String> locationList = (List<String>) hitAsMap.get(Constants.LOCATION);
							updateStrKeyOccurences(locationList, locationOccuranceMap);

							List<String> organizationList = (List<String>) hitAsMap.get(Constants.ORGANIZATION);
							updateStrKeyOccurences(organizationList, organizationOccuranceMap);
							
							List<String> sectionList = (List<String>) hitAsMap.get(Constants.SECTION);
							updateStrKeyOccurences(sectionList, sectionOccuranceMap);

							List<Object> hostList = null;
							if(hitAsMap.get(Constants.HOST) instanceof List){
								hostList = (List<Object>) hitAsMap.get(Constants.HOST);
							}else if(hitAsMap.get(Constants.HOST) != null){
								hostList = Arrays.asList(hitAsMap.get(Constants.HOST));
							}
							updateIntKeyOccurences(hostList, hostOccuranceMap);
							
							if(hitAsMap.get(Constants.STORY_COUNT) != null && StringUtils.isNotBlank(hitAsMap.get(Constants.STORY_COUNT).toString())){
								storiesViewed = storiesViewed + (Integer) hitAsMap.get(Constants.STORY_COUNT);
							}
						}
					}
					
					record.put(Constants.CAT_ID_FIELD, getTopIntCat(categoryOccuranceMap));
					record.put(Constants.PARENT_CAT_ID_FIELD, getTopIntCat(parentCategoryOccuranceMap));
					record.put(Constants.PEOPLE, getTopStrCat(peopleOccuranceMap));
					record.put(Constants.EVENT, getTopStrCat(eventOccuranceMap));
					record.put(Constants.LOCATION, getTopStrCat(locationOccuranceMap));
					record.put(Constants.ORGANIZATION, getTopStrCat(organizationOccuranceMap));
					record.put(Constants.HOST, getTopIntCat(hostOccuranceMap));
					record.put(Constants.SECTION, getTopStrCat(sectionOccuranceMap));
					record.put(Constants.STORY_COUNT, storiesViewed);
					record.put(Constants.ROWID, session_id);
					record.put(Constants.DEVICEID, session_id);
					record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

					userFinalSummaryList.add(record);
					
					if (userFinalSummaryList.size() == 1000) {
						elasticSearchIndexService.indexOrUpdate(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
								userFinalSummaryList);
					
						userFinalSummaryList.clear();
						
						log.info("Thread " + threadIndex + ", Records indexed in "+Indexes.APP_USER_PROFILE+" index : "
								+ finalSummaryThreadRecordCount);
					}
					
				}catch(Exception e){
					log.error("Error occured in consolidating profile for session id: " + session_id
							+ ", Exception " + e.getMessage(), e);
					continue;
				}
			}
			
			if (!userFinalSummaryList.isEmpty()) {
				elasticSearchIndexService.indexOrUpdate(Indexes.APP_USER_PROFILE, MappingTypes.MAPPING_REALTIME,
						userFinalSummaryList);
				
				userFinalSummaryList.clear();
				
				log.info("Thread " + threadIndex + ", Records indexed in "+Indexes.APP_USER_PROFILE+" index : "
						+ finalSummaryThreadRecordCount);
			}
		}
		
		private List<String> addTermsResultListStr(Terms termResult, int size){
			List<String> resultList = new ArrayList<>();
			
			for (Terms.Bucket entry : termResult.getBuckets()) {
				if (entry.getKey() != null && !(entry.getKeyAsString()).isEmpty()) {
					resultList.add( entry.getKeyAsString());
					if (resultList.size() == size) {
						break;
					}
				}
			}
			return resultList;
		}
		
		private List<Integer> addTermsResultListInt(Terms termResult, int size){
			List<Integer> resultList = new ArrayList<>();
			
			for (Terms.Bucket entry : termResult.getBuckets()) {
				if (entry.getKey() != null && !(entry.getKeyAsString()).equalsIgnoreCase("0")) {
					resultList.add(Integer.valueOf(entry.getKeyAsString()));
					if (resultList.size() == size) {
						break;
					}
				}
			}
			return resultList; 
		}
		
		@SuppressWarnings("unchecked")
		private List<Integer> getTopIntCat(Map<Integer, Integer> map) {
			List<Integer> result = new ArrayList<>();
			Object[] a = map.entrySet().toArray();
			
			Arrays.sort(a, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<Integer, Integer>) o2).getValue()
							.compareTo(((Map.Entry<Integer, Integer>) o1).getValue());
				}
			});
			
			for (Object e : a) {
				result.add(((Map.Entry<Integer, Integer>) e).getKey());
				if (result.size() >= 5) {
					break;
				}
			}
			return result;
		}
		
		@SuppressWarnings("unchecked")
		private List<String> getTopStrCat(Map<String, Integer> map) {
			List<String> result = new ArrayList<>();
			Object[] a = map.entrySet().toArray();
			
			Arrays.sort(a, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<Integer, Integer>) o2).getValue()
							.compareTo(((Map.Entry<String, Integer>) o1).getValue());
				}
			});
			
			for (Object e : a) {
				result.add(((Map.Entry<String, Integer>) e).getKey());
				if (result.size() >= 5) {
					break;
				}
			}
			return result;
		}
		
		private void updateIntKeyOccurences(List<Object> list, Map<Integer, Integer> occuranceMap){
			if(list != null){
				for (Object key : list) {
					if(key != null){
						int keyInt = Integer.parseInt(key.toString());

						if (!occuranceMap.containsKey(keyInt)) {
							occuranceMap.put(keyInt, 1);
						} else {
							occuranceMap.put(keyInt, occuranceMap.get(keyInt) + 1);
						}
					}
				}
			}
		}
		
		private void updateStrKeyOccurences(List<String> list, Map<String, Integer> occuranceMap){
			if(list != null){
				for (String key : list) {
					
					if (!occuranceMap.containsKey(key)) {
						occuranceMap.put(key, 1);
					} else {
						occuranceMap.put(key, occuranceMap.get(key) + 1);
					}
				}
			}
		}
	}
	
	public static void main(String...strings ){
		Instant startInstance = Instant.now();
		
		new DailyAppUserProfileIndex2().run();
		
		Instant endInstance = Instant.now();
		
		System.out.println("AppUserDailyProfie JOb ended at - "+Duration.between(startInstance, endInstance).getSeconds());
		
	}
}
