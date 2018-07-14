package com.db.recommendation.jobs;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;

public class DailyCalculateCTR {

	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(DailyCalculateCTR.class);
	
	private Set<String> storiesSet = new HashSet<>();
	
	long batchSize = 500;

	public DailyCalculateCTR(){
		initializeClient();
		
		if(StringUtils.isNotBlank(DBConfig.getInstance().getProperty("dailyctrjob.batch.size"))){
			batchSize = Long.valueOf(DBConfig.getInstance().getProperty("dailyctrjob.batch.size"));
		}
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public void calculateImprAndClicks(){
		try{
			Map<String, Map<String, Integer>> storyWiseClickImprCalc = new HashMap<>();
			//Map<String, Map<String, Integer>> userWiseClickImprCalcWeb = new HashMap<>();
			
			String prevDate = DateUtil.getPreviousDate();
			String indexName = "identification_"+prevDate;

			log.info("Start calculating impressions & clicks for index "+indexName);

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.termsQuery(Constants.EVENT_TYPE, Arrays.asList(Constants.EVENT_TYPE_IMPRESSION, Constants.EVENT_TYPE_CLICK)));
			qb.must(QueryBuilders.termsQuery(Constants.HOST, Arrays.asList(Constants.Host.BHASKAR_MOBILE_WEB_HOST, Constants.Host.DIVYA_MOBILE_WEB_HOST)));

			SearchResponse searchResponse = client.prepareSearch(indexName).setScroll(new TimeValue(60000))
					.setQuery(qb).setSize(1000).execute().actionGet();

			do {
				for (SearchHit hit : searchResponse.getHits().getHits()) {
					Map<String, Object> identificationMap = hit.getSourceAsMap();

					if(identificationMap.containsKey(Constants.STORY_ID_FIELD)){
						String storyId = identificationMap.get(Constants.STORY_ID_FIELD).toString();
						
						storiesSet.add(storyId);

						incrementImpressionClickCounter(storyId, storyWiseClickImprCalc, identificationMap);
					}

					/*if(identificationMap.containsKey(Constants.SESSION_ID_FIELD)){
						String sessionId = identificationMap.get(Constants.SESSION_ID_FIELD).toString();

						incrementImpressionClickCounter(sessionId, userWiseClickImprCalcWeb, identificationMap);
					}*/
				}
				
				/*if(userWiseClickImprCalcWeb.keySet().size() >= 500){
					insertData(Indexes.USER_PERSONALIZATION_STATS, userWiseClickImprCalcWeb);
					userWiseClickImprCalcWeb.clear();
				}*/
				
				if(storyWiseClickImprCalc.keySet().size() >= 500){
					insertData(Indexes.STORY_UNIQUE_DETAIL, storyWiseClickImprCalc);
					storyWiseClickImprCalc.clear();
				}

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();

			} while (searchResponse.getHits().getHits().length != 0);
			
			/*if(!userWiseClickImprCalcWeb.keySet().isEmpty()){
				insertData(Indexes.USER_PERSONALIZATION_STATS, userWiseClickImprCalcWeb);
				userWiseClickImprCalcWeb.clear();
			}*/
			
			if(!storyWiseClickImprCalc.keySet().isEmpty()){
				insertData(Indexes.STORY_UNIQUE_DETAIL, storyWiseClickImprCalc);
				storyWiseClickImprCalc.clear();
			}

			log.info("Calculated impressions & clicks for index "+indexName);
		}catch(Exception e){
			log.error("Error while calculating CTR ", e);
		}
	}

	private void incrementImpressionClickCounter(String id, Map<String, Map<String, Integer>> counterMap, Map<String, Object> sourceMap){
		try{
			String eventType = Constants.CLICKS;
			int event = Integer.parseInt(sourceMap.get(Constants.EVENT_TYPE).toString());

			if(event == Constants.EVENT_TYPE_IMPRESSION){
				eventType = Constants.IMPRESSIONS;
			}

			if(!counterMap.containsKey(id)){
				Map<String, Integer> map = new HashMap<>();
				
				map.put(eventType, 1);

				counterMap.put(id, map);
			}else{
				Map<String, Integer> map = counterMap.get(id);
				
				if(map.containsKey(eventType)){
					map.put(eventType, map.get(eventType)+1);
				}else{
					map.put(eventType, 1);
				}
			}
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}

	private void insertData(String indexName, Map<String, Map<String, Integer>> incrementalMap){
		try{
			int counter = elasticSearchIndexService.incrementCounter(indexName, MappingTypes.MAPPING_REALTIME, incrementalMap);

			log.info("impression & Click updated for records " + counter + " in "+indexName);
		}catch(Exception e){
			log.error("Error while inserting data for index "+indexName, e);
		}
	}
	
	public void calculateStoriesCTR(){
		calculateCTR(Indexes.STORY_UNIQUE_DETAIL, storiesSet);
	}
	
	/*public void calculateUsersCTR(){
		calculateCTR(Indexes.USER_PERSONALIZATION_STATS);
	}*/
	
	private void calculateCTR(String indexName, Set<String> batchSet){
		log.info("Starting calculate ctr");
		try{
			MultiGetRequestBuilder mGetRequest = client.prepareMultiGet();
			
			int incrementCounter = 0;
			int batchNo = 0;
			
			for (String batchId : batchSet) {
				++incrementCounter;
				
				mGetRequest.add(indexName, MappingTypes.MAPPING_REALTIME,
						batchId);
				
				if(incrementCounter >= batchSize){
					performCTRBatchOperation(++batchNo, indexName, mGetRequest);

					mGetRequest = client.prepareMultiGet();
					incrementCounter=0;
				}
			}
			
			if(incrementCounter > 0){
				performCTRBatchOperation(++batchNo, indexName, mGetRequest);
			}
			log.info("Calculated ctr");
		}catch(Exception e){
			batchSet.clear();
			log.error("Error while calculating ctr in index "+indexName, e);
		}
		batchSet.clear();
	}

	private void performCTRBatchOperation(int batchIndex, String indexName, MultiGetRequestBuilder mGetRequest) {
		try{
			List<Map<String, Object>> ctrDataList  = new ArrayList<>();
			MultiGetResponse mGetResponse = mGetRequest.execute().actionGet();

			for (MultiGetItemResponse item : mGetResponse) {
				if (item.getResponse().isExists()) {
					Map<String, Object> ctrData = new HashMap<>();

					Map<String, Object> storyMap = item.getResponse().getSource();
					String id = item.getId();
					double impressions = 0;
					double clicks = 0;
					double ctr = 0;
					
					if(storyMap.get(Constants.IMPRESSIONS) != null){
						impressions = Double.parseDouble(storyMap.get(Constants.IMPRESSIONS).toString());
					}
					
					if(storyMap.get(Constants.CLICKS) != null){
						clicks = Double.parseDouble(storyMap.get(Constants.CLICKS).toString());
					}
					
					if(impressions != 0 && clicks != 0){
						ctr = BigDecimal.valueOf((clicks/impressions)*100).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
					}

					ctrData.put(Constants.ROWID, id);
					ctrData.put(Constants.CTR, ctr);

					ctrDataList.add(ctrData);
				}
			}
			int counter = elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, ctrDataList);

			log.info("Batch No "+batchIndex+" - "+counter+" CTR record updates in index "+indexName);
			ctrDataList.clear();
		}catch(Exception e){
			log.error("Error while calculating CTR for batch "+batchIndex, e);
		}
	}
	
	public static void main(String[] args) {
		Instant ins1 = Instant.now(); 
		
		DailyCalculateCTR dailyCalculateCTR = new DailyCalculateCTR();
		
		dailyCalculateCTR.calculateImprAndClicks();
		dailyCalculateCTR.calculateStoriesCTR();
		
		Instant ins2 = Instant.now(); 

		log.info("Calculate CTR JOb ended at "+Duration.between(ins1, ins2).getSeconds());
	}
}
