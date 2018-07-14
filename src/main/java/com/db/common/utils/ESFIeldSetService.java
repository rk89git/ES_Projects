package com.db.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;

public class ESFIeldSetService {

	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ESFIeldSetService.class);
	
	private int dayCountForSummaryCalculation = 7;

	int batchSize = 1000;

	public ESFIeldSetService() {
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

	public void reindex(String sourceIndex,String sourceType,String[] fields, Object value, boolean calcAverageImpressionClicks, boolean purgeStories, int storiesSize) {
		long startTime = System.currentTimeMillis();
		try {
			
			String prevDateIndexSuffix = DateUtil.getPreviousDate();
			String prevDate = prevDateIndexSuffix.replaceAll("_", "-");
			String startDate = DateUtil.getPreviousDate(DateUtil.getPreviousDate(), -dayCountForSummaryCalculation)
					.replaceAll("_", "-");
			
			List<String> dateList = DateUtil.getDates(startDate, prevDate);
			
			int numRec = 1000;

			QueryBuilder queryBuilder=null;

			if(sourceIndex.equalsIgnoreCase(Indexes.USER_PERSONALIZATION_STATS)){
				queryBuilder=QueryBuilders.termQuery(Constants.NOTIFICATION_STATUS,Constants.NOTIFICATION_ON);
			}
			else{
				queryBuilder=QueryBuilders.matchAllQuery();
			}

			SearchResponse scrollResp = client.prepareSearch().setIndices(sourceIndex).setTypes(sourceType)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc", SortOrder.ASC)
					.setScroll(new TimeValue(60000))
					.setQuery(queryBuilder)
					.setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<>();
			AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
			long dayWiseThreadRecordCount = 1;
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						dayWiseThreadRecordCount = dayWiseThreadAtomicInt.getAndIncrement();
						Map<String, Object> record = new HashMap<>();
						record.put(Constants.ROWID, hit.getId());
						Arrays.stream(fields).forEach(s -> record.put(s,value));
						
						if(calcAverageImpressionClicks){
							cacluateAverageImpressionClick(sourceType, record, hit.getId(), dateList);
						}
						
						if(purgeStories){
							purgeStories(hit.getSource(), record, storiesSize);
						}

						listWeightage.add(record);
						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.indexOrUpdate(sourceIndex, sourceType, listWeightage);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().getTotalHits() + "; Records Processed: "
									+ dayWiseThreadRecordCount);
							listWeightage.clear();
						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			// Index remaining data if there after completing loop
			if (listWeightage.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(sourceIndex, sourceType, listWeightage);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: " + dayWiseThreadRecordCount);
				listWeightage.clear();
			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}
	
	private void cacluateAverageImpressionClick(String sourceType, Map<String, Object> record, String id, List<String> dateList) {
		try{
			MultiGetRequest multiGetRequest = new MultiGetRequest();

			for (String date : dateList) {
				multiGetRequest.add(Indexes.USER_PROFILE_DAYWISE, sourceType, id + "_" + date);
			}

			MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();

			double impressions = 0;
			double clicks = 0;

			for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
				if (multiGetItemResponse.getResponse().isExists()) {
					Map<String, Object> hitAsMap = multiGetItemResponse.getResponse().getSourceAsMap();

					if(hitAsMap.get("notification_clicks") != null && StringUtils.isNotBlank(hitAsMap.get("notification_clicks").toString())){
						clicks += Integer.parseInt(hitAsMap.get("notification_clicks").toString());
					}

					if(hitAsMap.get("notification_impressions") != null && StringUtils.isNotBlank(hitAsMap.get("notification_impressions").toString())){
						impressions += Integer.parseInt(hitAsMap.get("notification_impressions").toString());
					}
				}
			}

			if(impressions > 0){
				record.put("notify_impression_last_week", Math.ceil(impressions/dayCountForSummaryCalculation));
			}else{
				record.put("notify_impression_last_week", impressions);
			}

			if(clicks > 0){
				record.put("notify_click_last_week", Math.ceil(clicks/dayCountForSummaryCalculation));
			}else{
				record.put("notify_click_last_week", clicks);
			}
		}catch(Exception e){
			log.error("Error while caculating average weekly impressions & clicks.",e);
		}
	}
	
	private void purgeStories(Map<String, Object> source, Map<String, Object> record, int storiesSize){
		try{
			if(source.get(Constants.STORIES) != null && StringUtils.isNotBlank(source.get(Constants.STORIES).toString())){
				List<Object> readStories = (List<Object>)source.get(Constants.STORIES);

				int readStoriesSizeIndex = readStories.size()-1;

				if(readStoriesSizeIndex >= storiesSize){
					record.put(Constants.STORIES, readStories.subList(readStoriesSizeIndex-storiesSize, readStoriesSizeIndex));
				}
			}

			if(source.get("notificationServedIds") != null && StringUtils.isNotBlank(source.get("notificationServedIds").toString())){
				List<Object> notificationServedStories = (List<Object>)source.get("notificationServedIds");

				int notificationServedStoriesIndex = notificationServedStories.size()-1;

				if(notificationServedStoriesIndex >= storiesSize){
					record.put("notificationServedIds", notificationServedStories.subList(notificationServedStoriesIndex-storiesSize, notificationServedStoriesIndex));
				}
			}
		}catch(Exception e){
			log.error("Error while Purging read Stories && notificationServedIds.",e);
		}
	}

	public static void main(String[] args) {
		ESFIeldSetService duvi = new ESFIeldSetService();
		
		boolean calcAverageImpressionClicks = false;
		boolean purgeStories = false;
		int storiesSize = 500;
		
		if(args.length>4){
			calcAverageImpressionClicks = Boolean.parseBoolean(args[4]);
		}
		
		if(args.length>5){
			purgeStories = Boolean.parseBoolean(args[5]);
		}
		
		if(args.length>6){
			storiesSize = Integer.parseInt(args[6]);
		}
		
		duvi.reindex(args[0],args[1],args[2].split(","),Integer.parseInt(args[3]), calcAverageImpressionClicks, purgeStories, storiesSize);
			//duvi.reindex("app_user_profile_device_id", "realtime","dayNotificationCounter,notify_sent_count,notify_manual_sent_count,notify_autobot_sent_count,notification_impressions,notification_clicks".split(","), 0);
	}

}