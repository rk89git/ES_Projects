package com.db.wisdom.jobs;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.wisdom.model.AggregateReportKey;
import com.db.wisdom.model.AggregateReportValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import java.util.*;

/**
 * Created by Satya on 14-03-2018.
 */
public class AggregateUsersReport {

	//private static List<String> ucbTrackers = Arrays.asList("news-m-ucb", "news-ucb", "news-m-ucb_1", "news-ucb_1");	

	ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	private static Logger log = LogManager.getLogger(AggregateUsersReport.class);

	String[] parrentChildMappingArray=new String[]{"521,3849","1463","960,3850","4444","9069","5483"};
	
	Map<String,List<String>> parentChildChannelMapping=getParentChildChannelMapping();

	Map<String,List<String>> getParentChildChannelMapping(){ 
		Map<String, List<String>> mappings = new HashMap<>();

		Arrays.stream(parrentChildMappingArray).forEach(e -> {
			String[] values = e.split(",");
			mappings.put(values[0], Arrays.asList(values));
		});
		return mappings;
	}

	void getUserSessions(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data) {
		
		String indexName[] = new String[]{"realtime_upvs_" + date.replaceAll("-", "_")};
		
		if (interval.equals(Constants.MTD)){
			indexName = getIndexNamePatternMonth("realtime_upvs_",date);
		}  
		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			indexName = IndexUtils.getDailyIndexes("realtime_upvs_",DateUtil.getPreviousDate(date.replaceAll("-","_"), -30).replaceAll("_","-"), date);
			//indexName = new String[]{"realtime_tracking_2018_0*"};
		}

		
		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE).field(Constants.HOST_TYPE).size(1000).
				subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID)).
				subAggregation(AggregationBuilders.cardinality(Constants.USER_COOKIE_ID).field(Constants.USER_COOKIE_ID));


		List<AggregationBuilder> listAggregation = new ArrayList<>();
		Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
		
		for (Map.Entry<String, List<String>> entry : set) {
			listAggregation.add(AggregationBuilders.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue())).subAggregation(hostAggregation));
		}

		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(indexName, MappingTypes.MAPPING_REALTIME, QueryBuilders.matchAllQuery(), 0, 0, null, null, null, listAggregation, null);

		
		Aggregations aggregationResponses = searchResponse.getAggregations() ;

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();
		for (String channel : parentChildChannelMappingKeySet) {
			Filter channelBucket = aggregationResponses.get(channel);
			for (Terms.Bucket hostBucket : ((Terms) channelBucket.getAggregations().get(Constants.HOST_TYPE)).getBuckets()) {
				String host = hostBucket.getKeyAsString();
				
				AggregateReportKey overallAggregateReportKey = new AggregateReportKey(channel, host, "channelOverall", "filter", date);
				
				double overllSessionCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESS_ID)).getValue();
				double overallUniqueUserCount = ((Cardinality) hostBucket.getAggregations().get(Constants.USER_COOKIE_ID)).getValue();
				
								
				AggregateReportValue overallAggregateReportValue = null;
				if (data.containsKey(overallAggregateReportKey))
					overallAggregateReportValue = data.get(overallAggregateReportKey);
				else
					overallAggregateReportValue = new AggregateReportValue();
				
				if (interval.equals(Constants.MTD)) {
					overallAggregateReportValue.setSessionsMTD(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitorsMTD(overallUniqueUserCount);
				} 
				else if (interval.equals(Constants.DAY)) {
					overallAggregateReportValue.setSessions(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitors(overallUniqueUserCount);
				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					overallAggregateReportValue.setSessionsPrev30Days(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitorsPrev30Days(overallUniqueUserCount);
				}				
				data.put(overallAggregateReportKey, overallAggregateReportValue);
				
			}
		}
	}


	private String[] getIndexNamePatternMonth(String prefix,String endDate) {
		String startDate=endDate.replaceAll("-\\d+?$", "-01");
		return endDate == null ? null : IndexUtils.getDailyIndexes(prefix,startDate,endDate);
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		String date = DateUtil.getPreviousDate().replaceAll("_","-");		
		System.out.println("previous date:  " + date);
		
		log.info("Preparing aggregate user report for " + date);
		AggregateUsersReport aggregateReport = new AggregateUsersReport();
		long startTime=System.currentTimeMillis();
		Map<AggregateReportKey, AggregateReportValue> data = new HashMap<>();
		
		aggregateReport.getUserSessions(date, "mtd", data);
		log.info("sessions users report done for " + date);
		log.info("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		System.out.println("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		
		aggregateReport.getUserSessions(date, "day", data);		
		log.info("sessions users report done for " + date);
		log.info("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		System.out.println("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		
		aggregateReport.getUserSessions(date, "prev_30_days", data);
		log.info("sessions and unique users report done for prev_30_days " + date);
		log.info("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		System.out.println("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		
		aggregateReport.index(data);
		log.info("aggregate report done in "+(System.currentTimeMillis()-startTime)/1000 + "s.");
		log.info("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		System.out.println("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
	}

	void index(Map<AggregateReportKey, AggregateReportValue> data){
		List<Map<String,Object>> listData= new ArrayList<>();

		Set<Map.Entry<AggregateReportKey, AggregateReportValue>> dataEntrySet = data.entrySet();
		for (Map.Entry<AggregateReportKey, AggregateReportValue> dataEntry:dataEntrySet){
			Map<String,Object> map = new HashMap<>();
			AggregateReportKey key =dataEntry.getKey();
			AggregateReportValue value=dataEntry.getValue();
			
			map.put(Constants.ROWID,key.getChannelNumber()+"_"+key.getHost()+"_"+key.getDate());
			map.put(Constants.CHANNEL_SLNO,key.getChannelNumber());
			map.put(Constants.HOST_TYPE,key.getHost());			
			map.put(Constants.DATE,key.getDate());
			map.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());			
			
			map.put("sessions",value.getSessions());			
			map.put("sessionsMTD",value.getSessionsMTD());			
			map.put("sessionsPrev30Days",value.getSessionsPrev30Days());
			
			map.put("uniqueVisitors",value.getUniqueVisitors());			
			map.put("uniqueVisitorsMTD",value.getUniqueVisitorsMTD());			;
			map.put("uniqueVisitorsPrev30Days",value.getUniqueVisitorsPrev30Days());
			listData.add(map);
		}
		elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.AGGREGATE_USERS), MappingTypes.MAPPING_REALTIME, listData);
	}

}
