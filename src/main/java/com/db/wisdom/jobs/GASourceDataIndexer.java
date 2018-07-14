package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.wisdom.model.AggregateReportKey;
import com.db.wisdom.model.AggregateReportValue;

public class GASourceDataIndexer {

	private static List<String> ucbTrackers = Arrays.asList("news-m-ucb", "news-ucb", "news-m-ucb_1", "news-ucb_1");	
	private static List<String> host_type = Arrays.asList("m", "w");
	private static List<String> host = Arrays.asList("15","27","24","1","26","2","4","3","48","10","9","20","5","6","7","62","8","49","4","40","28","33");
	private static List<String> t1 = Arrays.asList("ucb");
	private static List<String> t2 = Arrays.asList("fpaid");
	private static List<String> t3 = Arrays.asList("bgp", "vpaid", "njp", "nmp","fpamn","fpaamn","NJP", "NJNM", "NJS", "NJB", "fpamn1","NJP1", "xyz", "abc", "cmp", "fpamn6","fpamn", "opd", "gpd", "ipt", "aff", "ptil", "gpromo", "icub", "abc", "xyz", "cmp", "gr");
	private static List<String> t4 = Arrays.asList( "fpaid","bgp","Vpaid","njp","nmp","fpamn","fpaamn","NJP","NJNM","NJS","NJB","fpamn1","NJP1","fbo1","fpamn6","fbo","ABC","xyz","fbina","Gr","gpromo","money","opd");
	private static List<String> t5 = Arrays.asList("ucb", "cht", "nmax");


	ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	private static Logger log = LogManager.getLogger(GASourceDataIndexer.class);

	String[] parrentChildMappingArray=new String[]{"521,3849,1463,4444,3322,5483","1463","960,3850,3776,4371","4444","9069,9254","5483"};
	Map<String,List<String>> parentChildChannelMapping=getParentChildChannelMapping();

	Map<String,List<String>> getParentChildChannelMapping(){
		Map<String, List<String>> mappings = new HashMap<>();

		Arrays.stream(parrentChildMappingArray).forEach(e -> {
			String[] values = e.split(",");
			mappings.put(values[0], Arrays.asList(values));
		});
		return mappings;
	}

	void getSessions(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data, String source) {
		try {
			String indexName[] = new String[]{"realtime_upvs_" + date.replaceAll("-", "_")};
			if (interval.equals(Constants.MTD)){
				indexName = getIndexNamePatternMonth("realtime_upvs_",date);
			}
			else if(interval.equals(Constants.PREV_30_DAYS))
			{
				indexName = IndexUtils.getDailyIndexes("realtime_upvs_",DateUtil.getPreviousDate(date.replaceAll("-","_"), -30).replaceAll("_","-"), date);
			}

			//indexName = new String[]{"realtime_upvs_2018_0*"};

			AggregationBuilder sessionAgg = AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID);
			AggregationBuilder userAgg = AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD);
			TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE).field(Constants.HOST_TYPE).size(1000).
					subAggregation(sessionAgg).subAggregation(userAgg);

			List<AggregationBuilder> listAggregation = new ArrayList<>();
			Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
			for (Map.Entry<String, List<String>> entry : set) {
				hostAggregation.subAggregation(AggregationBuilders.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue()))
						.subAggregation(sessionAgg)
						.subAggregation(userAgg));
			}

			listAggregation.add(hostAggregation);
			QueryBuilder qb = getQuery(source);
			SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(indexName, MappingTypes.MAPPING_REALTIME, qb, 0, 0, null, null, null, listAggregation, null);

			Aggregations aggregationResponses = searchResponse.getAggregations();

			Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();
			for (Terms.Bucket hostBucket : ((Terms) aggregationResponses.get(Constants.HOST_TYPE)).getBuckets()) {
				String host = hostBucket.getKeyAsString();
				AggregateReportKey overallAggregateReportKey = new AggregateReportKey("all", host, source, "spl_tracker", date);
				double overallSessionCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESS_ID)).getValue();
				double overallUniqueUserCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESSION_ID_FIELD)).getValue();
				AggregateReportValue overallAgregateReportValue = null;
				if (data.containsKey(overallAggregateReportKey))
					overallAgregateReportValue = data.get(overallAggregateReportKey);
				else
					overallAgregateReportValue = new AggregateReportValue();

				if (interval.equals(Constants.MTD)) {
					overallAgregateReportValue.setSessionsMTD(overallSessionCount);
					overallAgregateReportValue.setUniqueVisitorsMTD(overallUniqueUserCount);
				} else if (interval.equals(Constants.DAY)) {
					overallAgregateReportValue.setSessions(overallSessionCount);
					overallAgregateReportValue.setUniqueVisitors(overallUniqueUserCount);
				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					overallAgregateReportValue.setSessionsPrev30Days(overallSessionCount);
					overallAgregateReportValue.setUniqueVisitorsPrev30Days(overallUniqueUserCount);
				}
				data.put(overallAggregateReportKey, overallAgregateReportValue);

				for (String channel : parentChildChannelMappingKeySet) {
					Filter channelBucket = hostBucket.getAggregations().get(channel);
					AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, source, "spl_tracker", date);
					double sessionCount = ((Cardinality) channelBucket.getAggregations().get(Constants.SESS_ID)).getValue();
					double uniqueUserCount = ((Cardinality) channelBucket.getAggregations().get(Constants.SESSION_ID_FIELD)).getValue();
					AggregateReportValue aggregateReportValue = null;
					if (data.containsKey(aggregateReportKey))
						aggregateReportValue = data.get(aggregateReportKey);
					else
						aggregateReportValue = new AggregateReportValue();

					if (interval.equals(Constants.MTD)) {
						aggregateReportValue.setSessionsMTD(sessionCount);
						aggregateReportValue.setUniqueVisitorsMTD(uniqueUserCount);
					} 
					else if (interval.equals(Constants.DAY)) {
						aggregateReportValue.setSessions(sessionCount);
						aggregateReportValue.setUniqueVisitors(uniqueUserCount);
					}
					else if (interval.equals(Constants.PREV_30_DAYS)) {
						aggregateReportValue.setSessionsPrev30Days(sessionCount);
						aggregateReportValue.setUniqueVisitorsPrev30Days(uniqueUserCount);
					}
					data.put(aggregateReportKey, aggregateReportValue);				
				}
			}

		} 
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while processing GA source data", e);
		}
	}

	private String[] getIndexNamePatternMonth(String prefix,String endDate) {
		String startDate=endDate.replaceAll("-\\d+?$", "-01");
		return endDate == null ? null : IndexUtils.getDailyIndexes(prefix,startDate,endDate);
	}

	private QueryBuilder getQuery(String source)
	{
		BoolQueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.HOST_TYPE, host_type))
				.must(QueryBuilders.termsQuery(Constants.HOST, host));
		QueryBuilder q1 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t1));
		QueryBuilder q2 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t2))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "social"));
		QueryBuilder q3 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t3))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "social"));
		QueryBuilder q4 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "direct"));
				//.mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t5));
		QueryBuilder q5 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "search"));
				//.mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t5));
		QueryBuilder q6 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "social"));
		QueryBuilder q7 = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "social"));
		QueryBuilder q8 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "direct"))
				.mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t5));
		QueryBuilder q9 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t4))
				.must(QueryBuilders.termQuery(Constants.REF_PLATFORM, "search"))
				.mustNot(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, t5));
		QueryBuilder q10 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Constants.REF_PLATFORM, "search"));

		if(source.equalsIgnoreCase("A1"))
		{
			qb.must(q1);
		}
		else if(source.equalsIgnoreCase("A2"))
		{
			qb.must(q2);
		}
		else if(source.equalsIgnoreCase("A3"))
		{
			qb.must(q3);
		}
		else if(source.equalsIgnoreCase("A4"))
		{
			qb.must(q4);
		}
		else if(source.equalsIgnoreCase("A5"))
		{
			qb.must(q5);
		}
		else if(source.equalsIgnoreCase("A6"))
		{
			qb.must(q6);
		}
		else if(source.equalsIgnoreCase("A7"))
		{
			qb.must(q7);
		}
		else if(source.equalsIgnoreCase("A8"))
		{
			qb.must(q8);
		}
		else if(source.equalsIgnoreCase("A9"))
		{
			qb.must(q9);
		}
		else if(source.equalsIgnoreCase("A10"))
		{
			qb.must(q10);
		}
		return qb;
	}


	public static void main(String[] args) {
		String date = DateUtil.getPreviousDate().replaceAll("_","-");
		//date = "2018-04-11";
		log.info("Preparing ga source data for " + date);
		GASourceDataIndexer gaSourceData = new GASourceDataIndexer();
		long startTime=System.currentTimeMillis();
		Map<AggregateReportKey, AggregateReportValue> data = new HashMap<>();		
		
		for(int i=1;i<=11;i++){
			String number = String.valueOf(i);
			gaSourceData.getSessions(date, "day", data, "A"+number);
			log.info("session report done for A"+number+ " "+ date); 
			gaSourceData.getSessions(date, "mtd", data, "A"+number);
			log.info("session report done for mtd A"+number+ " "+ date); 
			gaSourceData.getSessions(date, "prev_30_days", data, "A"+number);
			log.info("session report done for prev30days A"+number+ " "+ date);
		}		
		
		gaSourceData.index(data);
		log.info("aggregate report done in "+(System.currentTimeMillis()-startTime)/1000 + "s.");
	}

	void index(Map<AggregateReportKey, AggregateReportValue> data){
		List<Map<String,Object>> listData= new ArrayList<>();

		Set<Map.Entry<AggregateReportKey, AggregateReportValue>> dataEntrySet = data.entrySet();
		for (Map.Entry<AggregateReportKey, AggregateReportValue> dataEntry:dataEntrySet){
			Map<String,Object> map = new HashMap<>();
			AggregateReportKey key =dataEntry.getKey();
			AggregateReportValue value=dataEntry.getValue();
			map.put(Constants.ROWID,key.getChannelNumber()+"_"+key.getHost()+"_"+key.getIdentifierType()+"_"+key.getIdentifierValue()+"_"+key.getDate());
			map.put(Constants.CHANNEL_SLNO,key.getChannelNumber());
			map.put(Constants.HOST_TYPE,key.getHost());
			map.put("identifierType",key.getIdentifierType());
			map.put("identifierValue",key.getIdentifierValue());
			map.put(Constants.DATE,key.getDate());
			map.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			map.put("uniqueVisitors",value.getUniqueVisitors());
			map.put("uniqueVisitorsMTD",value.getUniqueVisitorsMTD());
			map.put("uniquePageViews",value.getUniquePageViews());
			map.put("uniquePageViewsMTD",value.getUniquePageViewsMTD());
			map.put("pageViews",value.getPageViews());
			map.put("pageViewsMTD",value.getPageViewsMTD());
			map.put("sessions",value.getSessions());
			map.put("sessionsMTD",value.getSessionsMTD());
			map.put("uniqueVisitorsPrev30Days",value.getUniqueVisitorsPrev30Days());
			map.put("uniquePageViewsPrev30Days",value.getUniquePageViewsPrev30Days());
			map.put("sessionsPrev30Days",value.getSessionsPrev30Days());
			map.put("pageViewsPrev30Days",value.getPageViewsPrev30Days());
			listData.add(map);
		}
		//System.out.println("data.........."+listData);
		elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_REPORT,DateUtil.getPreviousDate().replaceAll("_", "-"),DateUtil.getPreviousDate().replaceAll("_", "-"))[0], MappingTypes.MAPPING_REALTIME,listData);
	}
}
