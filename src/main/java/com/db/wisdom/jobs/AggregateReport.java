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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import javax.management.Query;
import java.util.*;

/**
 * Created by Satya on 14-03-2018.
 */
public class AggregateReport {

	private static List<String> ucbTrackers = Arrays.asList("news-m-ucb", "news-ucb", "news-m-ucb_1", "news-ucb_1");	

	ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	private static Logger log = LogManager.getLogger(AggregateReport.class);

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
			indexName = IndexUtils.getDailyIndexes("realtime_upvs_",DateUtil.getPreviousDate(date.replaceAll("-","_"), -10).replaceAll("_","-"), date);
		}

		//indexName = new String[]{"realtime_upvs_2018_0*"};
		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE).field(Constants.HOST_TYPE).size(1000).
				subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID)).
				subAggregation(AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD)).				
				subAggregation(AggregationBuilders.terms(Constants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(1000).
						subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID)).
						subAggregation(AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD))
						).
				subAggregation(AggregationBuilders.terms(Constants.SPL_TRACKER).field(Constants.SPL_TRACKER).size(1000).
						subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID)).
						subAggregation(AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD))
						);


		List<AggregationBuilder> listAggregation = new ArrayList<>();
		Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
		for (Map.Entry<String, List<String>> entry : set) {
			listAggregation.add(AggregationBuilders.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue())).subAggregation(hostAggregation));
		}

		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(indexName, MappingTypes.MAPPING_REALTIME, QueryBuilders.matchAllQuery(), 0, 0, null, null, null, listAggregation, null);


		Aggregations aggregationResponses = searchResponse.getAggregations();

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();
		for (String channel : parentChildChannelMappingKeySet) {
			Filter channelBucket = aggregationResponses.get(channel);
			for (Terms.Bucket hostBucket : ((Terms) channelBucket.getAggregations().get(Constants.HOST_TYPE)).getBuckets()) {
				String host = hostBucket.getKeyAsString();
				
				AggregateReportKey overallAggregateReportKey = new AggregateReportKey(channel, host, "channelOverall", "filter", date);
				double overllSessionCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESS_ID)).getValue();
				double overallUniqueUserCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESSION_ID_FIELD)).getValue();
				AggregateReportValue overallAggregateReportValue = null;
				if (data.containsKey(overallAggregateReportKey))
					overallAggregateReportValue = data.get(overallAggregateReportKey);
				else
					overallAggregateReportValue = new AggregateReportValue();
				
				if (interval.equals(Constants.MTD)) {
					overallAggregateReportValue.setSessionsMTD(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitorsMTD(overallUniqueUserCount);
				} else if (interval.equals(Constants.DAY)) {
					overallAggregateReportValue.setSessions(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitors(overallUniqueUserCount);
				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					overallAggregateReportValue.setSessionsPrev30Days(overllSessionCount);
					overallAggregateReportValue.setUniqueVisitorsPrev30Days(overallUniqueUserCount);
				}
				data.put(overallAggregateReportKey, overallAggregateReportValue);
				
				for (Terms.Bucket superCatIdBucket : ((Terms) hostBucket.getAggregations().get(Constants.SUPER_CAT_NAME)).getBuckets()) {
					String superCatId = superCatIdBucket.getKeyAsString();
					AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, superCatId, Constants.SUPER_CAT_NAME, date);
					double sessionCount = ((Cardinality) superCatIdBucket.getAggregations().get(Constants.SESS_ID)).getValue();
					double UniqueUserCount = ((Cardinality) superCatIdBucket.getAggregations().get(Constants.SESSION_ID_FIELD)).getValue();
					AggregateReportValue aggregateReportValue = null;
					if (data.containsKey(aggregateReportKey))
						aggregateReportValue = data.get(aggregateReportKey);
					else
						aggregateReportValue = new AggregateReportValue();
					if (interval.equals(Constants.MTD)) {
						aggregateReportValue.setSessionsMTD(sessionCount);
						aggregateReportValue.setUniqueVisitorsMTD(UniqueUserCount);
					} else if (interval.equals(Constants.DAY)) {
						aggregateReportValue.setSessions(sessionCount);
						aggregateReportValue.setUniqueVisitors(UniqueUserCount);
					}
					else if (interval.equals(Constants.PREV_30_DAYS)) {
						aggregateReportValue.setSessionsPrev30Days(sessionCount);
						aggregateReportValue.setUniqueVisitorsPrev30Days(UniqueUserCount);
					}
					data.put(aggregateReportKey, aggregateReportValue);
				}
				for (Terms.Bucket splTrackerBucket : ((Terms) hostBucket.getAggregations().get(Constants.SPL_TRACKER)).getBuckets()) {
					String splTracker = splTrackerBucket.getKeyAsString();
					AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, splTracker, Constants.SPL_TRACKER, date);
					double sessionCount = ((Cardinality) splTrackerBucket.getAggregations().get(Constants.SESS_ID)).getValue();
					double UniqueUserCount = ((Cardinality) splTrackerBucket.getAggregations().get(Constants.SESSION_ID_FIELD)).getValue();
					AggregateReportValue aggregateReportValue = null;
					if (data.containsKey(aggregateReportKey))
						aggregateReportValue = data.get(aggregateReportKey);
					else
						aggregateReportValue = new AggregateReportValue();
					if (interval.equals(Constants.MTD)) {
						aggregateReportValue.setSessionsMTD(sessionCount);
						aggregateReportValue.setUniqueVisitorsMTD(UniqueUserCount);
					} else if (interval.equals(Constants.DAY)) {
						aggregateReportValue.setSessions(sessionCount);
						aggregateReportValue.setUniqueVisitors(UniqueUserCount);
					}
					else if (interval.equals(Constants.PREV_30_DAYS)) {
						aggregateReportValue.setSessionsPrev30Days(sessionCount);
						aggregateReportValue.setUniqueVisitorsPrev30Days(UniqueUserCount);
					}

					data.put(aggregateReportKey, aggregateReportValue);

				}
			}
		}
	}

	void getRemainingUserSessions(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data,
			String target) {

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		String identifier = "";
		String identifierValue = "";

		if ("up".equalsIgnoreCase(target)) {
			identifier = "state";
			identifierValue = "up";
			boolQueryBuilder.should(QueryBuilders.termQuery(Constants.ABBREVIATIONS, "up"));
			boolQueryBuilder.should(QueryBuilders.prefixQuery(Constants.URL_FOLDERNAME, "/uttar-pradesh/")).minimumShouldMatch(1);
		} else if ("withoutUc".equalsIgnoreCase(target)){
			identifier = "filter";
			identifierValue = "withoutUc";
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, ucbTrackers));
		}

		String indexName[] = new String[] { "realtime_upvs_" + date.replaceAll("-", "_") };
		if (interval.equals(Constants.MTD)) {
			indexName = getIndexNamePatternMonth("realtime_upvs_", date);
		}


		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			indexName = IndexUtils.getDailyIndexes("realtime_upvs_",DateUtil.getPreviousDate(date.replaceAll("-","_"), -10).replaceAll("_","-"), date);
		}
		//indexName = new String[]{"realtime_upvs_2018_0*"};

		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE)
				.field(Constants.HOST_TYPE).size(1000)
				.subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID))
				.subAggregation(
						AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD));

		List<AggregationBuilder> listAggregation = new ArrayList<>();
		Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
		for (Map.Entry<String, List<String>> entry : set) {
			listAggregation.add(AggregationBuilders
					.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue()))
					.subAggregation(hostAggregation));
		}

		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(indexName,
				MappingTypes.MAPPING_REALTIME, boolQueryBuilder, 0, 0, null, null, null, listAggregation,
				null);

		Aggregations aggregationResponses = searchResponse.getAggregations();

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();

		for (String channel : parentChildChannelMappingKeySet) {
			Filter channelBucket = aggregationResponses.get(channel);
			for (Terms.Bucket hostBucket : ((Terms) channelBucket.getAggregations().get(Constants.HOST_TYPE))
					.getBuckets()) {
				String host = hostBucket.getKeyAsString();

				AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, identifierValue,
						identifier, date);
				double sessionCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESS_ID)).getValue();
				double UniqueUserCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESSION_ID_FIELD))
						.getValue();
				AggregateReportValue aggregateReportValue = null;

				if (data.containsKey(aggregateReportKey)) {
					aggregateReportValue = data.get(aggregateReportKey);
				} else {
					aggregateReportValue = new AggregateReportValue();
				}

				if (interval.equals(Constants.MTD)) {
					aggregateReportValue.setSessionsMTD(sessionCount);
					aggregateReportValue.setUniqueVisitorsMTD(UniqueUserCount);
				} else if (interval.equals(Constants.DAY)) {
					aggregateReportValue.setSessions(sessionCount);
					aggregateReportValue.setUniqueVisitors(UniqueUserCount);
				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					aggregateReportValue.setSessionsPrev30Days(sessionCount);
					aggregateReportValue.setUniqueVisitorsPrev30Days(UniqueUserCount);
				}

				data.put(aggregateReportKey, aggregateReportValue);
			}
		}
	}

	void getOverallUserSessions(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data,
			String target) {

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		String identifier = "";
		String identifierValue = "";

		if ("withoutUcOverall".equalsIgnoreCase(target)){
			identifier = "filter";
			identifierValue = "withoutUcOverall";
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, ucbTrackers));
		}
		else if ("overall".equalsIgnoreCase(target)){

			identifier = "filter";
			identifierValue = "overall";

		}

		String indexName[] = new String[] { "realtime_upvs_" + date.replaceAll("-", "_") };
		if (interval.equals(Constants.MTD)) {
			indexName = getIndexNamePatternMonth("realtime_upvs_", date);
		}	

		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			indexName = IndexUtils.getDailyIndexes("realtime_upvs_",DateUtil.getPreviousDate(date.replaceAll("-","_"), -10).replaceAll("_","-"), date);
		}
		//indexName = new String[]{"realtime_upvs_2018_0*"};

		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE)
				.field(Constants.HOST_TYPE).size(1000)
				.subAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID))
				.subAggregation(AggregationBuilders.cardinality(Constants.SESSION_ID_FIELD).field(Constants.SESSION_ID_FIELD));

		List<AggregationBuilder> listAggregation = new ArrayList<>();
		listAggregation.add(hostAggregation);

		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(indexName,
				MappingTypes.MAPPING_REALTIME, boolQueryBuilder, 0, 0, null, null, null, listAggregation,
				null);

		Aggregations aggregationResponses = searchResponse.getAggregations();

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();

		for (Terms.Bucket hostBucket : ((Terms)aggregationResponses.get(Constants.HOST_TYPE)).getBuckets()) {
			String host = hostBucket.getKeyAsString();
			String channel = "all";

			AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, identifierValue,
					identifier, date);
			double sessionCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESS_ID)).getValue();
			double UniqueUserCount = ((Cardinality) hostBucket.getAggregations().get(Constants.SESSION_ID_FIELD))
					.getValue();
			AggregateReportValue aggregateReportValue = null;

			if (data.containsKey(aggregateReportKey)) {
				aggregateReportValue = data.get(aggregateReportKey);
			} else {
				aggregateReportValue = new AggregateReportValue();
			}

			if (interval.equals(Constants.MTD)) {
				aggregateReportValue.setSessionsMTD(sessionCount);
				aggregateReportValue.setUniqueVisitorsMTD(UniqueUserCount);
			} else if (interval.equals(Constants.DAY)) {
				aggregateReportValue.setSessions(sessionCount);
				aggregateReportValue.setUniqueVisitors(UniqueUserCount);
			}
			else if (interval.equals(Constants.PREV_30_DAYS)) {
				aggregateReportValue.setSessionsPrev30Days(sessionCount);
				aggregateReportValue.setUniqueVisitorsPrev30Days(UniqueUserCount);
			}

			data.put(aggregateReportKey, aggregateReportValue);
		}
	}



	void getPageViews(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data) {

		BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
		String queryDate=date;
		if(interval.equals(Constants.MTD))
		{
			queryDate=queryDate.substring(0,7)+"-01";
		}
		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			queryDate = DateUtil.getPreviousDate(date.replaceAll("-","_"), -30).replaceAll("_","-");
		}

		boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(queryDate).lte(date));

		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE).field(Constants.HOST_TYPE).size(1000).
				subAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS)).
				subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS)).
				subAggregation(AggregationBuilders.terms(Constants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(1000).
						subAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS)).
						subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS))
						).
				subAggregation(AggregationBuilders.terms(Constants.SPL_TRACKER).field(Constants.SPL_TRACKER).size(1000).
						subAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS)).
						subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS))
						);

		List<AggregationBuilder> listAggregation = new ArrayList<>();
		Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
		for (Map.Entry<String, List<String>> entry : set) {
			listAggregation.add(AggregationBuilders.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue())).subAggregation(hostAggregation));
		}
		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, queryDate, date), MappingTypes.MAPPING_REALTIME,boolQueryBuilder,0,0,null,null,null,listAggregation,null);
		Aggregations aggregationResponses = searchResponse.getAggregations();

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();
		for (String channel : parentChildChannelMappingKeySet) {
			Filter channelBucket = aggregationResponses.get(channel);
			for (Terms.Bucket hostBucket : ((Terms) channelBucket.getAggregations().get(Constants.HOST_TYPE)).getBuckets()) {
				String host = hostBucket.getKeyAsString();
				
				AggregateReportKey overallAggregateReportKey = new AggregateReportKey(channel, host, "channelOverall", "filter", date);
				double overllUniquePageViews = ((Sum) hostBucket.getAggregations().get(Constants.UVS)).getValue();
				double overallPageViews = ((Sum) hostBucket.getAggregations().get(Constants.PVS)).getValue();
				AggregateReportValue overallAggregateReportValue = null;
				if (data.containsKey(overallAggregateReportKey))
					overallAggregateReportValue = data.get(overallAggregateReportKey);
				else
					overallAggregateReportValue = new AggregateReportValue();
				
				if (interval.equals(Constants.MTD)) {
					overallAggregateReportValue.setUniquePageViewsMTD(overllUniquePageViews);
					overallAggregateReportValue.setPageViewsMTD(overallPageViews);
				} else if (interval.equals(Constants.DAY)) {
					overallAggregateReportValue.setUniquePageViews(overllUniquePageViews);
					overallAggregateReportValue.setPageViews(overallPageViews);
				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					overallAggregateReportValue.setUniquePageViewsPrev30Days(overllUniquePageViews);
					overallAggregateReportValue.setPageViewsPrev30Days(overallPageViews);
				}
				data.put(overallAggregateReportKey, overallAggregateReportValue);
				
				for (Terms.Bucket superCatIdBucket : ((Terms) hostBucket.getAggregations().get(Constants.SUPER_CAT_NAME)).getBuckets()) {
					String superCatId = superCatIdBucket.getKeyAsString();
					AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, superCatId, Constants.SUPER_CAT_NAME, date);
					double uniquePageViews = ((Sum) superCatIdBucket.getAggregations().get(Constants.UVS)).getValue();
					double pageViews = ((Sum) superCatIdBucket.getAggregations().get(Constants.PVS)).getValue();
					AggregateReportValue aggregateReportValue = null;
					if (data.containsKey(aggregateReportKey))
						aggregateReportValue = data.get(aggregateReportKey);
					else
						aggregateReportValue = new AggregateReportValue();
					if (interval.equals(Constants.MTD)) {
						aggregateReportValue.setPageViewsMTD(pageViews);
						aggregateReportValue.setUniquePageViewsMTD(uniquePageViews);
					} 
					else if (interval.equals(Constants.DAY)) {
						aggregateReportValue.setPageViews(pageViews);
						aggregateReportValue.setUniquePageViews(uniquePageViews);
					}
					else if (interval.equals(Constants.PREV_30_DAYS)) {
						aggregateReportValue.setPageViewsPrev30Days(pageViews);
						aggregateReportValue.setUniquePageViewsPrev30Days(uniquePageViews);

					}
					data.put(aggregateReportKey, aggregateReportValue);
				}

				for (Terms.Bucket splTrackerBucket : ((Terms) hostBucket.getAggregations().get(Constants.SPL_TRACKER)).getBuckets()) {
					String splTracker = splTrackerBucket.getKeyAsString();
					AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, splTracker, Constants.SPL_TRACKER, date);
					double uniquePageViews = ((Sum) splTrackerBucket.getAggregations().get(Constants.UVS)).getValue();
					double pageViews = ((Sum) splTrackerBucket.getAggregations().get(Constants.PVS)).getValue();
					AggregateReportValue aggregateReportValue = null;
					if (data.containsKey(aggregateReportKey))
						aggregateReportValue = data.get(aggregateReportKey);
					else
						aggregateReportValue = new AggregateReportValue();
					if (interval.equals(Constants.MTD)) {
						aggregateReportValue.setPageViewsMTD(pageViews);
						aggregateReportValue.setUniquePageViewsMTD(uniquePageViews);
					} 
					else if (interval.equals(Constants.DAY)) {
						aggregateReportValue.setPageViews(pageViews);
						aggregateReportValue.setUniquePageViews(uniquePageViews);

					}
					else if (interval.equals(Constants.PREV_30_DAYS)) {
						aggregateReportValue.setPageViewsPrev30Days(pageViews);
						aggregateReportValue.setUniquePageViewsPrev30Days(uniquePageViews);

					}
					data.put(aggregateReportKey, aggregateReportValue);
				}
			}

		}
	}

	void getRemainingPageViews(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data,
			String target) {

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		String identifier = "";
		String identifierValue = "";

		if ("up".equalsIgnoreCase(target)) {
			identifier = "state";
			identifierValue = "up";
			boolQueryBuilder.should(QueryBuilders.termQuery(Constants.ABBREVIATIONS, "up"));
			boolQueryBuilder.should(QueryBuilders.prefixQuery(Constants.URL_FOLDERNAME, "/uttar-pradesh/")).minimumShouldMatch(1);
		} else if ("withoutUc".equalsIgnoreCase(target)){
			identifier = "filter";
			identifierValue = "withoutUc";
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, ucbTrackers));
		}

		String queryDate = date;
		if (interval.equals(Constants.MTD)) {
			queryDate = queryDate.substring(0, 7)+"-01";
		}
		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			queryDate = DateUtil.getPreviousDate(date.replaceAll("-","_"), -30).replaceAll("_","-");
		}

		boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(queryDate).lte(date));

		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE)
				.field(Constants.HOST_TYPE).size(1000)
				.subAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS))
				.subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS));

		List<AggregationBuilder> listAggregation = new ArrayList<>();
		Set<Map.Entry<String, List<String>>> set = parentChildChannelMapping.entrySet();
		for (Map.Entry<String, List<String>> entry : set) {
			listAggregation.add(AggregationBuilders
					.filter(entry.getKey(), QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, entry.getValue()))
					.subAggregation(hostAggregation));
		}
		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, queryDate, date),
				MappingTypes.MAPPING_REALTIME, boolQueryBuilder, 0, 0, null, null, null, listAggregation, null);

		Aggregations aggregationResponses = searchResponse.getAggregations();

		Set<String> parentChildChannelMappingKeySet = parentChildChannelMapping.keySet();
		for (String channel : parentChildChannelMappingKeySet) {
			Filter channelBucket = aggregationResponses.get(channel);
			for (Terms.Bucket hostBucket : ((Terms) channelBucket.getAggregations().get(Constants.HOST_TYPE))
					.getBuckets()) {
				String host = hostBucket.getKeyAsString();

				AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, identifierValue,
						identifier, date);
				double uniquePageViews = ((Sum) hostBucket.getAggregations().get(Constants.UVS)).getValue();
				double pageViews = ((Sum) hostBucket.getAggregations().get(Constants.PVS)).getValue();

				AggregateReportValue aggregateReportValue = null;
				if (data.containsKey(aggregateReportKey))
					aggregateReportValue = data.get(aggregateReportKey);
				else
					aggregateReportValue = new AggregateReportValue();
				if (interval.equals(Constants.MTD)) {
					aggregateReportValue.setPageViewsMTD(pageViews);
					aggregateReportValue.setUniquePageViewsMTD(uniquePageViews);
				} 
				else if (interval.equals(Constants.DAY)) {
					aggregateReportValue.setPageViews(pageViews);
					aggregateReportValue.setUniquePageViews(uniquePageViews);

				}
				else if (interval.equals(Constants.PREV_30_DAYS)) {
					aggregateReportValue.setPageViewsPrev30Days(pageViews);
					aggregateReportValue.setUniquePageViewsPrev30Days(uniquePageViews);

				}
				data.put(aggregateReportKey, aggregateReportValue);
			}
		}
	}

	void getOverallPageViews(String date, String interval, Map<AggregateReportKey, AggregateReportValue> data,
			String target) {

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		String identifier = "";
		String identifierValue = "";

		if ("withoutUcOverall".equalsIgnoreCase(target)){
			identifier = "filter";
			identifierValue = "withoutUcOverall";
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, ucbTrackers));
		}

		else if ("overall".equalsIgnoreCase(target)){
			identifier = "filter";
			identifierValue = "overall";		
		}

		String queryDate = date;

		if (interval.equals(Constants.MTD)) {
			queryDate = queryDate.substring(0, 7)+"-01";
		}

		else if(interval.equals(Constants.PREV_30_DAYS))
		{
			queryDate = DateUtil.getPreviousDate(date.replaceAll("-","_"), -30).replaceAll("_","-");
		}

		boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(queryDate).lte(date));

		TermsAggregationBuilder hostAggregation = AggregationBuilders.terms(Constants.HOST_TYPE)
				.field(Constants.HOST_TYPE).size(1000)
				.subAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS))
				.subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS));

		List<AggregationBuilder> listAggregation = new ArrayList<>();
		listAggregation.add(hostAggregation);
		SearchResponse searchResponse = elasticSearchIndexService.getSearchResponse1(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, queryDate, date),
				MappingTypes.MAPPING_REALTIME, boolQueryBuilder, 0, 0, null, null, null, listAggregation, null);

		Aggregations aggregationResponses = searchResponse.getAggregations();

		for (Terms.Bucket hostBucket : ((Terms) aggregationResponses.get(Constants.HOST_TYPE)).getBuckets()) {
			String host = hostBucket.getKeyAsString();
			String channel = "all";
			AggregateReportKey aggregateReportKey = new AggregateReportKey(channel, host, identifierValue,
					identifier, date);
			double uniquePageViews = ((Sum) hostBucket.getAggregations().get(Constants.UVS)).getValue();
			double pageViews = ((Sum) hostBucket.getAggregations().get(Constants.PVS)).getValue();

			AggregateReportValue aggregateReportValue = null;
			if (data.containsKey(aggregateReportKey))
				aggregateReportValue = data.get(aggregateReportKey);
			else
				aggregateReportValue = new AggregateReportValue();
			if (interval.equals(Constants.MTD)) {
				aggregateReportValue.setPageViewsMTD(pageViews);
				aggregateReportValue.setUniquePageViewsMTD(uniquePageViews);
			} 
			else if (interval.equals(Constants.DAY)) {
				aggregateReportValue.setPageViews(pageViews);
				aggregateReportValue.setUniquePageViews(uniquePageViews);

			}
			else if (interval.equals(Constants.PREV_30_DAYS)) {
				aggregateReportValue.setPageViewsPrev30Days(pageViews);
				aggregateReportValue.setUniquePageViewsPrev30Days(uniquePageViews);

			}
			data.put(aggregateReportKey, aggregateReportValue);
		}
	}




	private String[] getIndexNamePatternMonth(String prefix,String endDate) {
		String startDate=endDate.replaceAll("-\\d+?$", "-01");
		return endDate == null ? null : IndexUtils.getDailyIndexes(prefix,startDate,endDate);
	}

	public static void main(String[] args) {
		String date = DateUtil.getPreviousDate().replaceAll("_","-");
		//date = "2018-04-05";
		log.info("Preparing aggregate report for " + date);
		AggregateReport aggregateReport = new AggregateReport();
		long startTime=System.currentTimeMillis();
		Map<AggregateReportKey, AggregateReportValue> data = new HashMap<>();
		aggregateReport.getPageViews(date, "day", data);
		aggregateReport.getRemainingPageViews(date, "day", data, "up");
		aggregateReport.getRemainingPageViews(date, "day", data, "withoutUc");
		aggregateReport.getOverallPageViews(date, "day", data, "withoutUcOverall");
		aggregateReport.getOverallPageViews(date, "day", data, "overall");
		log.info("page and unique page views report done for " + date);    	
		aggregateReport.getPageViews(date, "mtd", data);
		aggregateReport.getRemainingPageViews(date, "mtd", data, "up");
		aggregateReport.getRemainingPageViews(date, "mtd", data, "withoutUc");
		aggregateReport.getOverallPageViews(date, "mtd", data, "withoutUcOverall");
		aggregateReport.getOverallPageViews(date, "mtd", data, "overall");
		log.info("page and unique page views report done for month " + date);
		aggregateReport.getPageViews(date, "prev_30_days", data);
		aggregateReport.getRemainingPageViews(date, "prev_30_days", data, "up");
		aggregateReport.getRemainingPageViews(date, "prev_30_days", data, "withoutUc");
		aggregateReport.getOverallPageViews(date, "prev_30_days", data, "withoutUcOverall");
		aggregateReport.getOverallPageViews(date, "prev_30_days", data, "overall");
		log.info("page and unique page views report done for prev 30 days " + date);
		aggregateReport.getUserSessions(date, "day", data);
		aggregateReport.getRemainingUserSessions(date, "day", data, "up");
		aggregateReport.getRemainingUserSessions(date, "day", data, "withoutUc");
		aggregateReport.getOverallUserSessions(date, "day", data, "withoutUcOverall");
		aggregateReport.getOverallUserSessions(date, "day", data, "overall");
		log.info("sessions and unique users report done for " + date);
		aggregateReport.getUserSessions(date, "mtd", data);
		aggregateReport.getRemainingUserSessions(date, "mtd", data, "up");
		aggregateReport.getRemainingUserSessions(date, "mtd", data, "withoutUc");
		aggregateReport.getOverallUserSessions(date, "mtd", data, "withoutUcOverall");
		aggregateReport.getOverallUserSessions(date, "mtd", data, "overall");
		log.info("sessions and unique users report done for month " + date);
		aggregateReport.getUserSessions(date, "prev_30_days", data);
		aggregateReport.getRemainingUserSessions(date, "prev_30_days", data, "up");
		aggregateReport.getRemainingUserSessions(date, "prev_30_days", data, "withoutUc");
		aggregateReport.getOverallUserSessions(date, "prev_30_days", data, "withoutUcOverall");
		aggregateReport.getOverallUserSessions(date, "prev_30_days", data, "overall");
		log.info("sessions and unique users report done for prev_30_days " + date);
		aggregateReport.index(data);
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
		elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_REPORT,DateUtil.getPreviousDate().replaceAll("_","-"), DateUtil.getPreviousDate().replaceAll("_","-"))[0], MappingTypes.MAPPING_REALTIME,
				listData);
	}

	//todo mapping for new index-satya
	//todo if cron is working-satya
	//todo retrieval API-Rakesh
	//todo up,epaper-Piyush

}
