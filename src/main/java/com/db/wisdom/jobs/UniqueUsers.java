package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.constants.Constants.CricketConstants.SessionTypeConstants;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;

public class UniqueUsers {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> uvsData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(UniqueUsers.class);

	public void insertData(String date) {
		try{
			String indexNameForDay = "realtime_upvs_"+date.replaceAll("-", "_");
			String[] indexNameForMonth = IndexUtils.getDailyIndexes("realtime_upvs_", "2018-01-23", date);
			SearchResponse resForDay = client.prepareSearch(indexNameForDay).setTypes(MappingTypes.MAPPING_REALTIME)
					.addAggregation(AggregationBuilders.terms("host").field(Constants.HOST)
							.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))
							.subAggregation(AggregationBuilders.terms("channel").field(Constants.CHANNEL_SLNO)
									.subAggregation(AggregationBuilders.terms("superCatId").field(Constants.SUPER_CAT_ID)
											.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))	
											)
									.subAggregation(AggregationBuilders.terms("special_tracker").field(Constants.SPL_TRACKER)
											.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))	
											)									
									.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))
									.size(100))
							.size(10))
					.setSize(0).execute().actionGet(); 

			SearchResponse resForMonth = client.prepareSearch(indexNameForMonth).setTypes(MappingTypes.MAPPING_REALTIME)
					.addAggregation(AggregationBuilders.terms("host").field(Constants.HOST)
							.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))
							.subAggregation(AggregationBuilders.terms("channel").field(Constants.CHANNEL_SLNO)
									.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))
									.subAggregation(AggregationBuilders.terms("superCatId").field(Constants.SUPER_CAT_ID)
											.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))		
											)
									.subAggregation(AggregationBuilders.terms("special_tracker").field(Constants.SPL_TRACKER)
											.subAggregation(AggregationBuilders.cardinality("userCount").field(Constants.SESSION_ID_FIELD))	
											)
									.size(100))
							.size(10))
					.setSize(0).execute().actionGet(); 

			String id;
			Terms hostTerms = resForDay.getAggregations().get("host");
			for(Terms.Bucket hostBucket:hostTerms.getBuckets()){
				Map<String, Object> uvMap = new HashMap<>();
				long usercount = 0;
				String host = hostBucket.getKeyAsString();
				Cardinality userCountAgg = hostBucket.getAggregations().get("userCount");
				usercount = userCountAgg.getValue();
				id = date.replaceAll("-", "_")+"_"+host;
				uvMap.put(Constants.UVS, usercount);
				uvMap.put(Constants.DATE, date);
				uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				uvMap.put(Constants.INTERVAL, Constants.DAY);
				uvMap.put(Constants.HOST, host);
				uvMap.put(Constants.DOMAINTYPE, Constants.HOST);
				uvMap.put(Constants.ROWID, id);
				uvsData.add(uvMap);

				Terms channelTerms = hostBucket.getAggregations().get("channel");
				for(Terms.Bucket channelBucket:channelTerms.getBuckets()){
					String channel = channelBucket.getKeyAsString();
					uvMap = new HashMap<>();
					usercount = 0;
					userCountAgg = channelBucket.getAggregations().get("userCount");
					usercount = userCountAgg.getValue();
					id = date.replaceAll("-", "_")+"_"+host+"_"+channel;

					uvMap.put(Constants.UVS, usercount);
					uvMap.put(Constants.DATE, date);
					uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					uvMap.put(Constants.INTERVAL, Constants.DAY);
					uvMap.put(Constants.HOST, host);
					uvMap.put(Constants.CHANNEL_SLNO, channel);
					uvMap.put(Constants.DOMAINTYPE, Constants.CHANNEL_SLNO);
					uvMap.put(Constants.ROWID, id);
					uvsData.add(uvMap);

					Terms sup_cat_Terms = channelBucket.getAggregations().get("superCatId");
					for(Terms.Bucket super_cat_id_bucket :sup_cat_Terms.getBuckets()){
						uvMap = new HashMap<>();
						usercount = 0;						
						
						Integer superCatId = Integer.valueOf(super_cat_id_bucket.getKeyAsString());						
						
						userCountAgg = super_cat_id_bucket.getAggregations().get("userCount");
						usercount = userCountAgg.getValue();
						id = date.replaceAll("-", "_")+"_"+host+"_"+channel+"_"+superCatId;

						uvMap.put(Constants.SUPER_CAT_ID, superCatId);
						uvMap.put(Constants.UVS, usercount);
						uvMap.put(Constants.DATE, date);
						uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						uvMap.put(Constants.INTERVAL, Constants.DAY);
						uvMap.put(Constants.HOST, host);
						uvMap.put(Constants.CHANNEL_SLNO, channel);
						uvMap.put(Constants.DOMAINTYPE, Constants.SUPER_CAT_ID);						
						uvMap.put(Constants.ROWID, id);
						uvsData.add(uvMap);
					}				

					// ## Preparing doc for special tracker 
					
					Terms specialTrackersTerms = channelBucket.getAggregations().get("special_tracker");
					for(Terms.Bucket special_tracker_bucket :specialTrackersTerms.getBuckets()){
						uvMap = new HashMap<>();
						usercount = 0;						
						
						Integer special_tracker = Integer.valueOf(special_tracker_bucket.getKeyAsString());						
						
						userCountAgg = special_tracker_bucket.getAggregations().get("userCount");
						usercount = userCountAgg.getValue();
						id = date.replaceAll("-", "_")+"_"+host+"_"+channel+"_"+special_tracker;

						uvMap.put(Constants.SPL_TRACKER, special_tracker);
						uvMap.put(Constants.UVS, usercount);
						uvMap.put(Constants.DATE, date);
						uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						uvMap.put(Constants.INTERVAL, Constants.DAY);
						uvMap.put(Constants.HOST, host);
						uvMap.put(Constants.CHANNEL_SLNO, channel);
						uvMap.put(Constants.DOMAINTYPE, Constants.SPL_TRACKER);						
						uvMap.put(Constants.ROWID, id);
						uvsData.add(uvMap);

					}					
					
					if (uvsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.UVS_COUNT), MappingTypes.MAPPING_REALTIME,
								uvsData);
						log.info("Records inserted in uvs_count index, size: " + uvsData.size());
						System.out.println("Records inserted in uvs_count index, size: " + uvsData.size());
						uvsData.clear();
					}

				}

			}

			hostTerms = resForMonth.getAggregations().get("host");
			for(Terms.Bucket hostBucket:hostTerms.getBuckets()){
				Map<String, Object> uvMap = new HashMap<>();
				long usercount = 0;
				String host = hostBucket.getKeyAsString();
				Cardinality userCountAgg = hostBucket.getAggregations().get("userCount");
				usercount = userCountAgg.getValue();
				id = date.replaceAll("-", "_").substring(0, 7)+"_"+host;
				uvMap.put(Constants.UVS, usercount);
				uvMap.put(Constants.DATE, date);
				uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				uvMap.put(Constants.INTERVAL, Constants.MONTH);
				uvMap.put(Constants.HOST, host);
				uvMap.put(Constants.DOMAINTYPE, Constants.HOST);
				uvMap.put(Constants.ROWID, id);
				uvsData.add(uvMap);

				Terms channelTerms = hostBucket.getAggregations().get("channel");
				for(Terms.Bucket channelBucket:channelTerms.getBuckets()){
					String channel = channelBucket.getKeyAsString();
					uvMap = new HashMap<>();
					usercount = 0;
					userCountAgg = channelBucket.getAggregations().get("userCount");
					usercount = userCountAgg.getValue();
					id = date.replaceAll("-", "_").substring(0, 7)+"_"+host+"_"+channel;
					uvMap.put(Constants.UVS, usercount);
					uvMap.put(Constants.DATE, date);
					uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					uvMap.put(Constants.INTERVAL, Constants.MONTH);
					uvMap.put(Constants.HOST, host);
					uvMap.put(Constants.CHANNEL_SLNO, channel);
					uvMap.put(Constants.DOMAINTYPE, Constants.CHANNEL_SLNO);
					uvMap.put(Constants.ROWID, id);
					uvsData.add(uvMap);
					
					Terms sup_cat_Terms = channelBucket.getAggregations().get("superCatId");
					for(Terms.Bucket super_cat_id_bucket :sup_cat_Terms.getBuckets()){
						uvMap = new HashMap<>();
						usercount = 0;						
						Integer superCatId = Integer.valueOf(super_cat_id_bucket.getKeyAsString());						
						userCountAgg = super_cat_id_bucket.getAggregations().get("userCount");
						usercount = userCountAgg.getValue();
						id = date.replaceAll("-", "_").substring(0, 7)+"_"+host+"_"+channel+"_"+superCatId;

						uvMap.put(Constants.SUPER_CAT_ID, superCatId);
						uvMap.put(Constants.UVS, usercount);
						uvMap.put(Constants.DATE, date);
						uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						uvMap.put(Constants.INTERVAL, Constants.MONTH);
						uvMap.put(Constants.HOST, host);
						uvMap.put(Constants.CHANNEL_SLNO, channel);
						uvMap.put(Constants.DOMAINTYPE, Constants.SUPER_CAT_ID);						
						uvMap.put(Constants.ROWID, id);
						uvsData.add(uvMap);

					}

					// ## Preparing doc for special tracker 
					
					Terms specialTrackersTerms = channelBucket.getAggregations().get("special_tracker");
					for(Terms.Bucket special_tracker_bucket :specialTrackersTerms.getBuckets()){
						uvMap = new HashMap<>();
						usercount = 0;						
						
						Integer special_tracker = Integer.valueOf(special_tracker_bucket.getKeyAsString());						
						
						userCountAgg = special_tracker_bucket.getAggregations().get("userCount");
						usercount = userCountAgg.getValue();
						id = date.replaceAll("-", "_").substring(0, 7)+"_"+host+"_"+channel+"_"+special_tracker;

						uvMap.put(Constants.SPL_TRACKER, special_tracker);
						uvMap.put(Constants.UVS, usercount);
						uvMap.put(Constants.DATE, date);
						uvMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						uvMap.put(Constants.INTERVAL, Constants.MONTH);
						uvMap.put(Constants.HOST, host);
						uvMap.put(Constants.CHANNEL_SLNO, channel);
						uvMap.put(Constants.DOMAINTYPE, Constants.SPL_TRACKER);						
						uvMap.put(Constants.ROWID, id);
						uvsData.add(uvMap);

					}
					
					if (uvsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.UVS_COUNT), MappingTypes.MAPPING_REALTIME,
								uvsData);
						log.info("Records inserted in uvs_count index, size: " + uvsData.size());
						System.out.println("Records inserted in uvs_count index, size: " + uvsData.size());
						uvsData.clear();
					}

				}

			}

			if (uvsData.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.UVS_COUNT), MappingTypes.MAPPING_REALTIME,
						uvsData);
				log.info("Records inserted in uvs_count index, size: " + uvsData.size());
				System.out.println("Records inserted in uvs_count index, size: " + uvsData.size());
				uvsData.clear();
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		UniqueUsers sci = new UniqueUsers();
		//sci.insertData("2018-02-10");
		//sci.insertData("2018-02-11");
		sci.insertData("2018-03-13");
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
		System.out.println("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}


}
