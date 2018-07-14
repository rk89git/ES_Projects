package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class FbPageInsightsHourlyIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> fbHourlyData = new ArrayList<Map<String, Object>>();

	public void insertData(String date) {
		int channelCount=100;

		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb.must(QueryBuilders.rangeQuery(Constants.CRON_DATETIME).lte(date).gte(date));
		
		SearchResponse res = client.prepareSearch(Indexes.FB_PAGE_INSIGHTS).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(bqb)
				.addAggregation(AggregationBuilders.terms("channel").field(Constants.CHANNEL_SLNO).size(channelCount)
						.subAggregation(AggregationBuilders.dateHistogram("hour").field(Constants.CRON_DATETIME).dateHistogramInterval(DateHistogramInterval.hours(1))
								.subAggregation(AggregationBuilders.topHits("topHit").size(1).sort(Constants.CRON_DATETIME, SortOrder.DESC))))
				.setSize(0).execute().actionGet(); 

		Terms terms = res.getAggregations().get("channel");
		System.out.println("Static size of stories: "+channelCount+"; Stories found: "+terms.getBuckets().size());

		for(Terms.Bucket channel:terms.getBuckets()){		

			Integer prev_ia_all_views = 0;
			Integer prev_page_unique_views = 0;
			Integer prev_page_views = 0;
			Integer prev_page_unlikes = 0;
			Integer prev_page_likes = 0;
			Integer prev_page_positive_feedback_by_type = 0;
			Integer prev_page_negative_feedback_by_type = 0;			
			Integer prev_page_negative_feedback = 0;
			Integer prev_page_consumptions_by_consumption_type = 0;
			Integer prev_page_consumptions = 0;
			Integer prev_page_post_engagements = 0;			
             
			/*
			 * To derive data of 0th Hour data. It would be fetched by negating
			 * 23rd hour data of last day.
			 */

			BoolQueryBuilder prevDayLastRecordQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, channel.getKey()))
					.must(QueryBuilders.rangeQuery(Constants.CRON_DATETIME)
							.gte(DateUtil.getPreviousDate(date, "yyyy-MM-dd", -10))
							.lte(DateUtil.getPreviousDate(date, "yyyy-MM-dd")));

			SearchResponse prevRes = client.prepareSearch(Indexes.FB_PAGE_INSIGHTS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(prevDayLastRecordQuery).setSize(1)
					.addSort(Constants.CRON_DATETIME, SortOrder.DESC).execute().actionGet();
			if(prevRes.getHits().getHits().length>0){
				Map<String, Object> map = prevRes.getHits().getHits()[0].getSource();
				
				if(map.get(Constants.IA_ALL_VIEWS)!=null){
					prev_ia_all_views = (Integer)map.get(Constants.IA_ALL_VIEWS);
				}
				
				if(map.get(Constants.PAGE_UNIQUE_VIEWS)!=null){
					prev_page_unique_views = (Integer)map.get(Constants.PAGE_UNIQUE_VIEWS);
				}
				
				if(map.get(Constants.PAGE_VIEWS)!=null){
					prev_page_views = (Integer)map.get(Constants.PAGE_VIEWS);
				}
				
				if(map.get(Constants.PAGE_UNLIKES)!=null){
					prev_page_unlikes = (Integer)map.get(Constants.PAGE_UNLIKES);
				}
				
				if(map.get(Constants.PAGE_LIKES)!=null){
					prev_page_likes = (Integer)map.get(Constants.PAGE_LIKES);
				}
				
				if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE)!=null){
					prev_page_negative_feedback_by_type = (Integer)map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE);
				}
				
				if(map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE)!=null){
					prev_page_positive_feedback_by_type = (Integer)map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE);
				}
				
				if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK)!=null){
					prev_page_negative_feedback = (Integer)map.get(Constants.PAGE_NEGATIVE_FEEDBACK);
				}
				
				if(map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE)!=null){
					prev_page_consumptions_by_consumption_type = (Integer)map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE);
				}
				
				if(map.get(Constants.PAGE_CONSUMPTIONS)!=null){
					prev_page_consumptions = (Integer)map.get(Constants.PAGE_CONSUMPTIONS);
				}
				
				if(map.get(Constants.PAGE_POST_ENGAGEMENTS)!=null){
					prev_page_post_engagements = (Integer)map.get(Constants.PAGE_POST_ENGAGEMENTS);
				}
			}

			Histogram interval = channel.getAggregations().get("hour");

			for (Histogram.Bucket bucket : interval.getBuckets()) {
				TopHits topHits = bucket.getAggregations().get("topHit");					
				Map<String, Object> map = topHits.getHits().getHits()[0].getSource();
				Map<String, Object> data = new HashMap<>(map);

				String rowId = map.get(Constants.CHANNEL_SLNO) + "_" + bucket.getKeyAsString().split(":")[0].replaceAll("T", "_");

				data.put(Constants.ROWID, rowId);
				
				if(map.get(Constants.IA_ALL_VIEWS)!=null){
					Integer ia_all_views = 0;
					if(map.get(Constants.IA_ALL_VIEWS) instanceof Integer){
						ia_all_views =  (Integer)map.get(Constants.IA_ALL_VIEWS);
					} else if (map.get(Constants.IA_ALL_VIEWS) instanceof String) {
						ia_all_views = Integer.parseInt(map.get(Constants.IA_ALL_VIEWS).toString());
					}
					// Handle negative values
					if (ia_all_views - prev_ia_all_views >= 0) {
						data.put(Constants.IA_ALL_VIEWS,ia_all_views-prev_ia_all_views);
						prev_ia_all_views = ia_all_views;				
					} else {
						data.put(Constants.IA_ALL_VIEWS, 0);						
					}

				}
				
				if(map.get(Constants.PAGE_UNIQUE_VIEWS)!=null){
					Integer page_unique_views = 0;
					if(map.get(Constants.PAGE_UNIQUE_VIEWS) instanceof Integer){
						page_unique_views =  (Integer)map.get(Constants.PAGE_UNIQUE_VIEWS);
					} else if (map.get(Constants.PAGE_UNIQUE_VIEWS) instanceof String) {
						page_unique_views = Integer.parseInt(map.get(Constants.PAGE_UNIQUE_VIEWS).toString());
					}
					// Handle negative values
					if (page_unique_views - prev_page_unique_views >= 0) {
						data.put(Constants.PAGE_UNIQUE_VIEWS,page_unique_views-prev_page_unique_views);
						prev_page_unique_views = page_unique_views;				
					} else {
						data.put(Constants.PAGE_UNIQUE_VIEWS, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_VIEWS)!=null){
					Integer page_views = 0;
					if(map.get(Constants.PAGE_VIEWS) instanceof Integer){
						page_views =  (Integer)map.get(Constants.PAGE_VIEWS);
					} else if (map.get(Constants.PAGE_VIEWS) instanceof String) {
						page_views = Integer.parseInt(map.get(Constants.PAGE_VIEWS).toString());
					}
					// Handle negative values
					if (page_views - prev_page_views >= 0) {
						data.put(Constants.PAGE_VIEWS,page_views-prev_page_views);
						prev_page_views = page_views;				
					} else {
						data.put(Constants.PAGE_VIEWS, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_UNLIKES)!=null){
					Integer page_unlikes = 0;
					if(map.get(Constants.PAGE_UNLIKES) instanceof Integer){
						page_unlikes =  (Integer)map.get(Constants.PAGE_UNLIKES);
					} else if (map.get(Constants.PAGE_UNLIKES) instanceof String) {
						page_unlikes = Integer.parseInt(map.get(Constants.PAGE_UNLIKES).toString());
					}
					// Handle negative values
					if (page_unlikes - prev_page_unlikes >= 0) {
						data.put(Constants.PAGE_UNLIKES,page_unlikes-prev_page_unlikes);
						prev_page_unlikes = page_unlikes;				
					} else {
						data.put(Constants.PAGE_UNLIKES, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_LIKES)!=null){
					Integer page_likes = 0;
					if(map.get(Constants.PAGE_LIKES) instanceof Integer){
						page_likes =  (Integer)map.get(Constants.PAGE_LIKES);
					} else if (map.get(Constants.PAGE_LIKES) instanceof String) {
						page_likes = Integer.parseInt(map.get(Constants.PAGE_LIKES).toString());
					}
					// Handle negative values
					if (page_likes - prev_page_likes >= 0) {
						data.put(Constants.PAGE_LIKES,page_likes-prev_page_likes);
						prev_page_likes = page_likes;				
					} else {
						data.put(Constants.PAGE_LIKES, 0);						
					}

				}
				
				if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE)!=null){
					Integer page_negative_feedback_by_type = 0;
					if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE) instanceof Integer){
						page_negative_feedback_by_type =  (Integer)map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE);
					} else if (map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE) instanceof String) {
						page_negative_feedback_by_type = Integer.parseInt(map.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE).toString());
					}
					// Handle negative values
					if (page_negative_feedback_by_type - prev_page_negative_feedback_by_type >= 0) {
						data.put(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE,page_negative_feedback_by_type-prev_page_negative_feedback_by_type);
						prev_page_negative_feedback_by_type = page_negative_feedback_by_type;				
					} else {
						data.put(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE)!=null){
					Integer page_positive_feedback_by_type = 0;
					if(map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE) instanceof Integer){
						page_positive_feedback_by_type =  (Integer)map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE);
					} else if (map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE) instanceof String) {
						page_positive_feedback_by_type = Integer.parseInt(map.get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE).toString());
					}
					// Handle negative values
					if (page_positive_feedback_by_type - prev_page_positive_feedback_by_type >= 0) {
						data.put(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE,page_positive_feedback_by_type-prev_page_positive_feedback_by_type);
						prev_page_positive_feedback_by_type = page_positive_feedback_by_type;				
					} else {
						data.put(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK)!=null){
					Integer page_negative_feedback = 0;
					if(map.get(Constants.PAGE_NEGATIVE_FEEDBACK) instanceof Integer){
						page_negative_feedback =  (Integer)map.get(Constants.PAGE_NEGATIVE_FEEDBACK);
					} else if (map.get(Constants.PAGE_NEGATIVE_FEEDBACK) instanceof String) {
						page_negative_feedback = Integer.parseInt(map.get(Constants.PAGE_NEGATIVE_FEEDBACK).toString());
					}
					// Handle negative values
					if (page_negative_feedback - prev_page_negative_feedback >= 0) {
						data.put(Constants.PAGE_NEGATIVE_FEEDBACK,page_negative_feedback-prev_page_negative_feedback);
						prev_page_negative_feedback = page_negative_feedback;				
					} else {
						data.put(Constants.PAGE_NEGATIVE_FEEDBACK, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE)!=null){
					Integer page_consumptions_by_consumption_type = 0;
					if(map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE) instanceof Integer){
						page_consumptions_by_consumption_type =  (Integer)map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE);
					} else if (map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE) instanceof String) {
						page_consumptions_by_consumption_type = Integer.parseInt(map.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE).toString());
					}
					// Handle negative values
					if (page_consumptions_by_consumption_type - prev_page_consumptions_by_consumption_type >= 0) {
						data.put(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE,page_consumptions_by_consumption_type-prev_page_consumptions_by_consumption_type);
						prev_page_consumptions_by_consumption_type = page_consumptions_by_consumption_type;				
					} else {
						data.put(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_CONSUMPTIONS)!=null){
					Integer page_consumptions = 0;
					if(map.get(Constants.PAGE_CONSUMPTIONS) instanceof Integer){
						page_consumptions =  (Integer)map.get(Constants.PAGE_CONSUMPTIONS);
					} else if (map.get(Constants.PAGE_CONSUMPTIONS) instanceof String) {
						page_consumptions = Integer.parseInt(map.get(Constants.PAGE_CONSUMPTIONS).toString());
					}
					// Handle negative values
					if (page_consumptions - prev_page_consumptions >= 0) {
						data.put(Constants.PAGE_CONSUMPTIONS,page_consumptions-prev_page_consumptions);
						prev_page_consumptions = page_consumptions;				
					} else {
						data.put(Constants.PAGE_CONSUMPTIONS, 0);						
					}

				}	
				
				if(map.get(Constants.PAGE_POST_ENGAGEMENTS)!=null){
					Integer page_post_engagements = 0;
					if(map.get(Constants.PAGE_POST_ENGAGEMENTS) instanceof Integer){
						page_post_engagements =  (Integer)map.get(Constants.PAGE_POST_ENGAGEMENTS);
					} else if (map.get(Constants.PAGE_POST_ENGAGEMENTS) instanceof String) {
						page_post_engagements = Integer.parseInt(map.get(Constants.PAGE_POST_ENGAGEMENTS).toString());
					}
					// Handle negative values
					if (page_post_engagements - prev_page_post_engagements >= 0) {
						data.put(Constants.PAGE_POST_ENGAGEMENTS,page_post_engagements-prev_page_post_engagements);
						prev_page_post_engagements = page_post_engagements;				
					} else {
						data.put(Constants.PAGE_POST_ENGAGEMENTS, 0);						
					}

				}				
				fbHourlyData.add(data);
			}

			if (fbHourlyData.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(Indexes.FB_PAGE_INSIGHTS_HOURLY, MappingTypes.MAPPING_REALTIME,
						fbHourlyData);
				System.out.println("Records inserted in fb_page_insights_hourly index, size: " + fbHourlyData.size());
				fbHourlyData.clear();
			}

		}

	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		FbPageInsightsHourlyIndexer fb = new FbPageInsightsHourlyIndexer();


		/*List<String> dateList;
		try {
			dateList = DateUtil.getDates(args[0], args[1]);
			for(String date:dateList)
			{ 
				System.out.println("==================================="+date+"===========================");
				fb.insertData(date.replaceAll("_", "-")); 
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block	
			e.printStackTrace();
		} 
*/
		//fb.insertData(DateUtil.getCurrentDate().replaceAll("_", "-"));
		fb.insertData("2017-09-12");
		System.out.println("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
