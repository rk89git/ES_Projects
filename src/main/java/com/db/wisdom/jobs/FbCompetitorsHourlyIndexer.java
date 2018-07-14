package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;

public class FbCompetitorsHourlyIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> fbHourlyData = new ArrayList<Map<String, Object>>();
	
	int batchSize = 1000;
	
	private static Logger log = LogManager.getLogger(FbCompetitorsHourlyIndexer.class);
	
	public void insertData(String date) {
		int storyCount=20000;

		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(date).gte(date));
		
		String[] indexName = IndexUtils.getMonthlyIndexes(Indexes.FB_COMPETITOR_HISTORY, date, DateUtil.getPreviousDate(date, "yyyy-MM-dd", -10));

		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(bqb)
				.addAggregation(AggregationBuilders.terms("story").field(Constants.STORY_ID_FIELD).size(storyCount)
						.subAggregation(AggregationBuilders.dateHistogram("hour").field(Constants.DATE_TIME_FIELD).dateHistogramInterval(DateHistogramInterval.hours(1))
								.subAggregation(AggregationBuilders.topHits("topHit").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
								))
				.setSize(0).execute().actionGet(); 

		Terms terms = res.getAggregations().get("story");
		log.info("Static size of stories: "+storyCount+"; Stories found: "+terms.getBuckets().size());

		for(Terms.Bucket story:terms.getBuckets()){		

			int prevReactionThankful = 0;
			int prevReactionSad = 0;
			int prevReactionAngry = 0;			
			int prevReactionWow = 0;
			int prevReactionHaha = 0;
			int prevReactionLove = 0;
			int prevLikes = 0;
			int prevComments = 0;
			int prevShares = 0;
			int prevTotalEngagemment = 0;
			int prevVideoViews = 0;
			
			/*
			 * To derive data of 0th Hour data. It would be fetched by negating
			 * 23rd hour data of last day.
			 */

			BoolQueryBuilder prevDayLastRecordQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, story.getKey()))
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
							.gte(DateUtil.getPreviousDate(date, "yyyy-MM-dd", -10))
							.lte(DateUtil.getPreviousDate(date, "yyyy-MM-dd")));

			SearchResponse prevRes = client.prepareSearch(indexName)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(prevDayLastRecordQuery).setSize(1)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute().actionGet();
			if(prevRes.getHits().getHits().length>0){
				Map<String, Object> map = prevRes.getHits().getHits()[0].getSource();
				if(map.get(Constants.SHARES)!=null){
					prevShares = (Integer)map.get(Constants.SHARES);
				}
				if(map.get(Constants.REACTION_THANKFUL)!=null){
					prevReactionThankful = (Integer)map.get(Constants.REACTION_THANKFUL);
				}
				if(map.get(Constants.REACTION_SAD)!=null){
					prevReactionSad = (Integer)map.get(Constants.REACTION_SAD);
				}
				if(map.get(Constants.REACTION_ANGRY)!=null){
					prevReactionAngry = (Integer)map.get(Constants.REACTION_ANGRY);	
				}
				if(map.get(Constants.REACTION_WOW)!=null){
					prevReactionWow = (Integer)map.get(Constants.REACTION_WOW);
				}
				if(map.get(Constants.REACTION_HAHA)!=null){
					prevReactionHaha = (Integer)map.get(Constants.REACTION_HAHA);
				}
				if(map.get(Constants.REACTION_LOVE)!=null){
					prevReactionLove = (Integer)map.get(Constants.REACTION_LOVE);
				}
				if(map.get(Constants.LIKES)!=null){
					prevLikes = (Integer)map.get(Constants.LIKES);
				}
				if(map.get(Constants.COMMENTS)!=null){
					prevComments = (Integer)map.get(Constants.COMMENTS);
				}
				if(map.get(Constants.TOTAL_ENGAGEMENT)!=null){
					prevTotalEngagemment = (Integer)map.get(Constants.TOTAL_ENGAGEMENT);
				}
				if(map.get(Constants.VIDEO_VIEWS)!=null){
					prevVideoViews = (Integer)map.get(Constants.VIDEO_VIEWS);
				}

			}

			Histogram interval = story.getAggregations().get("hour");

			for (Histogram.Bucket bucket : interval.getBuckets()) {
				TopHits topHits = bucket.getAggregations().get("topHit");
				if(topHits.getHits().getHits().length>0){
				Map<String, Object> map = topHits.getHits().getHits()[0].getSource();
				Map<String, Object> data = new HashMap<>(map);

				String rowId = (String) map.get(Constants.STORY_ID_FIELD) + "_" + map.get(Constants.CHANNEL_SLNO) + "_"
						+ bucket.getKeyAsString().split(":")[0].replaceAll("T", "_");

				data.put(Constants.ROWID, rowId);
				
				if(map.get(Constants.SHARES)!=null){
					int shares = 0;
					if(map.get(Constants.SHARES) instanceof Integer){
						shares =  (Integer)map.get(Constants.SHARES);
					} else if (map.get(Constants.SHARES) instanceof String) {
						shares = Integer.parseInt(map.get(Constants.SHARES).toString());
					}
					// Handle negative values
					if (shares - prevShares >= 0) {
						data.put(Constants.SHARES,shares-prevShares);
						prevShares = shares;
					} else {
						data.put(Constants.SHARES, 0);
						// prevShares is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_THANKFUL)!=null){
					int reaction_thankful = 0;
					if(map.get(Constants.REACTION_THANKFUL) instanceof Integer){
						reaction_thankful =  (Integer)map.get(Constants.REACTION_THANKFUL);
					} else if (map.get(Constants.REACTION_THANKFUL) instanceof String) {
						reaction_thankful = Integer.parseInt(map.get(Constants.REACTION_THANKFUL).toString());
					}

					// Handle negative values
					if (reaction_thankful - prevReactionThankful >= 0) {
						data.put(Constants.REACTION_THANKFUL,reaction_thankful-prevReactionThankful);
						prevReactionThankful = reaction_thankful;
					} else {
						data.put(Constants.REACTION_THANKFUL, 0);
						// prevReactionThankful is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_SAD)!=null){
					int reaction_sad = 0;
					if(map.get(Constants.REACTION_SAD) instanceof Integer){
						reaction_sad =  (Integer)map.get(Constants.REACTION_SAD);
					} else if (map.get(Constants.REACTION_SAD) instanceof String) {
						reaction_sad = Integer.parseInt(map.get(Constants.REACTION_SAD).toString());
					}

					// Handle negative values
					if (reaction_sad - prevReactionSad >= 0) {
						data.put(Constants.REACTION_SAD,reaction_sad-prevReactionSad);
						prevReactionSad = reaction_sad;
					} else {
						data.put(Constants.REACTION_SAD, 0);
						// prevReactionSad is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_ANGRY)!=null){
					int reaction_angry = 0;
					if(map.get(Constants.REACTION_ANGRY) instanceof Integer){
						reaction_angry =  (Integer)map.get(Constants.REACTION_ANGRY);
					} else if (map.get(Constants.REACTION_ANGRY) instanceof String) {
						reaction_angry = Integer.parseInt(map.get(Constants.REACTION_ANGRY).toString());
					}

					// Handle negative values
					if (reaction_angry - prevReactionAngry >= 0) {
						data.put(Constants.REACTION_ANGRY,reaction_angry-prevReactionAngry);
						prevReactionAngry = reaction_angry;
					} else {
						data.put(Constants.REACTION_ANGRY, 0);
						// prevReactionAngry is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_WOW)!=null){
					int reaction_wow = 0;
					if(map.get(Constants.REACTION_WOW) instanceof Integer){
						reaction_wow =  (Integer)map.get(Constants.REACTION_WOW);
					} else if (map.get(Constants.REACTION_WOW) instanceof String) {
						reaction_wow = Integer.parseInt(map.get(Constants.REACTION_WOW).toString());
					}

					// Handle negative values
					if (reaction_wow - prevReactionWow >= 0) {
						data.put(Constants.REACTION_WOW,reaction_wow-prevReactionWow);
						prevReactionWow = reaction_wow;
					} else {
						data.put(Constants.REACTION_WOW, 0);
						// prevReactionWow is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_HAHA)!=null){
					int reaction_haha = 0;
					if(map.get(Constants.REACTION_HAHA) instanceof Integer){
						reaction_haha =  (Integer)map.get(Constants.REACTION_HAHA);
					} else if (map.get(Constants.REACTION_HAHA) instanceof String) {
						reaction_haha = Integer.parseInt(map.get(Constants.REACTION_HAHA).toString());
					}

					// Handle negative values
					if (reaction_haha - prevReactionHaha >= 0) {
						data.put(Constants.REACTION_HAHA,reaction_haha-prevReactionHaha);
						prevReactionHaha = reaction_haha;
					} else {
						data.put(Constants.REACTION_HAHA, 0);
						// prevReactionHaha is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.REACTION_LOVE)!=null){
					int reaction_love = 0;
					if(map.get(Constants.REACTION_LOVE) instanceof Integer){
						reaction_love =  (Integer)map.get(Constants.REACTION_LOVE);
					} else if (map.get(Constants.REACTION_LOVE) instanceof String) {
						reaction_love = Integer.parseInt(map.get(Constants.REACTION_LOVE).toString());
					}

					// Handle negative values
					if (reaction_love - prevReactionLove >= 0) {
						data.put(Constants.REACTION_LOVE,reaction_love-prevReactionLove);
						prevReactionLove = reaction_love;
					} else {
						data.put(Constants.REACTION_LOVE, 0);
						// prevReactionLove is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.LIKES)!=null){
					int likes = 0;
					if(map.get(Constants.LIKES) instanceof Integer){
						likes =  (Integer)map.get(Constants.LIKES);
					} else if (map.get(Constants.LIKES) instanceof String) {
						likes = Integer.parseInt(map.get(Constants.LIKES).toString());
					}

					// Handle negative values
					if (likes - prevLikes >= 0) {
						data.put(Constants.LIKES,likes-prevLikes);
						prevLikes = likes;
					} else {
						data.put(Constants.LIKES, 0);
						// prevLikes is not updated bcoz data of current hour
						// was incorrect
					}
				}

				if(map.get(Constants.COMMENTS)!=null){
					int comments = 0;
					if(map.get(Constants.COMMENTS) instanceof Integer){
						comments =  (Integer)map.get(Constants.COMMENTS);
					} else if (map.get(Constants.COMMENTS) instanceof String) {
						comments = Integer.parseInt(map.get(Constants.COMMENTS).toString());
					}

					// Handle negative values
					if (comments - prevComments >= 0) {
						data.put(Constants.COMMENTS,comments-prevComments);
						prevComments = comments;
					} else {
						data.put(Constants.COMMENTS, 0);
						// prevHideClicks is not updated bcoz data of current hour
						// was incorrect
					}
				}	
				
				if(map.get(Constants.TOTAL_ENGAGEMENT)!=null){
					int total_engagement = 0;
					if(map.get(Constants.TOTAL_ENGAGEMENT) instanceof Integer){
						total_engagement =  (Integer)map.get(Constants.TOTAL_ENGAGEMENT);
					} else if (map.get(Constants.TOTAL_ENGAGEMENT) instanceof String) {
						total_engagement = Integer.parseInt(map.get(Constants.TOTAL_ENGAGEMENT).toString());
					}

					// Handle negative values
					if (total_engagement - prevComments >= 0) {
						data.put(Constants.TOTAL_ENGAGEMENT,total_engagement-prevTotalEngagemment);
						prevTotalEngagemment = total_engagement;
					} else {
						data.put(Constants.TOTAL_ENGAGEMENT, 0);
						// prevHideClicks is not updated bcoz data of current hour
						// was incorrect
					}
				}
				
				if(map.get(Constants.VIDEO_VIEWS)!=null){
					int video_views = 0;
					if(map.get(Constants.VIDEO_VIEWS) instanceof Integer){
						video_views =  (Integer)map.get(Constants.VIDEO_VIEWS);
					} else if (map.get(Constants.VIDEO_VIEWS) instanceof String) {
						video_views = Integer.parseInt(map.get(Constants.VIDEO_VIEWS).toString());
					}

					// Handle negative values
					if (video_views - prevVideoViews >= 0) {
						data.put(Constants.VIDEO_VIEWS,video_views-prevVideoViews);
						prevVideoViews = video_views;
					} else {
						data.put(Constants.VIDEO_VIEWS, 0);
						// prevHideClicks is not updated bcoz data of current hour
						// was incorrect
					}
				}
				
				fbHourlyData.add(data);
			}				
			}
			if (fbHourlyData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(Indexes.FB_COMPETITOR_HOURLY, MappingTypes.MAPPING_REALTIME,
						fbHourlyData);
				log.info("Records inserted in fb_competitor_hourly index, size: " + fbHourlyData.size());
				fbHourlyData.clear();
			}

		}
		if (fbHourlyData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.FB_COMPETITOR_HOURLY, MappingTypes.MAPPING_REALTIME,
					fbHourlyData);
			log.info("Records inserted in fb_competitor_hourly index, size: " + fbHourlyData.size());
			fbHourlyData.clear();
		}
	}
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		FbCompetitorsHourlyIndexer fb = new FbCompetitorsHourlyIndexer();


		/*List<String> dateList;
		try {
			dateList = DateUtil.getDates(args[0], args[1]);
			for(String date:dateList)
			{ 
				log.info("==================================="+date+"===========================");
				fb.insertData(date.replaceAll("_", "-")); 
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block	
			e.printStackTrace();
		} 
*/
		fb.insertData(DateUtil.getCurrentDate().replaceAll("_", "-"));
		//fb.insertData("2017-11-01");
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
