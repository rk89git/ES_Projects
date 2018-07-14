package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.Arrays;
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

public class FbInsightsHourlyIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> fbHourlyData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(FbInsightsHourlyIndexer.class);

	public void insertData(String date) {
		int storyCount=5000;

		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(date).gte(date));
		bqb.mustNot(QueryBuilders.termQuery(Constants.TOTAL_REACH, 0));
		
		String[] indexName = IndexUtils.getYearlyIndexes(Indexes.FB_INSIGHTS_HISTORY, DateUtil.getPreviousDate(date, "yyyy-MM-dd", -10),date);
	SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(bqb)
				.addAggregation(AggregationBuilders.terms("story").field(Constants.STORY_ID_FIELD).size(storyCount)
						.subAggregation(AggregationBuilders.dateHistogram("hour").field(Constants.DATE_TIME_FIELD).dateHistogramInterval(DateHistogramInterval.hours(1))
								.subAggregation(AggregationBuilders.topHits("topHit").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
								.subAggregation(AggregationBuilders.terms("popular").field(Constants.IS_POPULAR).size(5))))
				.setSize(0).execute().actionGet(); 

		Terms terms = res.getAggregations().get("story");
		log.info("Static size of stories: "+storyCount+"; Stories found: "+terms.getBuckets().size());
        for(Terms.Bucket story:terms.getBuckets()){		

			int prevUniqueReach = 0;
			int prevShares = 0;
			int prevTotalReach = 0;
			int prevLinkClicks = 0;
			int prevReactionThankful = 0;
			int prevReactionSad = 0;
			int prevReactionAngry = 0;			
			int prevReactionWow = 0;
			int prevReactionHaha = 0;
			int prevReactionLove = 0;
			int prevHideClicks = 0;
			int prevHideAllClicks = 0;
			int prevReportSpamClicks = 0;
			int prevLikes = 0;
			int prevComments = 0;
			int prevIaClicks = 0;
			int prevVideoViews = 0;
			int prevUniqueVideoViews = 0;
			int prevIaTimespent = 0;
			int prevPostImpressionsUnique = 0;
			int prevPostImpressionsPaid = 0;

			/*
			 * To derive data of 0th Hour data. It would be fetched by negating
			 * 23rd hour data of last day.
			 */

			BoolQueryBuilder prevDayLastRecordQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, story.getKey()))
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
							.gte(DateUtil.getPreviousDate(date, "yyyy-MM-dd", -10))
							.lte(DateUtil.getPreviousDate(date, "yyyy-MM-dd")))
					.mustNot(QueryBuilders.termQuery(Constants.TOTAL_REACH, 0));

			SearchResponse prevRes = client.prepareSearch(indexName)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(prevDayLastRecordQuery).setSize(1)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute().actionGet();
			if(prevRes.getHits().getHits().length>0){
				Map<String, Object> map = prevRes.getHits().getHits()[0].getSource();
				if(map.get(Constants.UNIQUE_REACH)!=null){
					prevUniqueReach = (Integer)map.get(Constants.UNIQUE_REACH);
				}
				if(map.get(Constants.SHARES)!=null){
					prevShares = (Integer)map.get(Constants.SHARES);
				}
				if(map.get(Constants.TOTAL_REACH)!=null){
					prevTotalReach = (Integer)map.get(Constants.TOTAL_REACH);
				}
				if(map.get(Constants.LINK_CLICKS)!=null){
					prevLinkClicks = (Integer)map.get(Constants.LINK_CLICKS);
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
				if(map.get(Constants.HIDE_CLICKS)!=null){
					prevHideClicks = (Integer)map.get(Constants.HIDE_CLICKS);
				}
				if(map.get(Constants.HIDE_ALL_CLICKS)!=null){
					prevHideAllClicks = (Integer)map.get(Constants.HIDE_ALL_CLICKS);
				}
				if(map.get(Constants.REPORT_SPAM_CLICKS)!=null){
					prevReportSpamClicks = (Integer)map.get(Constants.REPORT_SPAM_CLICKS);
				}
				if(map.get(Constants.LIKES)!=null){
					prevLikes = (Integer)map.get(Constants.LIKES);
				}
				if(map.get(Constants.COMMENTS)!=null){
					prevComments = (Integer)map.get(Constants.COMMENTS);
				}
				if(map.get(Constants.IA_CLICKS)!=null){
					if(map.get(Constants.IA_CLICKS) instanceof Integer){
						prevIaClicks =  (Integer)map.get(Constants.IA_CLICKS);
					} else if (map.get(Constants.IA_CLICKS) instanceof String) {
						prevIaClicks = Integer.parseInt(map.get(Constants.IA_CLICKS).toString());
					}
				}
				if(map.get(Constants.VIDEO_VIEWS)!=null){
					if(map.get(Constants.VIDEO_VIEWS) instanceof Integer){
						prevVideoViews =  (Integer)map.get(Constants.VIDEO_VIEWS);
					} else if (map.get(Constants.VIDEO_VIEWS) instanceof String) {
						prevVideoViews = Integer.parseInt(map.get(Constants.VIDEO_VIEWS).toString());
					}
				}
				if(map.get(Constants.UNIQUE_VIDEO_VIEWS)!=null){
					if(map.get(Constants.UNIQUE_VIDEO_VIEWS) instanceof Integer){
						prevUniqueVideoViews =  (Integer)map.get(Constants.UNIQUE_VIDEO_VIEWS);
					} else if (map.get(Constants.UNIQUE_VIDEO_VIEWS) instanceof String) {
						prevUniqueVideoViews = Integer.parseInt(map.get(Constants.UNIQUE_VIDEO_VIEWS).toString());
					}
				}
				if(map.get(Constants.IA_TIMESPENT)!=null){
					if(map.get(Constants.IA_TIMESPENT) instanceof Integer){
						prevIaTimespent =  (Integer)map.get(Constants.IA_TIMESPENT);
					} else if (map.get(Constants.IA_TIMESPENT) instanceof String) {
						prevIaTimespent = Integer.parseInt(map.get(Constants.IA_TIMESPENT).toString());
					}
				}
				if(map.get(Constants.POST_IMPRESSIONS_UNIQUE)!=null){
					if(map.get(Constants.POST_IMPRESSIONS_UNIQUE) instanceof Integer){
						prevPostImpressionsUnique =  (Integer)map.get(Constants.POST_IMPRESSIONS_UNIQUE);
					} else if (map.get(Constants.POST_IMPRESSIONS_UNIQUE) instanceof String) {
						prevPostImpressionsUnique = Integer.parseInt(map.get(Constants.POST_IMPRESSIONS_UNIQUE).toString());
					}
				}
				if(map.get(Constants.POST_IMPRESSIONS_PAID)!=null){
					if(map.get(Constants.POST_IMPRESSIONS_PAID) instanceof Integer){
						prevPostImpressionsPaid =  (Integer)map.get(Constants.POST_IMPRESSIONS_PAID);
					} else if (map.get(Constants.POST_IMPRESSIONS_PAID) instanceof String) {
						prevPostImpressionsPaid = Integer.parseInt(map.get(Constants.POST_IMPRESSIONS_PAID).toString());
					}
				}

			}

			Histogram interval = story.getAggregations().get("hour");

			for (Histogram.Bucket bucket : interval.getBuckets()) {
				TopHits topHits = bucket.getAggregations().get("topHit");
				if(topHits.getHits().getHits().length>0){
					Map<String, Object> map = topHits.getHits().getHits()[0].getSource();
					Map<String, Object> data = new HashMap<>(map);

					//set ispopular true or false on the basis of number of documents having ispopular true or false.
					Terms isPopularTerms = bucket.getAggregations().get("popular");
					long popular = 0;
					long notPopular = 0;
					//double popularPercentage = 0.0;
					for(Terms.Bucket isPopular:isPopularTerms.getBuckets()){	
						if(isPopular.getKey().equals("true")){
							popular = isPopular.getDocCount();
						}
						else if(isPopular.getKey().equals("false")){
							notPopular = isPopular.getDocCount();
						}
					}

					/*popularPercentage =(((Long)popular).doubleValue()/(popular+notPopular))*100;
				if(popularPercentage>=50){
					data.put(Constants.IS_POPULAR, true);
				}
				else {
					data.put(Constants.IS_POPULAR, false);
				}

				data.put(Constants.POPULAR_PERCENTAGE, popularPercentage);*/

					data.put(Constants.POPULAR_COUNT, popular);
					data.put(Constants.NOT_POPULAR_COUNT, notPopular);
					data.put(Constants.POPULARITY_SCORE, popular-notPopular);


					String rowId = (String) map.get(Constants.STORY_ID_FIELD) + "_" + map.get(Constants.CHANNEL_SLNO) + "_"
							+ bucket.getKeyAsString().split(":")[0].replaceAll("T", "_");

					data.put(Constants.ROWID, rowId);
					if(map.get(Constants.UNIQUE_REACH)!=null){
						int unique_reach = 0;
						if(map.get(Constants.UNIQUE_REACH) instanceof Integer){
							unique_reach =  (Integer)map.get(Constants.UNIQUE_REACH);
						} else if (map.get(Constants.UNIQUE_REACH) instanceof String) {
							unique_reach = Integer.parseInt(map.get(Constants.UNIQUE_REACH).toString());
						}
						// Handle negative values
						if (unique_reach - prevUniqueReach >= 0) {
							data.put(Constants.UNIQUE_REACH,unique_reach-prevUniqueReach);
							prevUniqueReach = unique_reach;				
						} else {
							data.put(Constants.UNIQUE_REACH, 0);
							// prevUniqueReach is not updated bcoz data of current
							// hour was incorrect
						}

					}

					if(map.get(Constants.TOTAL_REACH)!=null){
						int total_reach = 0;
						if(map.get(Constants.TOTAL_REACH) instanceof Integer){
							total_reach =  (Integer)map.get(Constants.TOTAL_REACH);
						} else if (map.get(Constants.TOTAL_REACH) instanceof String) {
							total_reach = Integer.parseInt(map.get(Constants.TOTAL_REACH).toString());
						}

						// Handle negative values
						if (total_reach - prevTotalReach >= 0) {
							data.put(Constants.TOTAL_REACH,total_reach-prevTotalReach);
							prevTotalReach = total_reach;
						} else {
							data.put(Constants.TOTAL_REACH, 0);
							// prevTotalReach is not updated bcoz data of current
							// hour was incorrect
						}
					}

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

					if(map.get(Constants.LINK_CLICKS)!=null){
						int link_clicks = 0;
						if(map.get(Constants.LINK_CLICKS) instanceof Integer){
							link_clicks =  (Integer)map.get(Constants.LINK_CLICKS);
						} else if (map.get(Constants.LINK_CLICKS) instanceof String) {
							link_clicks = Integer.parseInt(map.get(Constants.LINK_CLICKS).toString());
						}

						// Handle negative values
						if (link_clicks - prevLinkClicks >= 0) {
							data.put(Constants.LINK_CLICKS,link_clicks-prevLinkClicks);
							prevLinkClicks = link_clicks;
						} else {
							data.put(Constants.LINK_CLICKS, 0);
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

					if(map.get(Constants.HIDE_CLICKS)!=null){
						int hide_clicks = 0;
						if(map.get(Constants.HIDE_CLICKS) instanceof Integer){
							hide_clicks =  (Integer)map.get(Constants.HIDE_CLICKS);
						} else if (map.get(Constants.HIDE_CLICKS) instanceof String) {
							hide_clicks = Integer.parseInt(map.get(Constants.HIDE_CLICKS).toString());
						}

						// Handle negative values
						if (hide_clicks - prevHideClicks >= 0) {
							data.put(Constants.HIDE_CLICKS,hide_clicks-prevHideClicks);
							prevHideClicks = hide_clicks;
						} else {
							data.put(Constants.HIDE_CLICKS, 0);
							// prevHideClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}

					if(map.get(Constants.HIDE_ALL_CLICKS)!=null){
						int hide_all_clicks = 0;
						if(map.get(Constants.HIDE_ALL_CLICKS) instanceof Integer){
							hide_all_clicks =  (Integer)map.get(Constants.HIDE_ALL_CLICKS);
						} else if (map.get(Constants.HIDE_ALL_CLICKS) instanceof String) {
							hide_all_clicks = Integer.parseInt(map.get(Constants.HIDE_ALL_CLICKS).toString());
						}

						// Handle negative values
						if (hide_all_clicks - prevHideAllClicks >= 0) {
							data.put(Constants.HIDE_ALL_CLICKS,hide_all_clicks-prevHideAllClicks);
							prevHideAllClicks = hide_all_clicks;
						} else {
							data.put(Constants.HIDE_ALL_CLICKS, 0);
							// prevHideAllClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}

					if(map.get(Constants.REPORT_SPAM_CLICKS)!=null){
						int report_spam_clicks = 0;
						if(map.get(Constants.REPORT_SPAM_CLICKS) instanceof Integer){
							report_spam_clicks =  (Integer)map.get(Constants.REPORT_SPAM_CLICKS);
						} else if (map.get(Constants.REPORT_SPAM_CLICKS) instanceof String) {
							report_spam_clicks = Integer.parseInt(map.get(Constants.REPORT_SPAM_CLICKS).toString());
						}

						// Handle negative values
						if (report_spam_clicks - prevReportSpamClicks >= 0) {
							data.put(Constants.REPORT_SPAM_CLICKS,report_spam_clicks-prevReportSpamClicks);
							prevReportSpamClicks = report_spam_clicks;
						} else {
							data.put(Constants.REPORT_SPAM_CLICKS, 0);
							// prevReportSpamClicks is not updated bcoz data of current hour
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

					if(map.get(Constants.IA_CLICKS)!=null){
						int ia_clicks = 0;
						if(map.get(Constants.IA_CLICKS) instanceof Integer){
							ia_clicks =  (Integer)map.get(Constants.IA_CLICKS);
						} else if (map.get(Constants.IA_CLICKS) instanceof String) {
							ia_clicks = Integer.parseInt(map.get(Constants.IA_CLICKS).toString());
						}

						// Handle negative values
						if (ia_clicks - prevIaClicks >= 0) {
							data.put(Constants.IA_CLICKS,ia_clicks-prevIaClicks);
							prevIaClicks = ia_clicks;
						} else {
							data.put(Constants.IA_CLICKS, 0);
							// prevIaClicks is not updated bcoz data of current hour
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
							// prevIaClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}

					if(map.get(Constants.UNIQUE_VIDEO_VIEWS)!=null){
						int unique_video_views = 0;
						if(map.get(Constants.UNIQUE_VIDEO_VIEWS) instanceof Integer){
							unique_video_views =  (Integer)map.get(Constants.UNIQUE_VIDEO_VIEWS);
						} else if (map.get(Constants.UNIQUE_VIDEO_VIEWS) instanceof String) {
							unique_video_views = Integer.parseInt(map.get(Constants.UNIQUE_VIDEO_VIEWS).toString());
						}

						// Handle negative values
						if (unique_video_views - prevUniqueVideoViews >= 0) {
							data.put(Constants.UNIQUE_VIDEO_VIEWS,unique_video_views-prevUniqueVideoViews);
							prevUniqueVideoViews = unique_video_views;
						} else {
							data.put(Constants.UNIQUE_VIDEO_VIEWS, 0);
							// prevIaClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}
					
					if(map.get(Constants.IA_TIMESPENT)!=null){
						int ia_timespent = 0;
						if(map.get(Constants.IA_TIMESPENT) instanceof Integer){
							ia_timespent =  (Integer)map.get(Constants.IA_TIMESPENT);
						} else if (map.get(Constants.IA_TIMESPENT) instanceof String) {
							ia_timespent = Integer.parseInt(map.get(Constants.IA_TIMESPENT).toString());
						}

						// Handle negative values
						if (ia_timespent - prevIaTimespent >= 0) {
							data.put(Constants.IA_TIMESPENT,ia_timespent-prevIaTimespent);
							prevIaTimespent = ia_timespent;
						} else {
							data.put(Constants.IA_TIMESPENT, 0);
							// prevIaClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}
					
					if(map.get(Constants.POST_IMPRESSIONS_PAID)!=null){
						int post_impressions_paid = 0;
						if(map.get(Constants.POST_IMPRESSIONS_PAID) instanceof Integer){
							post_impressions_paid =  (Integer)map.get(Constants.POST_IMPRESSIONS_PAID);
						} else if (map.get(Constants.POST_IMPRESSIONS_PAID) instanceof String) {
							post_impressions_paid = Integer.parseInt(map.get(Constants.POST_IMPRESSIONS_PAID).toString());
						}

						// Handle negative values
						if (post_impressions_paid - prevPostImpressionsPaid >= 0) {
							data.put(Constants.POST_IMPRESSIONS_PAID,post_impressions_paid-prevPostImpressionsPaid);
							prevPostImpressionsPaid = post_impressions_paid;
						} else {
							data.put(Constants.POST_IMPRESSIONS_PAID, 0);
							// prevIaClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}
					if(map.get(Constants.POST_IMPRESSIONS_UNIQUE)!=null){
						int post_impressions_unique = 0;
						if(map.get(Constants.POST_IMPRESSIONS_UNIQUE) instanceof Integer){
							post_impressions_unique =  (Integer)map.get(Constants.POST_IMPRESSIONS_UNIQUE);
						} else if (map.get(Constants.POST_IMPRESSIONS_UNIQUE) instanceof String) {
							post_impressions_unique = Integer.parseInt(map.get(Constants.POST_IMPRESSIONS_UNIQUE).toString());
						}

						// Handle negative values
						if (post_impressions_unique - prevPostImpressionsUnique >= 0) {
							data.put(Constants.POST_IMPRESSIONS_UNIQUE,post_impressions_unique-prevPostImpressionsUnique);
							prevPostImpressionsUnique = post_impressions_unique;
						} else {
							data.put(Constants.POST_IMPRESSIONS_UNIQUE, 0);
							// prevIaClicks is not updated bcoz data of current hour
							// was incorrect
						}
					}

					fbHourlyData.add(data);
				}
			}

			if (fbHourlyData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(Indexes.FB_INSIGHTS_HOURLY, MappingTypes.MAPPING_REALTIME,
						fbHourlyData);
				log.info("Records inserted in fb_insights_hourly index, size: " + fbHourlyData.size());
				fbHourlyData.clear();
			}

		}

		if (fbHourlyData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(Indexes.FB_INSIGHTS_HOURLY, MappingTypes.MAPPING_REALTIME,
					fbHourlyData);
			log.info("Records inserted in fb_insights_hourly index, size: " + fbHourlyData.size());
			fbHourlyData.clear();
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		FbInsightsHourlyIndexer fb = new FbInsightsHourlyIndexer();


		//		List<String> dateList;
		//		try {
		//			dateList = DateUtil.getDates(args[0], args[1]);
		//			for(String date:dateList)
		//			{ 
		//				log.info("==================================="+date+"===========================");
		//				fb.insertData(date.replaceAll("_", "-")); 
		//			}
		//		} catch (Exception e) {
		//			// TODO Auto-generated catch block	
		//			e.printStackTrace();
		//		} 

		fb.insertData(DateUtil.getCurrentDate().replaceAll("_", "-"));
		//fb.insertData("2017-09-02");
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
