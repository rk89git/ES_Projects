package com.db.wisdom.testJobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class PopularityIndexer2 {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	public void insertData(String date) {
		int storyCount=5000;
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		Map<String, BoolQueryBuilder> queryMap = new HashMap<>();

		BoolQueryBuilder hourQuery = QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
						/*.gte("2017-07-14")
						.lte("2017-07-21"));*/
						.gte(DateUtil.addHoursToCurrentTime(-1).replaceAll(":.{2}", ":00"))
						.lte(DateUtil.getCurrentDateTime()));
		BoolQueryBuilder dayQuery = QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
						.gte(date)
						.lte(date));	
		BoolQueryBuilder monthQuery = QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
						.gte(date.replaceAll(".{2}$", "01"))
						.lte(date));

		//queryMap.put(Constants.HOUR, hourQuery);
		queryMap.put(Constants.DAY, dayQuery);
		//queryMap.put(Constants.MONTH, monthQuery);
		AbstractAggregationBuilder agg = AggregationBuilders.terms("story").field(Constants.STORY_ID_FIELD).size(storyCount)
				.subAggregation(AggregationBuilders.sum(Constants.POPULAR_COUNT).field(Constants.POPULAR_COUNT))
				.subAggregation(AggregationBuilders.sum(Constants.NOT_POPULAR_COUNT).field(Constants.NOT_POPULAR_COUNT))
				.subAggregation(AggregationBuilders.sum(Constants.POPULARITY_SCORE).field(Constants.POPULARITY_SCORE))
				.subAggregation(AggregationBuilders.topHits("topHit").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC));

		for(String interval:queryMap.keySet()){
			System.out.println("indexing data for "+interval+" for query "+queryMap.get(interval));
			
			BoolQueryBuilder boolQueryBuilder=queryMap.get(interval);
			boolQueryBuilder.filter(QueryBuilders.existsQuery(Constants.POPULARITY_SCORE));
			
			
			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQueryBuilder)
					.addAggregation(agg)
					.setSize(0).execute().actionGet();
			Terms storyTerms = res.getAggregations().get("story");

			for(Terms.Bucket storyBucket:storyTerms.getBuckets()){
				TopHits topHits = storyBucket.getAggregations().get("topHit");					
				Map<String, Object> map = topHits.getHits().getHits()[0].getSource();
				Map<String, Object> data = new HashMap<>(map);

				String rowId = "";
				if(interval.equals(Constants.HOUR)){
					rowId = (String) map.get(Constants.STORY_ID_FIELD) + "_" + map.get(Constants.CHANNEL_SLNO) + "_"
							+ map.get(Constants.DATE_TIME_FIELD).toString().split(":")[0].replaceAll("T", "_").replaceAll("-", "_");
				}
				else if(interval.equals(Constants.DAY)){
					rowId = (String) map.get(Constants.STORY_ID_FIELD) + "_" + map.get(Constants.CHANNEL_SLNO) + "_"
							+ map.get(Constants.DATE_TIME_FIELD).toString().split("T")[0].replaceAll("-", "_");
				}
				else if(interval.equals(Constants.MONTH)){
					rowId = (String) map.get(Constants.STORY_ID_FIELD) + "_" + map.get(Constants.CHANNEL_SLNO) + "_"
							+ map.get(Constants.DATE_TIME_FIELD).toString().substring(0, 7).replaceAll("-", "_");
				}

				Sum poupular_agg = storyBucket.getAggregations().get(Constants.POPULAR_COUNT);
				Sum not_poupular_agg = storyBucket.getAggregations().get(Constants.NOT_POPULAR_COUNT);
				Sum poupularity_score_agg = storyBucket.getAggregations().get(Constants.POPULARITY_SCORE);

				data.put(Constants.ROWID, rowId);
				data.put(Constants.POPULAR_COUNT, poupular_agg.getValue());
				data.put(Constants.NOT_POPULAR_COUNT, not_poupular_agg.getValue());
				data.put(Constants.POPULARITY_SCORE, poupularity_score_agg.getValue());
				data.put(Constants.INTERVAL, interval);				

				dataList.add(data);


			}
			if (dataList.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(Indexes.FB_INSIGHTS_POPULAR, MappingTypes.MAPPING_REALTIME,
						dataList);
				System.out.println("Records inserted in fb_insights_popular index, size: " + dataList.size());
				dataList.clear();
			}
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		PopularityIndexer2 fb = new PopularityIndexer2();		
		fb.insertData(args[0]);
		System.out.println("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
