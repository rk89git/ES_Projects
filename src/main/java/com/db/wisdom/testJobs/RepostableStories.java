package com.db.wisdom.testJobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class RepostableStories {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();
	
	int batchSize = 5000;
	
	public void insertData() {
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();

		BoolQueryBuilder qb = QueryBuilders.boolQuery()
				/*.must(QueryBuilders.rangeQuery(Constants.CREATED_DATETIME)
						.gte(DateUtil.getPreviousDate(DateUtil.getCurrentDate(), -100))
						.lte(DateUtil.getPreviousDate(DateUtil.getCurrentDate(), -21)))*/
				.must(QueryBuilders.termQuery(Constants.IS_REPOSTABLE, 2));

		SearchResponse res = client.prepareSearch(Indexes.FB_DASHBOARD).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(qb)
				.addAggregation(AggregationBuilders.terms("category").field(Constants.CAT_NAME).size(10000)
						.subAggregation(AggregationBuilders.percentiles(Constants.FIELD1).field(Constants.FIELD1).percentiles(50))
						.subAggregation(AggregationBuilders.percentiles("slot1").field(Constants.FIELD1).percentiles(33))
						.subAggregation(AggregationBuilders.percentiles("slot2").field(Constants.FIELD1).percentiles(66))
						.subAggregation(AggregationBuilders.percentiles("slot3").field(Constants.FIELD1).percentiles(100))
						.subAggregation(AggregationBuilders.percentiles(Constants.FIELD2).field(Constants.FIELD2).percentiles(50))
						.subAggregation(AggregationBuilders.percentiles(Constants.FIELD3).field(Constants.FIELD3).percentiles(50))
						.subAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(10000)
								.subAggregation(AggregationBuilders.topHits("top").size(1))))
				.setSize(0).execute().actionGet();
		
		Terms catAgg = res.getAggregations().get("category");
		for(Terms.Bucket category:catAgg.getBuckets()){
			double field1M = 0.0;
			double field2M = 0.0;
			double field3M = 0.0;
			double slot1 = 0.0;
			double slot2 = 0.0;
			double slot3 = 0.0;
			Percentiles f1_agg = category.getAggregations().get(Constants.FIELD1);
			for (Percentile entry : f1_agg) {
				field1M = entry.getValue();
			}
			Percentiles f2_agg = category.getAggregations().get(Constants.FIELD2);
			for (Percentile entry : f2_agg) {
				field2M = entry.getValue();
			}
			Percentiles f3_agg = category.getAggregations().get(Constants.FIELD3);
			for (Percentile entry : f3_agg) {
				field3M = entry.getValue();
			}
			Percentiles s1_agg = category.getAggregations().get("slot1");
			for (Percentile entry : s1_agg) {
				slot1 = entry.getValue();
			}
			Percentiles s2_agg = category.getAggregations().get("slot2");
			for (Percentile entry : s2_agg) {
				slot2 = entry.getValue();
			}
			Percentiles s3_agg = category.getAggregations().get("slot3");
			for (Percentile entry : s3_agg) {
				slot3 = entry.getValue();
			}
			Terms storyAgg = category.getAggregations().get("stories");
			for(Terms.Bucket story:storyAgg.getBuckets()){
				TopHits topHitAgg = story.getAggregations().get("top");
				Map<String , Object> source = new HashMap<>(topHitAgg.getHits().getHits()[0].getSource());
				Double score = 0.0;
				double f1 = 0.0;
				double f2 = 0.0;
				double f3 = 0.0;
				double normf1 = 0.0;
				double normf2 = 0.0;
				double normf3 = 0.0;
				if(source.get(Constants.FIELD1)!=null)
				{
					f1 = (Double)source.get(Constants.FIELD1);
				}
				
				if(source.get(Constants.FIELD2)!=null)
				{
					f2 = (Double)source.get(Constants.FIELD2);
				}
				
				if(source.get(Constants.FIELD3)!=null)
				{
					f3 = (Double)source.get(Constants.FIELD3);
				}
				
				if(f1!=0&&field1M!=0){
					normf1 = f1/field1M;
				}
				
				if(f2!=0&&field2M!=0){
					normf2 = f2/field2M;
				}
				
				if(f3!=0&&field3M!=0){
					normf3 = f3/field3M;
				}
				
				if(normf1>0.0&&normf2>0.0&&normf3>0.0){
					score = normf1*normf2*normf3;
				}
				
				source.put(Constants.SCORE, score);
				if(f1>=0&&f1<=slot1){
					source.put(Constants.SLOT, "1");
				}                               
				else if(f1>slot1&&f1<=slot2){
					source.put(Constants.SLOT, "2");
				}
				else if(f1>slot2&&f1<=slot3){
					source.put(Constants.SLOT, "3");
				}
				
				source.put(Constants.ROWID, source.get(Constants.STORY_ID_FIELD));
				dataList.add(source);
				if (dataList.size() == batchSize) {
					elasticSearchIndexService.index(Indexes.FB_DASHBOARD, MappingTypes.MAPPING_REALTIME, dataList);
					System.out.println(dataList.size()+ "........Records Processed: ");
					dataList.clear();
				}
			}
		}
		
		if (dataList.size() > 0) {
			elasticSearchIndexService.index(Indexes.FB_DASHBOARD, MappingTypes.MAPPING_REALTIME, dataList);
			System.out.println(dataList.size()+ "........Records Processed: ");
			dataList.clear();
		}
	}
	public static void main(String[] args) {
		RepostableStories obj = new RepostableStories();
		obj.insertData();
	}
}
