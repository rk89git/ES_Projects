package com.db.wisdom.testJobs;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.kafka.consumer.RealTimeConsumerRunnableTask;

import scala.util.control.Exception.Catch;

public class StoryCountIndexer {

	private static Logger log = LogManager.getLogger(RealTimeConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();	
	public void insertData(String startDate, String endDate) {
		long start = System.currentTimeMillis();
		SearchResponse scrollResp = client.prepareSearch(Indexes.USER_PROFILE_DAYWISE).setTypes(MappingTypes.MAPPING_REALTIME)
				//.setSearchType(SearchType.SCAN)
				.addSort("_doc",SortOrder.ASC).setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(startDate).lte(endDate))
						.must(QueryBuilders.termQuery(Constants.HOST, "15"))
						.mustNot(QueryBuilders.termQuery(Constants.TRACKER, "news-ucb")))
				.setSize(1000).execute().actionGet(); 
		long totalHits=scrollResp.getHits().getTotalHits();
		System.out.println("Totalhits: "+totalHits);	
		long processed=0;
		while (true) {
			try{
			
			Map<String, Integer> userMap = new HashMap<>();			
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> map = hit.getSource();
				String session_id = (String)map.get(Constants.SESSION_ID_FIELD);
				
				if (userMap.containsKey(session_id)) {
					userMap.put(session_id, userMap.get(session_id)  + (Integer)map.get(Constants.STORY_COUNT));	
				}else {
					userMap.put(session_id, (Integer)map.get(Constants.STORY_COUNT));
				}
				
			}
			processed=processed+scrollResp.getHits().getHits().length;
			System.out.println("Total hits: "+totalHits + "; Processed: "+processed);
			elasticSearchIndexService.incrementCounter("unique_user_story_count", MappingTypes.MAPPING_REALTIME, userMap, Constants.STORY_COUNT);
			
			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute()
					.actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Total time taken: "+((System.currentTimeMillis()-start)/1000));
	}

	public static void main(String[] args) {
		StoryCountIndexer indexer = new StoryCountIndexer();
		indexer.insertData(args[0], args[1]);
	}
}
