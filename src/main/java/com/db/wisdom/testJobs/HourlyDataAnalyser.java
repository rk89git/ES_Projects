package com.db.wisdom.testJobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
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

public class HourlyDataAnalyser {

	private static Logger log = LogManager.getLogger(RealTimeConsumerRunnableTask.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> storyDetailsData = new ArrayList<Map<String, Object>>();

	private List<String> storyIdsforCounterUpdate = new ArrayList<String>();

	public void insertData() {
		long start = System.currentTimeMillis();
		String indexName = "realtime_2017_04_24";
		//+DateUtil.getPreviousDate();
		//	elasticSearchIndexService.deleteIndex(Indexes.STORY_DETAIL_HOURLY, MappingTypes.MAPPING_REALTIME);		
		SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				//.setSearchType(SearchType.SCAN)
				.addSort("_doc",SortOrder.ASC).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte("2017-04-24T18:00:00Z"))
				.setSize(1000).execute().actionGet(); 
		System.out.println("totalhits: "+scrollResp.getHits().getTotalHits());		
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> map = hit.getSource();
				String storyId = (String) map.get(Constants.STORY_ID_FIELD);
				String dateWithHour = ((String) map.get(Constants.DATE_TIME_FIELD)).split(":")[0].replaceAll("T", "-");
				String storyRowId = storyId + "_" + map.get(Constants.TRACKER) + "_" + map.get(Constants.REF_PLATFORM)
				+ "_" + map.get(Constants.VERSION) + "_" + map.get(Constants.HOST) + "_" + dateWithHour;
				Map<String, Object> storyDetail = new HashMap<String, Object>();
				storyDetail.put(Constants.STORY_ID_FIELD, map.get(Constants.STORY_ID_FIELD));
				storyDetail.put(Constants.TITLE, map.get(Constants.TITLE));
				storyDetail.put(Constants.CAT_ID_FIELD, map.get(Constants.CAT_ID_FIELD));
				storyDetail.put(Constants.PARENT_CAT_ID_FIELD, map.get(Constants.PARENT_CAT_ID_FIELD));
				storyDetail.put(Constants.STORY_ATTRIBUTE, map.get(Constants.STORY_ATTRIBUTE));
				storyDetail.put(Constants.PGTOTAL, map.get(Constants.PGTOTAL));
				storyDetail.put(Constants.TRACKER, map.get(Constants.TRACKER));
				storyDetail.put(Constants.REF_PLATFORM, map.get(Constants.REF_PLATFORM));
				storyDetail.put(Constants.VERSION, map.get(Constants.VERSION));
				storyDetail.put(Constants.HOST, map.get(Constants.HOST));
				storyDetail.put(Constants.CLASSFICATION_ID_FIELD, map.get(Constants.CLASSFICATION_ID_FIELD));
				storyDetail.put(Constants.SECTION, map.get(Constants.SECTION));
				storyDetail.put(Constants.DATE_TIME_FIELD, map.get(Constants.DATE_TIME_FIELD));
				storyDetail.put(Constants.STORY_PUBLISH_TIME, map.get(Constants.STORY_PUBLISH_TIME));
				storyDetail.put(Constants.STORY_MODIFIED_TIME, map.get(Constants.STORY_MODIFIED_TIME));
				storyDetail.put(Constants.AUTHOR_NAME, map.get(Constants.AUTHOR_NAME));
				storyDetail.put(Constants.UID, map.get(Constants.UID));
				storyDetail.put(Constants.HOST_TYPE, map.get(Constants.HOST_TYPE));
				storyDetail.put(Constants.REF_HOST, map.get(Constants.REF_HOST));
				storyDetail.put(Constants.CHANNEL_SLNO, map.get(Constants.CHANNEL_SLNO));
				storyDetail.put(Constants.ROWID, storyRowId);
				storyDetail.put(Constants.LOCATION, map.get(Constants.LOCATION));
				storyDetail.put(Constants.EVENT, map.get(Constants.EVENT));
				storyDetail.put(Constants.ORGANIZATION, map.get(Constants.ORGANIZATION));
				storyDetail.put(Constants.PEOPLE, map.get(Constants.PEOPLE));
				storyDetail.put(Constants.OTHER, map.get(Constants.OTHER));
				storyDetail.put(Constants.STORY_TYPE, map.get(Constants.STORY_TYPE));
				storyIdsforCounterUpdate.add(storyRowId);
				storyDetailsData.add(storyDetail);
			}

			// Index the story details in story_detail index
			if (storyDetailsData.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(Indexes.STORY_DETAIL_HOURLY, MappingTypes.MAPPING_REALTIME,
						storyDetailsData);
				log.info("Records inserted in story_detail index, size: " + storyDetailsData.size());
				System.out.println("Records inserted in story_detail index, size: " + storyDetailsData.size());
				storyDetailsData.clear();
			}

			// Update PV in story_detail index
			if (storyIdsforCounterUpdate.size() > 0) {
				elasticSearchIndexService.incrementCounter(Indexes.STORY_DETAIL_HOURLY, MappingTypes.MAPPING_REALTIME,
						storyIdsforCounterUpdate, Constants.PVS, 1);
				System.out.println("PVs updated in story_detail index, size: " + storyIdsforCounterUpdate.size());
				storyIdsforCounterUpdate.clear();
			}
			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute()
					.actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
		System.out.println("Total time taken: "+((System.currentTimeMillis()-start)/1000));
	}

	public static void main(String[] args) {
		HourlyDataAnalyser hda = new HourlyDataAnalyser();
		hda.insertData();
	}
}
