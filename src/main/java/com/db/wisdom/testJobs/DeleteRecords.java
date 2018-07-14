package com.db.wisdom.testJobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;

public class DeleteRecords {
	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	
	private static Logger log = LogManager.getLogger(ReProcess.class);

	
	public DeleteRecords() {
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public void process(String indexname,String type) {

		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;

			SearchResponse scrollResp = client.prepareSearch(indexname).setTypes(type)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					if(!hit.getId().contains(hit.getSource().get(Constants.STORY_ID_FIELD).toString())){
						DeleteResponse response = client.prepareDelete(indexname,type,  hit.getId()).get();
						System.out.println("deleting");
								}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public static void main(String[] args) {
		DeleteRecords dr = new DeleteRecords();
		dr.process(args[0],args[1]);

	}

}
