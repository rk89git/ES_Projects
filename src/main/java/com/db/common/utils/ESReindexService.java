package com.db.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;

public class ESReindexService {

	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ESReindexService.class);

	int batchSize = 5000;

	public ESReindexService() {
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

	public void reindex(String sourceIndex, String targetIndex) {
		long startTime = System.currentTimeMillis();
		try {
			List<String> docIdToDelete = new ArrayList<String>();
			int numRec = 1000;

			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc", SortOrder.ASC)
					.setScroll(new TimeValue(60000))
					.setQuery(QueryBuilders.matchAllQuery())
					.setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
			long dayWiseThreadRecordCount = 1;
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						dayWiseThreadRecordCount = dayWiseThreadAtomicInt.getAndIncrement();
						Map<String, Object> record = hit.getSource();
						record.put(Constants.ROWID, hit.getId());
						listWeightage.add(record);
						docIdToDelete.add(hit.getId());
						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
						//	elasticSearchIndexService.delete(sourceIndex, MappingTypes.MAPPING_REALTIME, docIdToDelete);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().getTotalHits() + "; Records Processed: "
									+ dayWiseThreadRecordCount);
							listWeightage.clear();
							docIdToDelete.clear();
						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			// Index remaining data if there after completing loop
			if (listWeightage.size() > 0) {
				elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
			//	elasticSearchIndexService.delete(sourceIndex, MappingTypes.MAPPING_REALTIME, docIdToDelete);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: " + dayWiseThreadRecordCount);
				listWeightage.clear();
				docIdToDelete.clear();
			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public static void main(String[] args) {
		ESReindexService duvi = new ESReindexService();
		//duvi.reindex(args[0], args[1]);
		
		duvi.reindex("rec_story_detail", "recommendation_story_detail");
	}

}