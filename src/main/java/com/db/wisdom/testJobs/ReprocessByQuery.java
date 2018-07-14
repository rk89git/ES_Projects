package com.db.wisdom.testJobs;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class ReprocessByQuery {

	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ReProcess.class);

	int batchSize = 5000;
	//static String ip = "104.199.156.183";   //global
	public ReprocessByQuery() {
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

	public void reindex(String sourceIndex) {
		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;

			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setQuery(
							/*QueryBuilders.boolQuery().must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO,"960"))
							.must(QueryBuilders.termQuery(Constants.CAT_ID_FIELD, "5033"))*/
							QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from("2017-06-28").to("2017-06-29T12:30:00Z")
							)					
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						Map<String, Object> record = hit.getSource();
						record.put(Constants.ROWID, hit.getId());

						//record.put("ga_cat_name", "Health");
						/*if(record.get(Constants.STORY_ID_FIELD)!=null){
							record.put(Constants.PROFILE_ID, record.get(Constants.STORY_ID_FIELD).toString().split("_")[0]);
						}*/
						
						record.put(Constants.PEOPLE, "");
						record.put(Constants.LOCATION, "");
						record.put(Constants.ORGANIZATION, "");
						record.put(Constants.EVENT, "");
						record.put(Constants.OTHER, "");
						listWeightage.add(record);

						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(sourceIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().totalHits() + "; Records Processed: ");
							listWeightage.clear();

						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						e.printStackTrace();
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
				elasticSearchIndexService.index(sourceIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: ");
				listWeightage.clear();

			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}
	
	public static void main(String[] args) {
		ReprocessByQuery reprocess = new ReprocessByQuery();
		reprocess.reindex(args[0]);
	}
}
