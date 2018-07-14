package com.db.wisdom.testJobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class Test {


	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ReProcess.class);

	int batchSize = 5000;
	//static String ip = "104.199.156.183";   //global
	public Test() {
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
		try {
			int numRec = 5000;

			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					.setQuery(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte("2018-01-10"))
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {

					Map<String, Object> record = hit.getSource();
					if(!record.get(Constants.STORY_ID_FIELD).equals(hit.getId()))
					{
						System.out.println("id............"+hit.getId());
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public static void main(String[] args) {
		Test reprocess = new Test();
		reprocess.reindex(Indexes.PROB_PREDICTION);
	}

}
