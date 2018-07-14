package com.db.cricket.services;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

/**
 * Handles Automatic stop of matches after completion.
 * 
 * @author Piyush Gupta
 *
 */
public class MatchStopSchedulerRunnable implements Runnable {

	private static final Logger LOG = LogManager.getLogger(MatchStopSchedulerRunnable.class);

	ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = null;

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public MatchStopSchedulerRunnable() {
		try {
			initializeClient();
		} catch (RuntimeException e) {
			LOG.error("Error occurred while starting thread for automated start of matches.", e);
			throw new DBAnalyticsException(e);
		}
	}

	@Override
	public void run() {

		try {
			LOG.info("Checking for matches to disable.");

			/*
			 * Pick records for recent matches for which widget was enabled.
			 */
			BoolQueryBuilder builder = new BoolQueryBuilder();
			builder.must(QueryBuilders.termQuery(CricketConstants.RECENT_FIELD, CricketConstants.YES))
					.must(QueryBuilders.termQuery(CricketConstants.FLAG_WIDGET_GLOBAL, Boolean.TRUE))
					.must(QueryBuilders.termQuery(CricketConstants.FLAG_WIDGET_HOME_PAGE, Boolean.TRUE));

			SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

			SearchHit[] hits = response.getHits().getHits();

			List<Map<String, Object>> updatesList = new ArrayList<>();

			for (SearchHit hit : hits) {
				Map<String, Object> matchDataMap = hit.getSource();

				if (matchDataMap != null && !matchDataMap.isEmpty()) {

					/*
					 * For each match, check if the time after end of match is more than 3 hours. If
					 * so, disable the match widget.
					 */
					LocalDateTime matchTime = LocalDateTime.parse(
							matchDataMap.get(IngestionConstants.END_MATCH_DATE_IST_FIELD).toString()
									+ matchDataMap.get(IngestionConstants.END_MATCH_TIME_IST_FIELD).toString(),
							DateTimeFormatter.ofPattern("M/d/yyyyHH:mm"));
					LocalDateTime currentTime = LocalDateTime.now();

					/*
					 * For Matches involving India, widget will stay on home page for atleast 3
					 * hours. Afterwards the widget will be available on sports category page for a
					 * day.
					 */
					if (ChronoUnit.HOURS.between(matchTime, currentTime) > 3) {
						Map<String, Object> updateData = new HashMap<>();
						updateData.put(Constants.ROWID, matchDataMap.get(IngestionConstants.MATCH_ID_FIELD));
						updateData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						updateData.put(CricketConstants.NOTIFICATION_STATUS_FIELD, false);
						// updateData.put(CricketConstants.FLAG_WIDGET_GLOBAL, false);
						updateData.put(CricketConstants.FLAG_WIDGET_HOME_PAGE, false);
						updateData.put(CricketConstants.FLAG_WIDGET_ARTICLE_PAGE, false);
						updateData.put(CricketConstants.FLAG_WIDGET_CATEGORY_PAGE, false);
						updatesList.add(updateData);
					}
				}
			}

			if (!updatesList.isEmpty()) {
				LOG.info("Disabling matches." + updatesList.toString());
				elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME,
						updatesList);
			}
		} catch (Exception e) {
			LOG.error("Encountered error in scheduler for autostop of matches: ", e);
		}
	}
}
