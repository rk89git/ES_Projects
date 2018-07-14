package com.db.cricket.services;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.db.common.constants.Constants.Host;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.cricket.utils.CricketUtils;

/**
 * Handles the auto scheduling of matches. Currently only matches for Team India
 * or IPL will be enabled.
 * 
 * @author Piyush Gupta
 *
 */
public class MatchStartSchedulerRunnable implements Runnable {

	private static final Logger LOG = LogManager.getLogger(MatchStartSchedulerRunnable.class);

	ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = null;

	private List<Integer> webHosts = Arrays.asList(Host.BHASKAR_WEB_HOST, Host.BHASKAR_MOBILE_WEB_HOST,
			Host.DIVYA_WEB_HOST, Host.DIVYA_MOBILE_WEB_HOST);

	private List<Integer> appHosts = Arrays.asList(/* Host.BHASKAR_APP_ANDROID_HOST, */Host.DIVYA_APP_ANDROID_HOST);

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public MatchStartSchedulerRunnable() {
		try {
			initializeClient();
		} catch (RuntimeException e) {
			LOG.error("Error occurred while starting thread for automated start of matches.", e);
			throw new DBAnalyticsException(e);
		}
	}

	@Override
	public void run() {

		/*
		 * Try catch block ensures that the thread is not removed from executors work
		 * queue on encountering an exception.
		 */
		try {
			
			LOG.info("Checking for matches to enable.");

			/*
			 * Get all matches where the widget_global field is not set and the match is an
			 * upcoming match.
			 */
			BoolQueryBuilder builder = new BoolQueryBuilder();
			builder.mustNot(QueryBuilders.existsQuery(CricketConstants.FLAG_WIDGET_GLOBAL)).must(QueryBuilders
					.termQuery(CricketConstants.UPCOMING_FIELD, CricketConstants.MATCH_UPCOMING_OR_LIVE_STATUS));

			SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

			SearchHit[] hits = response.getHits().getHits();

			List<Map<String, Object>> updatesList = new ArrayList<>();

			for (SearchHit hit : hits) {
				Map<String, Object> responseMap = hit.getSource();

				if (responseMap != null && !responseMap.isEmpty()) {

					if (CricketConstants.TEAM_INDIA
							.equalsIgnoreCase((String) responseMap.get(IngestionConstants.TEAM_A))
							|| CricketConstants.TEAM_INDIA
									.equalsIgnoreCase((String) responseMap.get(IngestionConstants.TEAM_B))
							|| responseMap.get(IngestionConstants.SERIES_NAME).toString()
									.contains(CricketConstants.IPL)) {
						/*
						 * For each match, check if the time to start of match is less than 5 hours. If
						 * so, enable the match widget.
						 */
						LocalDateTime matchTime = LocalDateTime.parse(
								responseMap.get(IngestionConstants.MATCH_DATE_IST_FIELD).toString()
										+ responseMap.get(IngestionConstants.MATCH_TIME_IST_FIELD).toString(),
								DateTimeFormatter.ofPattern("M/d/yyyyHH:mm"));
						LocalDateTime currentTime = LocalDateTime.now();

						if (ChronoUnit.HOURS.between(currentTime, matchTime) < 2) {
							LOG.info("Found match to enable: " + responseMap.get(IngestionConstants.MATCH_ID_FIELD));

							Map<String, Object> updateData = new HashMap<>();
							updateData.put(Constants.ROWID, responseMap.get(IngestionConstants.MATCH_ID_FIELD));
							updateData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

							/*
							 * Prepare notifications details.
							 */
							List<String> slIds = new ArrayList<>();

							for (Integer host : webHosts) {
								slIds.add(CricketUtils.getNotificationId(responseMap, host));
							}

							if (Boolean.parseBoolean(
									DBConfig.getInstance().getProperty("cricket.notification.app.enabled"))) {
								for (Integer host : appHosts) {
									slIds.add(CricketUtils.getNotificationId(responseMap, host));
								}
							}

							Map<String, Object> notificationDetails = new HashMap<>();
							notificationDetails.put(CricketConstants.NOTIFICATION_SL_ID_FIELD, slIds);
							notificationDetails.put(CricketConstants.NOTIFICATION_ICON_FIELD,
									CricketConstants.COMMENT_ICON_URL);

							/*
							 * Widget will be enabled on all articles, home page and category page for India
							 * matches.
							 */
							updateData.put(CricketConstants.FLAG_WIDGET_HOME_PAGE, true);
							updateData.put(CricketConstants.FLAG_WIDGET_ARTICLE_PAGE, true);
							updateData.put(CricketConstants.FLAG_WIDGET_SPORTS_ARTICLE_PAGE, false);

							updateData.put(CricketConstants.NOTIFICATION_STATUS_FIELD, true);
							updateData.put(CricketConstants.NOTIFICATION_DETAILS_FIELD, notificationDetails);
							updateData.put(CricketConstants.FLAG_WIDGET_CATEGORY_PAGE, true);
							updateData.put(CricketConstants.FLAG_WIDGET_GLOBAL, true);
							updateData.put(CricketConstants.FLAG_WIDGET_EVENT_ENABLED, true);

							updatesList.add(updateData);
						}
					}
				}
			}

			if (!updatesList.isEmpty()) {
				disablePreviousMatches();
				LOG.info("Enabling matches: " + updatesList);
				elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME,
						updatesList);
			}
		} catch (Exception e) {
			LOG.error("Encountered error in scheduler for auto start of matches: ", e);
		}
	}

	private void disablePreviousMatches() {

		BoolQueryBuilder builder = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery(CricketConstants.RECENT_FIELD, CricketConstants.YES));
		builder.must(QueryBuilders.termQuery(CricketConstants.FLAG_WIDGET_GLOBAL, Boolean.TRUE));

		try {
			SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

			List<Map<String, Object>> updates = new ArrayList<>();

			for (SearchHit hit : response.getHits().getHits()) {

				Map<String, Object> matchDataMap = hit.getSourceAsMap();

				Map<String, Object> updateData = new HashMap<>();
				updateData.put(Constants.ROWID, matchDataMap.get(IngestionConstants.MATCH_ID_FIELD));
				updateData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				updateData.put(CricketConstants.FLAG_WIDGET_GLOBAL, false);
				updateData.put(CricketConstants.FLAG_WIDGET_EVENT_ENABLED, false);
				updates.add(updateData);
			}
			
			if(!updates.isEmpty()) {
				LOG.info("Disabling matches: " + updates);
				elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME,
						updates);
			}
			
		} catch (Exception e) {
			LOG.error("Failed to disable matches, while enabling new match: ", e);
		}
	}

	public static void main(String[] args) {
		MatchStartSchedulerRunnable runnable = new MatchStartSchedulerRunnable();
		runnable.run();
	}
}
