package com.db.cricket.predictnwin.services;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.cricket.model.CricketQuery;

@Service
public class PredictNWinIngestionService {

	private static final Logger LOG = LogManager.getLogger(PredictNWinIngestionService.class);

	private static ElasticSearchIndexService indexService = ElasticSearchIndexService.getInstance();

	private static Client client = null;

	/**
	 * Base Constructor. Initialize based on defaults.
	 */
	PredictNWinIngestionService() {
		try {
			initializeClient();
			LOG.info("Client Initialized.");
		} catch (RuntimeException runtimeException) {
			LOG.error("Received an error while preparing client.", runtimeException);
			throw new DBAnalyticsException(runtimeException);
		}
	}

	/**
	 * Provide a new client instance on each call.
	 * 
	 * Ensures the closure of any existing client before creating a new one.
	 */
	private void initializeClient() {
		if (client != null) {
			client.close();
		}
		client = indexService.getClient();
	}

	/**
	 * Create or update performance record of a player at match end.
	 * 
	 * @param scorecard
	 *            the record representing the cricket score card at match end
	 * @param seriesId
	 */
	@SuppressWarnings("unchecked")
	public static void createOrUpdatePlayerPerformance(Map<String, Object> scorecard) {
		Map<String, Object> matchDetails = (HashMap<String, Object>) scorecard
				.get(IngestionConstants.MATCHDETAIL_FIELD);

		String seriesId = (String) matchDetails.get("Series_Id");
		String matchId = (String) matchDetails.get("Id");

		try {
			List<Map<String, Object>> playersStatList = new ArrayList<>();
			Map<String, Map<String, Object>> playersStatMap = new HashMap<>();

			List<Map<?, ?>> innings = (List<Map<?, ?>>) scorecard.get(IngestionConstants.INNINGS_FIELD);

			/*
			 * Get the list of players in a map.
			 */
			for (Map<?, ?> inning : innings) {

				List<Map<?, ?>> batsmen = (List<Map<?, ?>>) inning.get(IngestionConstants.BATSMEN);
				String teamId = (String) inning.get(IngestionConstants.BATTINGTEAM);

				/*
				 * For each batsman, populate the performance stats.
				 */
				for (Map<?, ?> batsman : batsmen) {
					Map<String, Object> stats = new HashMap<>();

					String id = (String) batsman.get(IngestionConstants.BATSMAN);

					playersStatMap.put(id, stats);
					playersStatList.add(stats);

					stats.put(IngestionConstants.BATSMAN, id);
					stats.put(IngestionConstants.SERIES_ID, seriesId);
					stats.put(IngestionConstants.TEAM_ID, teamId);

					if (StringUtils.isNotBlank((String) batsman.get(IngestionConstants.RUNS))) {
						stats.put(IngestionConstants.RUNS,
								Integer.parseInt((String) batsman.get(IngestionConstants.RUNS)));
					} else {
						stats.put(IngestionConstants.RUNS, 0);
					}

					if (StringUtils.isNotBlank((String) batsman.get(IngestionConstants.BALLS))) {
						stats.put(IngestionConstants.BALLS,
								Integer.parseInt((String) batsman.get(IngestionConstants.BALLS)));
					} else {
						stats.put(IngestionConstants.BALLS, 0);
					}

					if (StringUtils.isNotBlank((String) batsman.get(IngestionConstants.FOURS))) {
						stats.put(IngestionConstants.FOURS,
								Integer.parseInt((String) batsman.get(IngestionConstants.FOURS)));
					} else {
						stats.put(IngestionConstants.FOURS, 0);
					}

					if (StringUtils.isNotBlank((String) batsman.get(IngestionConstants.SIXES))) {
						stats.put(IngestionConstants.SIXES,
								Integer.parseInt((String) batsman.get(IngestionConstants.SIXES)));
					} else {
						stats.put(IngestionConstants.SIXES, 0);
					}

					if (Boolean.parseBoolean(String.valueOf(batsman.get("Isbatting")))
							|| ((String) (batsman.get("Dismissal"))).trim().equalsIgnoreCase("not out")) {

						stats.put(IngestionConstants.NO_OF_DISMISSALS, 0);
					} else {
						stats.put(IngestionConstants.NO_OF_DISMISSALS, 1);
					}

					stats.put(IngestionConstants.NO_OF_PLAYED_MATCHES, 1);
					stats.put(IngestionConstants.STRIKERATE, caclulateBattingStrikeRate(stats));
					stats.put(IngestionConstants.AVERAGE, caclulateBattingAverage(stats));
					stats.put(Constants.ROWID, id + "_" + seriesId);
					stats.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				}
			}

			if (!playersStatMap.isEmpty()) {
				updatePlayerStats(seriesId, playersStatMap);
			}

			indexService.indexOrUpdate(Indexes.CRICKET_PLAYER_PERFORMANCE, MappingTypes.MAPPING_REALTIME,
					playersStatList);
		} catch (Exception e) {
			LOG.error("ERROR while calculating stats of Series " + seriesId + " for match " + matchId, e);
		}
	}

	private static void updatePlayerStats(String seriesId, Map<String, Map<String, Object>> playersStatMap){
		try {
			MultiGetRequestBuilder mGetRequest = client.prepareMultiGet();

			for (String player : playersStatMap.keySet()) {
				mGetRequest.add(Indexes.CRICKET_PLAYER_PERFORMANCE, MappingTypes.MAPPING_REALTIME,
						(player + "_" + seriesId));
			}

			MultiGetResponse mGetResponse = mGetRequest.execute().actionGet();

			for (MultiGetItemResponse item : mGetResponse) {
				if (item.getResponse().isExists()) {
					Map<String, Object> playerOldStats = item.getResponse().getSource();
					Map<String, Object> playerUpdatedStats = playersStatMap
							.get(playerOldStats.get(IngestionConstants.BATSMAN));

					playerUpdatedStats.put(IngestionConstants.RUNS,
							(int) playerUpdatedStats.get(IngestionConstants.RUNS)
									+ (int) playerOldStats.get(IngestionConstants.RUNS));

					playerUpdatedStats.put(IngestionConstants.BALLS,
							(int) playerUpdatedStats.get(IngestionConstants.BALLS)
									+ (int) playerOldStats.get(IngestionConstants.BALLS));

					playerUpdatedStats.put(IngestionConstants.FOURS,
							(int) playerUpdatedStats.get(IngestionConstants.FOURS)
									+ (int) playerOldStats.get(IngestionConstants.FOURS));

					playerUpdatedStats.put(IngestionConstants.SIXES,
							(int) playerUpdatedStats.get(IngestionConstants.SIXES)
									+ (int) playerOldStats.get(IngestionConstants.SIXES));

					playerUpdatedStats.put(IngestionConstants.NO_OF_DISMISSALS,
							(int) playerUpdatedStats.get(IngestionConstants.NO_OF_DISMISSALS)
									+ (int) playerOldStats.get(IngestionConstants.NO_OF_DISMISSALS));

					playerUpdatedStats.put(IngestionConstants.NO_OF_PLAYED_MATCHES,
							(int) playerUpdatedStats.get(IngestionConstants.NO_OF_PLAYED_MATCHES)
									+ (int) playerOldStats.get(IngestionConstants.NO_OF_PLAYED_MATCHES));

					playerUpdatedStats.put(IngestionConstants.STRIKERATE,
							caclulateBattingStrikeRate(playerUpdatedStats));

					playerUpdatedStats.put(IngestionConstants.AVERAGE, caclulateBattingAverage(playerUpdatedStats));
				}
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	private static double caclulateBattingStrikeRate(Map<String, Object> playerStat){
		try {
			double playerRuns = (double) ((int) playerStat.get(IngestionConstants.RUNS));
			int playerBallsPlayed = (int) playerStat.get(IngestionConstants.BALLS);

			if (playerRuns == 0 || playerBallsPlayed == 0) {
				return 0.00;
			} else {
				return BigDecimal.valueOf((playerRuns / playerBallsPlayed) * 100).setScale(2, BigDecimal.ROUND_HALF_UP)
						.doubleValue();
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	private static double caclulateBattingAverage(Map<String, Object> playerStat){
		try {
			double playerRuns = (double) ((int) playerStat.get(IngestionConstants.RUNS));
			int playerDismissals = (int) playerStat.get(IngestionConstants.NO_OF_DISMISSALS);

			if (playerRuns == 0) {
				return 0.00;
			} else if (playerDismissals == 0) {
				return BigDecimal.valueOf(playerRuns).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			} else {
				return BigDecimal.valueOf(playerRuns / playerDismissals).setScale(2, BigDecimal.ROUND_HALF_UP)
						.doubleValue();
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	public static void calcMatchStatsForSeries(CricketQuery query) {

		BoolQueryBuilder scheduleQuery = QueryBuilders.boolQuery();
		scheduleQuery.must(QueryBuilders.termsQuery(IngestionConstants.SERIES_ID, query.getSeriesId()));
		scheduleQuery.mustNot(
				QueryBuilders.termQuery(IngestionConstants.UPCOMING, CricketConstants.MATCH_UPCOMING_OR_LIVE_STATUS));
		scheduleQuery.must(QueryBuilders.termsQuery(IngestionConstants.MATCH_STATUS,
				CricketConstants.STATUS_MATCH_ENDED, CricketConstants.STATUS_MATCH_ABANDONED));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE).setTypes(MappingTypes.MAPPING_REALTIME).setQuery(scheduleQuery)
				.addSort(IngestionConstants.MATCH_DATE_IST_FIELD, SortOrder.ASC)
				.addSort(IngestionConstants.MATCH_DATE_IST_FIELD, SortOrder.ASC).setSize(100).setFetchSource(false).execute()
				.actionGet();

		MultiGetRequestBuilder builder = client.prepareMultiGet();

		if (response.getHits().getTotalHits() > 0) {
			response.getHits()
					.forEach(e -> builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, e.getId()));
		}
		
		MultiGetResponse mGetResponse = builder.execute().actionGet();

		for (MultiGetItemResponse item : mGetResponse) {
			if (item.getResponse().isExists()) {
				createOrUpdatePlayerPerformance(item.getResponse().getSourceAsMap());
			}
		}

	}
}