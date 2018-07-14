package com.db.cricket.predictnwin.services;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Cricket.PredictWinConstants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.cricket.model.Batsman;
import com.db.cricket.model.Inning;
import com.db.cricket.model.Scorecard;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
@EnableScheduling
public class BidValidationService {

	private static final Logger LOG = LogManager.getLogger(BidValidationService.class);

	private ElasticSearchIndexService indexService = ElasticSearchIndexService.getInstance();

	private static Map<String, Scorecard> matchCache = new HashMap<>();

	private static Set<String> matchSet = new HashSet<>();

	private static final Gson gson = new GsonBuilder().create();

	private boolean prodMode = true;

	private Client client = null;

	private static Set<String> updatedMatchTeamPlayersCache = new HashSet<>();

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = indexService.getClient();
	}

	public BidValidationService() {
		try {
			initializeClient();
			prodMode = Boolean.parseBoolean(DBConfig.getInstance().getProperty("production.environment"));
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	@Scheduled(fixedRate = 4000)
	private void clear() {
		synchronized (matchCache) {
			matchCache.clear();
		}
	}

	private void updateMatchCache() {

		LOG.info("Updating Match Data in cache... ");

		BoolQueryBuilder matchCacheQuery = QueryBuilders.boolQuery()
				.mustNot(QueryBuilders.termsQuery(IngestionConstants.MATCH_STATUS,
						CricketConstants.MATCH_STATUS_MATCH_ENDED, CricketConstants.MATCH_STATUS_MATCH_ABANDONED))
				.must(QueryBuilders.rangeQuery(IngestionConstants.MATCH_DATE_IST_FIELD)
						.from(DateUtil.getCurrentDate("M/d/yyyy")));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(matchCacheQuery).setFetchSource(false)
				.addSort(IngestionConstants.MATCH_DATE_IST_FIELD, SortOrder.ASC).execute().actionGet();

		MultiGetRequestBuilder builder = client.prepareMultiGet();

		if (response.getHits().getHits().length > 0) {
			response.getHits()
					.forEach(e -> builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, e.getId()));
		}

		if (!prodMode) {
			builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, "184287");
			builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, "184845");
			builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, "184504");
			builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, "184832");
			builder.add(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, "184288");
		}

		MultiGetResponse mGetResponse = builder.execute().actionGet();
		Set<String> matchSetLocal = new HashSet<>();

		for (MultiGetItemResponse getResponse : mGetResponse.getResponses()) {
			String matchId = getResponse.getId();

			if (getResponse.getResponse().isExists()) {
				matchCache.put(matchId, gson.fromJson(getResponse.getResponse().getSourceAsString(), Scorecard.class));
			} else {
				matchCache.put(matchId, null);
			}
			matchSetLocal.add(matchId);
		}

		if (!matchSet.containsAll(matchSetLocal)) {

			synchronized (matchSet) {
				matchSet.clear();
				matchSet.addAll(matchSetLocal);
			}
			updateDisabledPlayersCache();
		}
	}

	public static void updateCache(String matchId, Map<String, Object> scoreCard) {
		if (StringUtils.isBlank(matchId) || (scoreCard == null || scoreCard.isEmpty())) {
			return;
		}

		matchCache.put(matchId, gson.fromJson(gson.toJson(matchCache.get(matchId)), Scorecard.class));
	}

	public Boolean isValid(Map<String, Object> record) {
		String matchId = (String) record.get(PredictWinConstants.MATCH_ID);
		String bidTypeId = (String) record.get(PredictWinConstants.BID_TYPE_ID);

		Scorecard scoreCard = null;

		boolean containsKey = false;
		synchronized (matchCache) {
			if (!matchCache.containsKey(matchId)) {
				updateMatchCache();
				scoreCard = matchCache.get(matchId);
			} else {
				scoreCard = matchCache.get(matchId);
			}
			containsKey = matchCache.containsKey(matchId);
		}

		/*
		 * First check if the cache contains this match id.
		 */
		if (StringUtils.isNotBlank(matchId) && containsKey) {

			/*
			 * Check for null or empty values
			 */
			try {
				synchronized (updatedMatchTeamPlayersCache) {
					if (updatedMatchTeamPlayersCache.contains(bidTypeId)) {
						LOG.error("Invalid Bid as bidtypeId " + bidTypeId + " of match " + matchId
								+ " is already disabled by DISABLED-API");
						return false;
					}
				}
			} catch (Exception e) {
				LOG.error("Error while validating bid with DISABLED-API cache", e);
			}

			LOG.info("Validating Bid Data...");
			if (isRecordValid(record)) {

				String userId = (String) record.get(PredictWinConstants.USER_ID);

				if (scoreCard == null) {
					LOG.info("Valid Bid as Scorecard is null for userId: " + userId);
					return true;
				}

				String bidType = record.get(PredictWinConstants.BID_TYPE).toString();

				LOG.info("Validating Bid for BidType: " + bidType + " And Id "
						+ record.get(PredictWinConstants.BID_TYPE_ID) + "  for userId: " + userId);
				/*
				 * If toss has not occurred, return true.
				 */
				if (PredictWinConstants.BID_TYPE_TOSS.equalsIgnoreCase(bidType)) {

					/*
					 * if (StringUtils.isBlank(scoreCard.getMatchdetail().getTosswonby())) {
					 * LOG.info("Valid Bid for Toss as toss not occured."); return true; } else {
					 * LOG.info("Toss Bid is Invalid"); }
					 */

					if (!CricketConstants.MATCH_STATUS_MATCH_ENDED
							.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())
							&& !CricketConstants.MATCH_STATUS_MATCH_ABANDONED
									.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())) {

						LOG.info("Valid Bid for Team Win as match not ended yet for userId: " + userId);
						return true;
					}
					LOG.error("InValid Bid for " + userId + " Team Win as match ended.");

				} else if (PredictWinConstants.BID_TYPE_TEAM.equalsIgnoreCase(bidType)) {

					if (!CricketConstants.MATCH_STATUS_MATCH_ENDED
							.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())
							&& !CricketConstants.MATCH_STATUS_MATCH_ABANDONED
									.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())) {
						List<Inning> innings = scoreCard.getInnings();

						if (innings == null || innings.isEmpty()) {
							LOG.info("Valid Bid for Team as match has not started for userId: " + userId);
							return true;
						} else {
							if (!scoreCard.getTeamAway().equalsIgnoreCase(bidTypeId)
									&& !scoreCard.getTeamHome().equalsIgnoreCase(bidTypeId)) {
								LOG.info("Bid is invalid as Team Id does not match for user: " + userId);
								return false;
							}

							if ((!scoreCard.getCurrentBattingTeam().equalsIgnoreCase(bidTypeId))
									&& innings.size() < 2) {
								LOG.info("Valid Bid for Team as it is yet to bat for userId: " + userId);
								return true;
							} else if (scoreCard.getCurrentBattingTeam().equalsIgnoreCase(bidTypeId)) {
								LOG.info("Valid Bid for Team as it is in play for userId: " + userId);
								return true;
							} else {
								LOG.info("Team Bid is not valid for user: " + userId);
							}
						}
					}
				} else if (PredictWinConstants.BID_TYPE_PLAYER.equalsIgnoreCase(bidType)) {

					/*
					 * First Check if match is abandoned.
					 */
					if (!CricketConstants.MATCH_STATUS_MATCH_ENDED
							.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())
							&& !CricketConstants.MATCH_STATUS_MATCH_ABANDONED
									.equalsIgnoreCase(scoreCard.getMatchdetail().getStatus())) {
						List<Inning> innings = scoreCard.getInnings();

						if (innings == null || innings.isEmpty()) {
							LOG.info("Valid Bid for Player as match not started  for userId: " + userId);
							return true;
						} else {
							Inning inning = innings.get(innings.size() - 1);

							final String batsmanId = (String) record.get(PredictWinConstants.BID_TYPE_ID);

							if (inning.getBatsmen() == null || inning.getBatsmen().isEmpty()) {
								LOG.info("Valid Bid for Player for userId: " + userId);
								return true;
							} else {
								for (Batsman batsman : inning.getBatsmen()) {
									if (batsman.getBatsman().equalsIgnoreCase(batsmanId)) {
										if (((batsman.getIsBatting() != null && batsman.getIsBatting())
												|| StringUtils.isBlank(batsman.getDismissal()))) {
											LOG.info("Valid Bid for Player for userId: " + userId);
											return true;
										} else {
											LOG.info("Player Bid invalid for user: " + userId + " and Batsman Id: "
													+ batsmanId);
											return false;
										}
									}
								}
								if (innings.size() == 1) {
									LOG.info("Bid is Valid as Players' team has not yet batted for userId: " + userId);
									return true;
								}
							}
						}
					}
				}
			}
		} else {
			LOG.info("Match Id: " + matchId + " Not found for Validation. Marking Record as Invalid. Match Cache: "
					+ matchCache);
		}
		return false;
	}

	@Scheduled(fixedRate = 3000)
	private void updateDisabledPlayersCache() {
		try {
			Set<String> matchSetLocal = null;
			synchronized (matchSet) {
				matchSetLocal = new HashSet<>(matchSet);
			}

			if (!matchSetLocal.isEmpty()) {

				BoolQueryBuilder cqb = QueryBuilders.boolQuery()
						.must(QueryBuilders.termsQuery(IngestionConstants.MATCH_ID_FIELD, matchSetLocal))
						.must(QueryBuilders.termQuery(IngestionConstants.ISENABLED, false));

				SearchResponse ser = client.prepareSearch(Indexes.CRICKET_MATCH_DISABLED_IDS)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(cqb).setSize(100).execute().actionGet();

				SearchHit[] searchHits = ser.getHits().getHits();

				synchronized (updatedMatchTeamPlayersCache) {
					updatedMatchTeamPlayersCache.clear();

					for (SearchHit hit : searchHits) {
						Map<String, Object> disabledMatch = hit.getSource();

						if (StringUtils.isNotBlank((String) disabledMatch.get(Constants.ID))) {
							updatedMatchTeamPlayersCache.add((String) disabledMatch.get(Constants.ID));
						}
					}
				}
				LOG.info("Successfully Updated Disabled Players & Teams cache for matches " + matchSet);
			} else {
				LOG.info("Could not update disabled players & teams cache as no Match is running currently");
			}
		} catch (Exception e) {
			LOG.error("Error while Updating Disabled Players & Teams cache", e);
		}
	}

	private boolean isRecordValid(Map<String, Object> record) {
		return StringUtils.isNotBlank((String) record.get(PredictWinConstants.TICKET_ID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.USER_ID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.VENDOR_ID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.P_VENDOR_ID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.BID_TYPE))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.BID_TYPE_ID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.PREDICTION))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.COINS_BID))
				&& StringUtils.isNotBlank((String) record.get(PredictWinConstants.CREATED_DATE_TIME));
	}

	public static void main(String[] args) {
		BidValidationService service = new BidValidationService();
		service.updateMatchCache();
		service.updateDisabledPlayersCache();

		Map<String, Object> record = new HashMap<>();

		record.put(PredictWinConstants.TICKET_ID, "139");
		record.put(PredictWinConstants.USER_ID, "5");
		record.put("matchId", "184504");
		record.put(PredictWinConstants.VENDOR_ID, "2");
		record.put(PredictWinConstants.P_VENDOR_ID, "1");
		record.put(PredictWinConstants.BID_TYPE, "toss");
		record.put(PredictWinConstants.BID_TYPE_ID, "4");
		record.put(PredictWinConstants.PREDICTION, "190");
		record.put(PredictWinConstants.COINS_BID, "10");
		record.put(PredictWinConstants.CREATED_DATE_TIME, "2018-04-04T18:26:02Z");

		service.isValid(record);
	}
}
