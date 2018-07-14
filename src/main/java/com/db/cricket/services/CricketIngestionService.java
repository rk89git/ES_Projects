package com.db.cricket.services;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Cricket;
import com.db.common.constants.Constants.Cricket.PredictWinConstants;
import com.db.common.constants.Constants.Cricket.Team;
import com.db.common.constants.Constants.Cricket.TeamStanding;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Constants.LogMessages;
import com.db.common.constants.Constants.NotificationConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.CricketGlobalVariableUtils;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.SelfExpiringHashMap;
import com.db.cricket.model.FellWicketsDetails;
import com.db.cricket.model.InningsBreakNotification;
import com.db.cricket.model.MatchWinNotification;
import com.db.cricket.model.MatchWonTeamDetails;
import com.db.cricket.model.OversNotification;
import com.db.cricket.model.PlayerNotification;
import com.db.cricket.model.TeamInningEndDetails;
import com.db.cricket.model.TeamNotification;
import com.db.cricket.model.TossNotification;
import com.db.cricket.model.TossWonDetails;
import com.db.cricket.model.WicketNotification;
import com.db.cricket.predictnwin.constants.PredictAndWinIndexes;
import com.db.cricket.predictnwin.services.PredictNWinIngestionService;
import com.db.cricket.predictnwin.services.PredictNWinQueryExecutorService;
import com.db.cricket.utils.CricketUtils;
import com.google.common.collect.ImmutableList;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

@Service
public class CricketIngestionService {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private ObjectMapper mapper = new ObjectMapper();

	private static final Logger LOG = LogManager.getLogger(CricketIngestionService.class);

	private static final Integer NOTIFICATION_DATA_LIFETIME = 300000;

	private Map<String, Object> notificationDataMap = new SelfExpiringHashMap<>(NOTIFICATION_DATA_LIFETIME);
	
	private Map<String, Object> eventNotificationFlagCache = new ConcurrentHashMap<>();

	private boolean productionMode = true;

	private boolean schedulerRequired = false;

	private static boolean isBidProcessing = false;

	private static ScheduledExecutorService matchSchedulerService = null;

	private CricketQueryExecutorService cricketQueryExecutorService = new CricketQueryExecutorService();
	
	@Autowired
	private PredictNWinQueryExecutorService predictNWinQueryExecutorService = new PredictNWinQueryExecutorService();

	@Autowired
	private PlayerProfileProcessor profileParser;
	
	public CricketIngestionService() {

		try {
			initializeClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}

		productionMode = Boolean.valueOf(DBConfig.getInstance().getString("production.environment"));

		schedulerRequired = Boolean.valueOf(DBConfig.getInstance().getString("cricket.scheduler.required"));

		/*
		 * Start threads for Auto live and Auto stop of matches.
		 * 
		 * Separate try/catch block to ensure that failure with executors does not
		 * interrupt ingestion of data.
		 */
		if (productionMode && schedulerRequired) {
			try {
				startMatchScheduling();
			} catch (Exception e) {
				LOG.error("Unable to start services for Auto live and Auto stop of Cricket matches.");
			}
		}

	}

	private Client client = null;

	@Autowired
	private CricketNotificationService cricketNotificationHelper = new CricketNotificationService();

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	/**
	 * Start Schedulers for enabling or disabling matches.
	 */
	private void startMatchScheduling() {

		matchSchedulerService = Executors.newScheduledThreadPool(2);

		LOG.info("Starting MatchStartSchedulerService for automated start of matches.");
		matchSchedulerService.scheduleAtFixedRate(new MatchStartSchedulerRunnable(), 0,
				CricketConstants.MATCH_START_SCHEDULER_TIME_IN_HOUR, TimeUnit.HOURS);

		LOG.info("Starting MatchStopSchedulerService for automated stop of matches.");
		matchSchedulerService.scheduleAtFixedRate(new MatchStopSchedulerRunnable(), 0,
				CricketConstants.MATCH_STOP_SCHEDULER_TIME_IN_HOUR, TimeUnit.HOURS);
	}

	/**
	 * Index incoming match data.
	 * 
	 * @param indexName
	 *            name of the index in which to index records
	 * @param fileName
	 *            name of the incoming data file.
	 * @param record
	 *            match data as a map
	 */
	@SuppressWarnings("unchecked")
	public void ingestData(String indexName, String fileName, Map<String, Object> record) {

		ExecutorService eService = Executors.newFixedThreadPool(7);
		if (indexName.equalsIgnoreCase(Indexes.CRICKET_SCORECARD)) {
			record = CricketUtils.modifyMap(record);

			String rowId = (String) ((HashMap<String, Object>) record.get(IngestionConstants.MATCHDETAIL_FIELD))
					.get("Id");

			Map<?, ?> notificationData = getNotificationData(rowId);

			// START: Logic to send notification
			final Map<String, Object> tempRecord = record;
			final GetResponse response = client
					.prepareGet(Indexes.CRICKET_SCORECARD, MappingTypes.MAPPING_REALTIME, rowId).execute().actionGet();
			final Map<?, ?> notificationDataRef = notificationData;

			final WicketNotification wNotification = new WicketNotification();
			final FellWicketsDetails fellWicketDetails = new FellWicketsDetails();
			wNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {

					if (isWicketFell(response, tempRecord, wNotification)) {
						if (productionMode && notificationData != null && !notificationData.isEmpty()) {
							LOG.info("Wicket Notification Submitted");
							cricketNotificationHelper.sendNotification(wNotification, notificationDataRef);
							LOG.info("Wicket Comment Submitted");
							cricketNotificationHelper.sendComment(wNotification);
							try {
								if (isWicketFell2(response, tempRecord, wNotification, fellWicketDetails)) {
									holdExecution(2000);

									while (isBidProcessing) {
										LOG.info(LogMessages.PREDICT_WIN_BID_PROCESSING_MESSAGE);
										holdExecution(3000);
									}
									isBidProcessing = true;
									LOG.info("Going to update bidders details on wicket Fall ");
									updateBiddersDataOnWicketFall(fellWicketDetails);
								}
							} catch (Exception e) {
								LOG.error("Predict N Win: Error occured while getting Player Details.");
							} finally {
								isBidProcessing = false;
							}
						}
					}

					return null;
				}
			});

			final PlayerNotification pNotification = new PlayerNotification();
			pNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() {
					try {
						if (playerFiftyOrHundered(response, tempRecord, pNotification)) {
							if (productionMode && notificationData != null && !notificationData.isEmpty()) {
								LOG.info("Player Fifty Notification Submitted");
								cricketNotificationHelper.sendNotification(pNotification, notificationDataRef);
								LOG.info("Player Fifty Comment Submitted");
								cricketNotificationHelper.sendComment(pNotification);
							}
						}
					} catch (Exception e) {
						LOG.error("Error in sending Player Notification", e);
					}
					return null;
				}
			});

			final TeamNotification tNotification = new TeamNotification();
			tNotification.setMatch_id(rowId);
			eService.submit(() -> {
				try {
					if (teamFiftyOrHundred(response, tempRecord, tNotification)) {
						if (productionMode && notificationData != null && !notificationData.isEmpty()) {
							LOG.info("Team Fifty/Hundred Notification Submitted");
							cricketNotificationHelper.sendNotification(tNotification, notificationDataRef);
							LOG.info("Team Fifty Comment Submitted");
							cricketNotificationHelper.sendComment(tNotification);
						}
					}
				} catch (Exception e) {
					LOG.error("Error in sending team Notification", e);
				}
				return null;
			});

			final InningsBreakNotification inbNotification = new InningsBreakNotification();
			final TeamInningEndDetails teamInningEndDetails = new TeamInningEndDetails();
			inbNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() {
					try {
						if (isInningsBreak(response, tempRecord, inbNotification)) {
							if (productionMode && notificationData != null && !notificationData.isEmpty()) {
								LOG.info("Submitting Innings Break Notification");
								cricketNotificationHelper.sendNotification(inbNotification, notificationDataRef);
								LOG.info("Innings Break Comment Submitted");
								cricketNotificationHelper.sendComment(inbNotification);
								if (isInningsBreak2(response, tempRecord, teamInningEndDetails)) {
									LOG.info("Going to update bidders details at then innning end ");

									holdExecution(2000);
									while (isBidProcessing) {
										LOG.info(LogMessages.PREDICT_WIN_BID_PROCESSING_MESSAGE);
										holdExecution(3000);
									}
									isBidProcessing = true;
									try {
										bidNotOutPlayersRuns(tempRecord);
									} catch (Exception e) {
										LOG.error("Error while biding not out players at match end", e);
									}

									updateBiddersDataOnTeamBreak(teamInningEndDetails);
								}
							}
						}

					} catch (Exception e) {
						LOG.error("Failed to send notification for innings break.", e);
					} finally {
						isBidProcessing = false;
					}
					return null;
				}
			});

			final TossNotification tossNotification = new TossNotification();
			final TossWonDetails tossWonDetails = new TossWonDetails();
			tossNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					try {
						if (isToss(response, tempRecord, tossNotification)) {
							if (productionMode && notificationData != null && !notificationData.isEmpty()) {
								LOG.info("Submitting Toss Notification");
								
								cricketNotificationHelper.sendNotification(tossNotification, notificationDataRef);
								
								if (!Boolean.parseBoolean((String) eventNotificationFlagCache.get(tossNotification.getMatch_id()+"_"+PredictWinConstants.TOSS_EVENT))){
									eventNotificationFlagCache.put(tossNotification.getMatch_id()+"_"+PredictWinConstants.TOSS_EVENT, "true");
									
									//predictNWinQueryExecutorService.notifyLazyUsers("false");
								}
								
								LOG.info("Toss Comment Submitted");
								cricketNotificationHelper.sendComment(tossNotification);
								if (isToss2(response, tempRecord, tossNotification, tossWonDetails)) {
									LOG.info("Going to update bidders details after Toss ");
									holdExecution(2000);
									while (isBidProcessing) {
										LOG.info(LogMessages.PREDICT_WIN_BID_PROCESSING_MESSAGE);
										holdExecution(3000);
									}
									isBidProcessing = true;
									updateBiddersDataOnToss(tossWonDetails);
								}
							}
						}

					} catch (Exception e) {
						LOG.error("Failed to send notification for toss.", e);
					} finally {
						isBidProcessing = false;
					}
					return null;
				}
			});

			final OversNotification oNotification = new OversNotification();
			oNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					try {
						if (hasOversElapsed(response, tempRecord, oNotification)) {
							if (productionMode && notificationData != null && !notificationData.isEmpty()) {
								LOG.info("Submitting Overs Notification");
								cricketNotificationHelper.sendNotification(oNotification, notificationDataRef);
								LOG.info("Overs Elapse Comment Submitted");
								cricketNotificationHelper.sendComment(oNotification);
							}
						}
					} catch (Exception e) {
						LOG.error("Failed to send notification for elapsed overs.", e);
					}
					return null;
				}
			});

			final MatchWinNotification winNotification = new MatchWinNotification();
			final TeamInningEndDetails secondInningEndDetails = new TeamInningEndDetails();
			final MatchWonTeamDetails matchWonTeamDetails = new MatchWonTeamDetails();

			winNotification.setMatch_id(rowId);

			eService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					try {
						if (hasWon(response, tempRecord, winNotification)) {
							if (productionMode && notificationData != null && !notificationData.isEmpty()) {
								LOG.info("Submitting notification for Match Win.");
								cricketNotificationHelper.sendNotification(winNotification, notificationDataRef);
								LOG.info("Match Win Comment Submitted");
								cricketNotificationHelper.sendComment(winNotification);
								if (hasWon2(response, tempRecord, secondInningEndDetails, matchWonTeamDetails)) {
									LOG.info("Going to update bidders details after Win/Lose ");
									holdExecution(2000);
									while (isBidProcessing) {
										LOG.info(LogMessages.PREDICT_WIN_BID_PROCESSING_MESSAGE);
										holdExecution(3000);
									}
									isBidProcessing = true;
									try {
										bidNotOutPlayersRuns(tempRecord);
									} catch (Exception e) {
										LOG.error("Error while biding not out players at match end", e);
									}

									if (!matchWonTeamDetails.getIsDraw()) {
										updateBiddersDataAfterMatchEnd(matchWonTeamDetails);
									}

									updateBiddersDataOnTeamBreak(secondInningEndDetails);

									/*
									 * Sleep for 3 sec so that previous updates are reflected in elasticsearch when
									 * ranking is calculated
									 */
									holdExecution(3000);
									cricketQueryExecutorService.calculateUsersBidRank(matchWonTeamDetails.getMatchId());

									updateProbableNextMatchTeamSquads(matchWonTeamDetails.getMatchId(), tempRecord);
								}
								
								for(String event : PredictWinConstants.EVENTS){
									eventNotificationFlagCache.remove(tossNotification.getMatch_id()+"_"+event);
								}
							}

							PredictNWinIngestionService.createOrUpdatePlayerPerformance(tempRecord);
						}
					} catch (Exception e) {
						LOG.error("Failed to send notification for Match win.", e);
					} finally {
						isBidProcessing = false;
					}
					return null;
				}
			});
			eService.shutdown();
			// END: Logic to send notification

			record.put(Constants.ROWID, rowId);
			index(indexName, record);
		} else if (indexName.equalsIgnoreCase(Indexes.CRICKET_SCHEDULE)) {
			List<Map<String, Object>> matches = (List<Map<String, Object>>) ((HashMap<String, Object>) record
					.get("data")).get("matches");
			for (Map<String, Object> match : matches) {
				String matchId = (String) match.get("match_Id");
				match.put(Constants.ROWID, matchId);
				match.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				String teama = (String) match.get("teama_Id");
				String teamb = (String) match.get("teamb_Id");
				Map<String, Object> namesMap = new HashMap<>();
				for (Entry<String, Object> entry : match.entrySet()) {
					if (entry.getKey().equals("toss_won_by") || entry.getKey().equals("current_batting_team")
							|| entry.getKey().equals("inn_team_1") || entry.getKey().equals("inn_team_2")
							|| entry.getKey().equals("inn_team_3") || entry.getKey().equals("inn_team_4")
							|| entry.getKey().equals("winningteam_Id")) {
						if (entry.getValue().equals(teama)) {
							String teamaName = (String) match.get("teama");
							namesMap.put(entry.getKey() + "_name", teamaName);
						} else if (entry.getValue().equals(teamb)) {
							String teambName = (String) match.get("teamb");
							namesMap.put(entry.getKey() + "_name", teambName);
						}
					}
				}

				match.putAll(namesMap);
			}

			elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, matches);
		} else if (indexName.equalsIgnoreCase(Indexes.CRICKET_COMMENTARY)) {

			String[] fileNameComponents = fileName.split("_");
			String matchId = fileNameComponents[0].substring(fileNameComponents[0].length() - 6);

			record.put(Constants.ROWID, matchId + "_" + fileNameComponents[3]);

			record.put("match_id", matchId);
			record.put("inningsNumber", fileNameComponents[3]);
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			index(indexName, record);
		} else if (indexName.equalsIgnoreCase(Indexes.CRICKET_TEAM_STANDING)) {
			pushCricketTeamStanding(record);
		} else if (indexName.equalsIgnoreCase(Indexes.CRICKET_TEAM_SQUADS)) {
			pushCricketTeamSquads(record);
		} else if (indexName.equalsIgnoreCase(Indexes.CRICKET_PLAYER_PROFILE)) {
			profileParser.processPlayerProfile(record);
		}
	}

	@SuppressWarnings("unchecked")
	private void bidNotOutPlayersRuns(Map<String, Object> currRecord) {
		try {
			
			Map<?,?> matchDetails = (Map<?, ?>) currRecord.get(IngestionConstants.MATCHDETAIL_FIELD);
			String matchId = matchDetails.get("Id").toString();

			boolean isTest = "Test".equalsIgnoreCase((String) matchDetails.get("Type"));
			
			if (currRecord.containsKey(IngestionConstants.INNINGS_FIELD)) {
				List<Map<String, Object>> inningsList = (List<Map<String, Object>>) currRecord
						.get(IngestionConstants.INNINGS_FIELD);

				if (!inningsList.isEmpty()) {
					updateBidNotOutPlayerRuns(matchId, inningsList, isTest);
				}
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	/**
	 * Update bidders data on end of innings
	 * 
	 * @param matchId
	 *            the match id to update against
	 * @param innings
	 *            the innings data to get not out players
	 */
	@SuppressWarnings("unchecked")
	private void updateBidNotOutPlayerRuns(String matchId, Map<String, Object> innings) {
		try {
			List<FellWicketsDetails> notOutWicketPlayersDetail = new ArrayList<>();

			if (innings.containsKey(IngestionConstants.BATSMEN)) {
				List<Map<String, Object>> inningsBatsmen = (List<Map<String, Object>>) innings
						.get(IngestionConstants.BATSMEN);

				for (Map<String, Object> inningsBatsman : inningsBatsmen) {

					if (((inningsBatsman.containsKey("Isbatting"))
							&& (inningsBatsman.get("Isbatting").toString()).trim().equalsIgnoreCase("true"))
							|| (inningsBatsman.get("Dismissal").toString()).trim().equalsIgnoreCase("not out")) {
						FellWicketsDetails notOutWicketDetail = new FellWicketsDetails();

						notOutWicketDetail.setMatchId(matchId);
						notOutWicketDetail
								.setScoredRunsByPlayer(Integer.parseInt(inningsBatsman.get("Runs").toString()));
						notOutWicketDetail.setPlayerId(inningsBatsman.get("Batsman").toString());

						notOutWicketPlayersDetail.add(notOutWicketDetail);
					}
				}
			}

			for (FellWicketsDetails notOutWicketPlayerDetail : notOutWicketPlayersDetail) {
				updateBiddersDataOnWicketFall(notOutWicketPlayerDetail);
			}

		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}
	
	/**
	 * Update bidders data on end of innings
	 * 
	 * @param matchId
	 *            the match id to update against
	 * @param innings
	 *            the innings data to get not out players
	 */
	@SuppressWarnings("unchecked")
	private void updateBidNotOutPlayerRuns(String matchId, List<?> innings, boolean isTest) {
		try {
			List<FellWicketsDetails> notOutWicketPlayersDetail = new ArrayList<>();

			Map<?,?> inning = (Map<?,?>) innings.get(innings.size()-1);
			
			if (inning.containsKey(IngestionConstants.BATSMEN)) {
				List<Map<?, ?>> inningsBatsmen = (List<Map<?, ?>>) inning.get(IngestionConstants.BATSMEN);

				for (Map<?, ?> inningsBatsman : inningsBatsmen) {

					if (Boolean.parseBoolean(String.valueOf(inningsBatsman.get("Isbatting")))
							|| (inningsBatsman.get("Dismissal").toString()).trim().equalsIgnoreCase("not out")) {
						FellWicketsDetails notOutWicketDetail = new FellWicketsDetails();

						notOutWicketDetail.setMatchId(matchId);
						notOutWicketDetail
								.setScoredRunsByPlayer(Integer.parseInt(inningsBatsman.get("Runs").toString()));
						notOutWicketDetail.setPlayerId(inningsBatsman.get("Batsman").toString());
						notOutWicketDetail.setIsTest(isTest);
						notOutWicketDetail.setInning(innings.size());
						notOutWicketPlayersDetail.add(notOutWicketDetail);
					}
				}
			}

			for (FellWicketsDetails notOutWicketPlayerDetail : notOutWicketPlayersDetail) {
				updateBiddersDataOnWicketFall(notOutWicketPlayerDetail);
			}

		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void updateBiddersDataOnWicketFall(FellWicketsDetails fellWicketDetails) {
		LOG.info("Update data on wicket wall : " + fellWicketDetails);
		try {
			Map<String, Set<String>> notificationMap = new HashMap<>();
			List<Map<String, Object>> records = new ArrayList<>();
			Map<String, Map<String, Integer>> pwUsersRankingIncObj = new HashMap<>();
			Map<String, Map<String, Object>> pwUsersRankingUpdateObj = new HashMap<>();

			String matchId = fellWicketDetails.getMatchId();
			BoolQueryBuilder qb = new BoolQueryBuilder();

			if (fellWicketDetails.getMatchId() != null) {
				qb.must(QueryBuilders.termQuery(PredictWinConstants.MATCH_ID, fellWicketDetails.getMatchId()));
			}

			if (fellWicketDetails.getPlayerId() != null) {
				qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE_ID, fellWicketDetails.getPlayerId()));
			}
			
			/*
			 * Handling test match scenario.
			 */
			if(fellWicketDetails.getIsTest()) {
				if(fellWicketDetails.getInning()<3) {
					qb.must(QueryBuilders.termQuery("inning", 1));
				}else {
					qb.must(QueryBuilders.termQuery("inning", 2));
				}
			}
			
			qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE, PredictWinConstants.BID_TYPE_PLAYER));
			// qb.must(QueryBuilders.termQuery(PredictWinConstants.PREDICTION,
			// fellWicketDetails.getScoredRunsByPlayer()));

			qb.must(QueryBuilders.rangeQuery(PredictWinConstants.PREDICTION)
					.gte(fellWicketDetails.getScoredRunsByPlayer() - 3)
					.lte(fellWicketDetails.getScoredRunsByPlayer() + 3));

			// qb.must(QueryBuilders.termQuery((PredictWinConstants.PREDICTION),
			// fellWicketDetails.getScoredRunsByPlayer()));

			// qb.must(QueryBuilders.termQuery(PredictWinConstants.IS_BID_ACCEPTED, true));

			/*
			 * Need to uncomment after test.
			 */
			 qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_WIN, true));
			 qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_REFUND, true));

			LOG.info("####qb ######" + qb.toString());
			long startTime = System.currentTimeMillis();
			int startFrom = 0;
			int numRec = 1000;
			int totalRecordsUpdated = 0;

			SearchResponse searchResponse = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setScroll(new TimeValue(60000)).setQuery(qb)
					.setSize(numRec).execute().actionGet();

			/*
			 * Gain factor for bids that had a correct prediction but were not accepted.
			 */
			double refundFactor = 0.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.reject.refund.factor") != null) {
				refundFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.reject.refund.factor"));
			}

			/*
			 * Gain factor for bids that had a exact prediction and were placed before
			 * match.
			 */
			double prematchBidExactMatchGainFactor = 2.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.prematch.exact.gain.factor") != null) {
				prematchBidExactMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.prematch.exact.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a correct prediction and were placed before
			 * match.
			 */
			double prematchBidRangeMatchGainFactor = 1.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.prematch.range.gain.factor") != null) {
				prematchBidRangeMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.prematch.range.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a exact prediction and were placed during
			 * match.
			 */
			double inmatchBidExactMatchGainFactor = 2.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.inmatch.exact.gain.factor") != null) {
				inmatchBidExactMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.inmatch.exact.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a correct prediction and were placed during
			 * match.
			 */
			double inmatchBidRangeMatchGainFactor = 1.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.inmatch.range.gain.factor") != null) {
				inmatchBidRangeMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.inmatch.range.gain.factor"));
			}

			Map<String, Object> notificationUtil = new HashMap<>();
			
			notificationUtil.put(NotificationConstants.STORY_ID, matchId);
			notificationUtil.put(NotificationConstants.SLID_KEY, matchId+"_"+PredictWinConstants.BID_WIN_EVENT);
			notificationUtil.put(NotificationConstants.CAT_ID, 9);
			notificationUtil.put(NotificationConstants.MESSAGE, PredictWinConstants.BID_WIN_NOTIFICATION_MESSAGE);
			
			while (true) {
				if (searchResponse.getHits().getHits().length > 0) {
					Map<String, Object> userWiseCoinMaps = new HashMap<>();
					startFrom += searchResponse.getHits().getHits().length;
					LOG.info("#####Index: users_bids for player bids " + ", Mapping: " + MappingTypes.MAPPING_REALTIME
							+ "total hits : " + searchResponse.getHits().getTotalHits() + ", Fetched Hits: "
							+ searchResponse.getHits().getHits().length + " , Total fetched : " + startFrom);

					for (SearchHit hit : searchResponse.getHits().getHits()) {

						Map<String, Object> source = hit.getSourceAsMap();

						prepareNotificationTarget(source, notificationMap);

						Map<String, Object> map = new HashMap<>();

						String userId = (String) source.get(PredictWinConstants.USER_ID);
						int prediction = Integer.parseInt((String) source.get(PredictWinConstants.PREDICTION));
						int coinsBid = Integer.parseInt((String) source.get(PredictWinConstants.COINS_BID));

						map.put(Constants.ROWID, hit.getId());
						map.put(PredictWinConstants.IS_WIN, true);

						int coinsGained = (int) (coinsBid * inmatchBidRangeMatchGainFactor);

						/*
						 * Differential gains for bids placed before match and bids placed during a
						 * match.
						 */
						if (Boolean.parseBoolean(String.valueOf(source.get(PredictWinConstants.IS_BID_ACCEPTED)))) {

							if (Boolean.parseBoolean(String.valueOf(source.get(PredictWinConstants.PRE_MATCH_BID)))) {

								if (fellWicketDetails.getScoredRunsByPlayer() == prediction) {
									coinsGained = (int) (coinsBid * prematchBidExactMatchGainFactor);
									map.put(PredictWinConstants.GAIN_FACTOR, prematchBidExactMatchGainFactor);
									map.put(Constants.STATUS, 4);
								} else {
									coinsGained = (int) (coinsBid * prematchBidRangeMatchGainFactor);
									map.put(PredictWinConstants.GAIN_FACTOR, prematchBidRangeMatchGainFactor);
									map.put(Constants.STATUS, 3);
								}

							} else {
								if (fellWicketDetails.getScoredRunsByPlayer() == prediction) {
									coinsGained = (int) (coinsBid * inmatchBidExactMatchGainFactor);
									map.put(PredictWinConstants.GAIN_FACTOR, inmatchBidExactMatchGainFactor);
									map.put(Constants.STATUS, 1);
								} else {
									map.put(PredictWinConstants.GAIN_FACTOR, inmatchBidRangeMatchGainFactor);
									map.put(Constants.STATUS, 0);
								}
							}

						} else {
							coinsGained = (int) (coinsBid * refundFactor);
							map.put(Constants.STATUS, 2);
							map.put(PredictWinConstants.IS_WIN, false);
							map.put(PredictWinConstants.IS_REFUND, true);
							map.put(PredictWinConstants.REFUND_FACTOR, refundFactor);
						}

						map.put(PredictWinConstants.COINS_GAINED, coinsGained);
						map.put(PredictWinConstants.UPDATED_DATE_TIME, DateUtil.getCurrentDateTime());
						records.add(map);

						String ticketId = (String) source.get(PredictWinConstants.TICKET_ID);
						Map<String, Object> ticketCoinMap = new HashMap<>();
						ticketCoinMap.put("ticketid", ticketId);
						ticketCoinMap.put("gaincoin", map.get(PredictWinConstants.COINS_GAINED));
						try {
							if (userWiseCoinMaps.get(userId) != null) {
								((List<Map<String, Object>>) userWiseCoinMaps.get(userId)).add(ticketCoinMap);
							} else {
								userWiseCoinMaps.put(userId, new ArrayList<>(Arrays.asList(ticketCoinMap)));
							}
						} catch (Exception e) {
							LOG.error("Error in creating map for sql post request", e);
						}

						preparePwUsersRankingIncObj(matchId, userId, pwUsersRankingIncObj, pwUsersRankingUpdateObj,
								coinsGained);
					}
					// Create Ranking
					try {
						elasticSearchIndexService.updateDocWithIncrementCounter(Indexes.PW_USER_RANKING,
								MappingTypes.MAPPING_REALTIME, pwUsersRankingIncObj, pwUsersRankingUpdateObj);
					} catch (Exception e) {
						LOG.error("Error while ingesting data in " + Indexes.PW_USER_RANKING + " index", e);
					}
					pwUsersRankingIncObj.clear();
					pwUsersRankingUpdateObj.clear();

					// post updated coins to sql
					if (!userWiseCoinMaps.isEmpty() && productionMode) {
						Map<String, Object> inputObjectCoinMaps = new HashMap<>();
						inputObjectCoinMaps.put("getdata", userWiseCoinMaps);
						CricketUtils.getBidPointsUpdated(inputObjectCoinMaps);
						userWiseCoinMaps.clear();
					}

					// Send Notification
					cricketNotificationHelper.sendBidWinNotifications(notificationMap, notificationUtil);
					notificationMap.clear();
				}
				if (!records.isEmpty()) {
					int updatedRecords = elasticSearchIndexService.indexOrUpdate(PredictAndWinIndexes.USERS_BIDS,
							MappingTypes.MAPPING_REALTIME, records);
					totalRecordsUpdated = totalRecordsUpdated + updatedRecords;
					records.clear();
				}

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();

				// Break condition: No hits are returned
				if (searchResponse.getHits().getHits().length == 0) {
					break;
				}
			}
			long endTime = System.currentTimeMillis();
			LOG.info("#####Total updated records for coinsGained : " + totalRecordsUpdated
					+ ". Excecution Time (Seconds) " + (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("####Error occured while processing bid details.", e);
		}
	}

	@SuppressWarnings("unchecked")
	private void updateBiddersDataOnTeamBreak(TeamInningEndDetails teamInningEndDetails) {
		LOG.info("####Update data on team break : " + teamInningEndDetails);
		Map<String, Set<String>> notificationMap = new HashMap<>();
		List<Map<String, Object>> records = new ArrayList<>();
		Map<String, Map<String, Integer>> pwUsersRankingIncObj = new HashMap<>();
		Map<String, Map<String, Object>> pwUsersRankingUpdateObj = new HashMap<>();

		String matchId = teamInningEndDetails.getMatchId();
		BoolQueryBuilder qb = new BoolQueryBuilder();
		if (matchId != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.MATCH_ID, matchId));
		}
		if (teamInningEndDetails.getTeamId() != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE_ID, teamInningEndDetails.getTeamId()));
		}
		qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE, PredictWinConstants.BID_TYPE_TEAM));
		// qb.must(QueryBuilders.termQuery(PredictWinConstants.PREDICTION,
		// fellWicketDetails.getScoredRunsByPlayer()));

		/*
		 * Handling test match scenario.
		 */
		if(teamInningEndDetails.getIsTest()) {
			if(teamInningEndDetails.getInning()<3) {
				qb.must(QueryBuilders.termQuery("inning", 1));
			}else {
				qb.must(QueryBuilders.termQuery("inning", 2));
			}
		}
		
		qb.must(QueryBuilders.rangeQuery(PredictWinConstants.PREDICTION)
				.gte(teamInningEndDetails.getScoredRunsByTeam() - 3)
				.lte(teamInningEndDetails.getScoredRunsByTeam() + 3));

		// qb.must(QueryBuilders.termQuery(PredictWinConstants.PREDICTION,
		// teamInningEndDetails.getScoredRunsByTeam()));
		// qb.must(QueryBuilders.termQuery(PredictWinConstants.IS_BID_ACCEPTED, true));
		 qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_WIN, true));
		 qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_REFUND, true));

		try {
			long startTime = System.currentTimeMillis();
			int startFrom = 0;
			int numRec = 1000;
			int totalRecordsUpdated = 0;

			SearchResponse searchResponse = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setScroll(new TimeValue(60000)).setQuery(qb)
					.setSize(numRec).execute().actionGet();

			/*
			 * Gain factor for bids that had a correct prediction but were not accepted.
			 */
			double refundFactor = 0.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.reject.refund.factor") != null) {
				refundFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.reject.refund.factor"));
			}

			/*
			 * Gain factor for bids that had a exact prediction and were placed before
			 * match.
			 */
			double prematchBidExactMatchGainFactor = 2.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.prematch.exact.gain.factor") != null) {
				prematchBidExactMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.prematch.exact.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a correct prediction and were placed before
			 * match.
			 */
			double prematchBidRangeMatchGainFactor = 1.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.prematch.range.gain.factor") != null) {
				prematchBidRangeMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.prematch.range.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a exact prediction and were placed during
			 * match.
			 */
			double inmatchBidExactMatchGainFactor = 2.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.inmatch.exact.gain.factor") != null) {
				inmatchBidExactMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.inmatch.exact.gain.factor"));
			}

			/*
			 * Gain factor for bids that had a correct prediction and were placed during
			 * match.
			 */
			double inmatchBidRangeMatchGainFactor = 1.0;

			if (DBConfig.getInstance().getProperty("predictwin.bid.inmatch.range.gain.factor") != null) {
				inmatchBidRangeMatchGainFactor = Double
						.parseDouble(DBConfig.getInstance().getProperty("predictwin.bid.inmatch.range.gain.factor"));
			}

			Map<String, Object> notificationUtil = new HashMap<>();
			
			notificationUtil.put(NotificationConstants.STORY_ID, matchId);
			notificationUtil.put(NotificationConstants.SLID_KEY, matchId+"_"+PredictWinConstants.BID_WIN_EVENT);
			notificationUtil.put(NotificationConstants.CAT_ID, 9);
			notificationUtil.put(NotificationConstants.MESSAGE, PredictWinConstants.BID_WIN_NOTIFICATION_MESSAGE);
			
			while (true) {
				if (searchResponse.getHits().getHits().length > 0) {
					Map<String, Object> userWiseCoinMaps = new HashMap<>();
					startFrom += searchResponse.getHits().getHits().length;
					LOG.info("####Index: users_bids " + ", Mapping: " + MappingTypes.MAPPING_REALTIME + "total hits : "
							+ searchResponse.getHits().getTotalHits() + ", Fetched Hits: "
							+ searchResponse.getHits().getHits().length + " , Total fetched : " + startFrom);

					for (SearchHit hit : searchResponse.getHits().getHits()) {
						Map<String, Object> source = hit.getSourceAsMap();

						prepareNotificationTarget(source, notificationMap);

						String userId = (String) source.get(PredictWinConstants.USER_ID);
						int prediction = Integer.parseInt((String) source.get(PredictWinConstants.PREDICTION));
						int coinsBid = Integer.parseInt((String) source.get(PredictWinConstants.COINS_BID));
						Map<String, Object> map = new HashMap<>();
						map.put(Constants.ROWID, hit.getId());
						map.put(PredictWinConstants.IS_WIN, true);

						int coinsGained = (int) (coinsBid * inmatchBidRangeMatchGainFactor);

						/*
						 * Differential gains for bids placed before match and bids placed during a
						 * match.
						 */
						if (Boolean.parseBoolean(String.valueOf(source.get(PredictWinConstants.IS_BID_ACCEPTED)))) {

							if (Boolean.parseBoolean(String.valueOf(source.get(PredictWinConstants.PRE_MATCH_BID)))) {

								if (teamInningEndDetails.getScoredRunsByTeam() == prediction) {
									coinsGained = (int) (coinsBid * prematchBidExactMatchGainFactor);
									map.put("gainFactor", prematchBidExactMatchGainFactor);
									map.put(Constants.STATUS, 4);
								} else {
									coinsGained = (int) (coinsBid * prematchBidRangeMatchGainFactor);
									map.put("gainFactor", prematchBidRangeMatchGainFactor);
									map.put(Constants.STATUS, 3);
								}

							} else {
								if (teamInningEndDetails.getScoredRunsByTeam() == prediction) {
									coinsGained = (int) (coinsBid * inmatchBidExactMatchGainFactor);
									map.put(PredictWinConstants.GAIN_FACTOR, inmatchBidExactMatchGainFactor);
									map.put(Constants.STATUS, 1);
								} else {
									map.put(PredictWinConstants.GAIN_FACTOR, inmatchBidRangeMatchGainFactor);
									map.put(Constants.STATUS, 0);
								}
							}

						} else {
							coinsGained = (int) (coinsBid * refundFactor);
							map.put(Constants.STATUS, 2);
							map.put(PredictWinConstants.IS_WIN, false);
							map.put(PredictWinConstants.IS_REFUND, true);
							map.put(PredictWinConstants.REFUND_FACTOR, refundFactor);
						}

						map.put(PredictWinConstants.COINS_GAINED, coinsGained);
						map.put(PredictWinConstants.UPDATED_DATE_TIME, DateUtil.getCurrentDateTime());

						records.add(map);
						String ticketId = (String) source.get(PredictWinConstants.TICKET_ID);
						Map<String, Object> ticketCoinMap = new HashMap<>();
						ticketCoinMap.put("ticketid", ticketId);
						ticketCoinMap.put("gaincoin", map.get(PredictWinConstants.COINS_GAINED));
						try {
							if (userWiseCoinMaps.get(userId) != null) {
								((List<Map<String, Object>>) userWiseCoinMaps.get(userId)).add(ticketCoinMap);
							} else {
								userWiseCoinMaps.put(userId, new ArrayList<>(Arrays.asList(ticketCoinMap)));
							}
						} catch (Exception e) {
							LOG.error("Error in creating map for sql post request", e);
						}

						preparePwUsersRankingIncObj(matchId, userId, pwUsersRankingIncObj, pwUsersRankingUpdateObj,
								coinsGained);
					}

					// Create Ranking
					try {
						elasticSearchIndexService.updateDocWithIncrementCounter(Indexes.PW_USER_RANKING,
								MappingTypes.MAPPING_REALTIME, pwUsersRankingIncObj, pwUsersRankingUpdateObj);
					} catch (Exception e) {
						LOG.error("Error while ingesting data in " + Indexes.PW_USER_RANKING + " index", e);
					}
					pwUsersRankingIncObj.clear();
					pwUsersRankingUpdateObj.clear();
					// post updated coins to sql

					if (!userWiseCoinMaps.isEmpty() && productionMode) {
						Map<String, Object> inputObjectCoinMaps = new HashMap<>();
						inputObjectCoinMaps.put("getdata", userWiseCoinMaps);
						CricketUtils.getBidPointsUpdated(inputObjectCoinMaps);
						userWiseCoinMaps.clear();
					}

					// Send Notification
					cricketNotificationHelper.sendBidWinNotifications(notificationMap, notificationUtil);
					notificationMap.clear();
				}

				if (!records.isEmpty()) {
					int updatedRecords = elasticSearchIndexService.indexOrUpdate(PredictAndWinIndexes.USERS_BIDS,
							MappingTypes.MAPPING_REALTIME, records);
					totalRecordsUpdated = totalRecordsUpdated + updatedRecords;
					records.clear();
				}

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();

				// Break condition: No hits are returned
				if (searchResponse.getHits().getHits().length == 0) {
					break;
				}
			}

			long endTime = System.currentTimeMillis();
			LOG.info("####Total updated records for coinsGained : " + totalRecordsUpdated
					+ ". Excecution Time (Seconds) " + (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("#####Error occured while processing bid details.", e);
		}

	}

	@SuppressWarnings("unchecked")
	private void updateBiddersDataOnToss(TossWonDetails tossWonDetails) {
		LOG.info("####Update data on toss done : " + tossWonDetails);
		Map<String, Set<String>> notificationMap = new HashMap<>();
		List<Map<String, Object>> records = new ArrayList<>();
		Map<String, Map<String, Integer>> pwUsersRankingIncObj = new HashMap<>();
		Map<String, Map<String, Object>> pwUsersRankingUpdateObj = new HashMap<>();

		String matchId = tossWonDetails.getMatchId();
		BoolQueryBuilder qb = new BoolQueryBuilder();
		if (matchId != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.MATCH_ID, matchId));
		}
		if (tossWonDetails.getTeamId() != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE_ID, tossWonDetails.getTeamId()));
		} else {
			LOG.error("Null issue of Team id. Object: " + tossWonDetails);
		}

		qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE, "toss"));
		qb.must(QueryBuilders.termQuery(PredictWinConstants.PREDICTION, tossWonDetails.getTeamId()));
		qb.must(QueryBuilders.termQuery(PredictWinConstants.IS_BID_ACCEPTED, true));
		qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_WIN, true));

		try {
			long startTime = System.currentTimeMillis();
			int startFrom = 0;
			int numRec = 1000;
			int totalRecordsUpdated = 0;

			SearchResponse searchResponse = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setScroll(new TimeValue(60000)).setQuery(qb)
					.setSize(numRec).execute().actionGet();
			
			Map<String, Object> notificationUtil = new HashMap<>();
			
			notificationUtil.put(NotificationConstants.STORY_ID, matchId);
			notificationUtil.put(NotificationConstants.SLID_KEY, matchId+"_"+PredictWinConstants.BID_WIN_EVENT);
			notificationUtil.put(NotificationConstants.CAT_ID, 9);
			notificationUtil.put(NotificationConstants.MESSAGE, PredictWinConstants.BID_WIN_NOTIFICATION_MESSAGE);

			while (true) {
				if (searchResponse.getHits().getHits().length > 0) {
					Map<String, Object> userWiseCoinMaps = new HashMap<>();
					startFrom += searchResponse.getHits().getHits().length;
					LOG.info("###Index: users_bids " + ", Mapping: " + MappingTypes.MAPPING_REALTIME + "total hits : "
							+ searchResponse.getHits().getTotalHits() + ", Fetched Hits: "
							+ searchResponse.getHits().getHits().length + " , Total fetched : " + startFrom);
					for (SearchHit hit : searchResponse.getHits().getHits()) {

						Map<String, Object> source = hit.getSourceAsMap();

						prepareNotificationTarget(source, notificationMap);

						String userId = (String) source.get(PredictWinConstants.USER_ID);
						int coinsBid = Integer.parseInt((String) source.get(PredictWinConstants.COINS_BID));
						Map<String, Object> map = new HashMap<>();
						map.put(Constants.ROWID, hit.getId());
						map.put(PredictWinConstants.IS_WIN, true);
						map.put(PredictWinConstants.COINS_GAINED, coinsBid * 2);
						map.put(Constants.STATUS, 1);
						records.add(map);

						String ticketId = (String) source.get(PredictWinConstants.TICKET_ID);
						Map<String, Object> ticketCoinMap = new HashMap<>();
						ticketCoinMap.put("ticketid", ticketId);
						ticketCoinMap.put("gaincoin", map.get(PredictWinConstants.COINS_GAINED));
						try {
							if (userWiseCoinMaps.get(userId) != null) {
								((List<Map<String, Object>>) userWiseCoinMaps.get(userId)).add(ticketCoinMap);
							} else {
								userWiseCoinMaps.put(userId, new ArrayList<>(Arrays.asList(ticketCoinMap)));
							}
						} catch (Exception e) {
							LOG.error("Error in creating map for sql post request", e);
						}

						preparePwUsersRankingIncObj(matchId, userId, pwUsersRankingIncObj, pwUsersRankingUpdateObj,
								coinsBid * 2);
					}

					// Create Ranking
					try {
						elasticSearchIndexService.updateDocWithIncrementCounter(Indexes.PW_USER_RANKING,
								MappingTypes.MAPPING_REALTIME, pwUsersRankingIncObj, pwUsersRankingUpdateObj);
					} catch (Exception e) {
						LOG.error("Error while ingesting data in " + Indexes.PW_USER_RANKING + " index", e);
					}
					pwUsersRankingIncObj.clear();
					pwUsersRankingUpdateObj.clear();

					if (!userWiseCoinMaps.isEmpty() && productionMode) {
						Map<String, Object> inputObjectCoinMaps = new HashMap<>();
						inputObjectCoinMaps.put("getdata", userWiseCoinMaps);
						CricketUtils.getBidPointsUpdated(inputObjectCoinMaps);
						userWiseCoinMaps.clear();
					}

					// Send Notification
					cricketNotificationHelper.sendBidWinNotifications(notificationMap, notificationUtil);
					notificationMap.clear();
				}

				if (!records.isEmpty()) {
					int updatedRecords = elasticSearchIndexService.indexOrUpdate(PredictAndWinIndexes.USERS_BIDS,
							MappingTypes.MAPPING_REALTIME, records);
					totalRecordsUpdated = totalRecordsUpdated + updatedRecords;
					records.clear();
				}
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();

				// Break condition: No hits are returned
				if (searchResponse.getHits().getHits().length == 0) {
					break;
				}
			}

			long endTime = System.currentTimeMillis();
			LOG.info("####Total updated records for coinsGained : " + totalRecordsUpdated
					+ ". Excecution Time (Seconds) " + (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("#####Error occured while processing bid details.", e);
		}

	}

	@SuppressWarnings("unchecked")
	private void updateBiddersDataAfterMatchEnd(MatchWonTeamDetails matchWonTeamDetails) {
		LOG.info("####Update data after match end : " + matchWonTeamDetails);
		Map<String, Set<String>> notificationMap = new HashMap<>();
		List<Map<String, Object>> records = new ArrayList<>();
		Map<String, Map<String, Integer>> pwUsersRankingIncObj = new HashMap<>();
		Map<String, Map<String, Object>> pwUsersRankingUpdateObj = new HashMap<>();

		String matchId = matchWonTeamDetails.getMatchId();
		BoolQueryBuilder qb = new BoolQueryBuilder();

		if (matchId != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.MATCH_ID, matchId));
		}
		if (matchWonTeamDetails.getTeamId() != null) {
			qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE_ID, matchWonTeamDetails.getTeamId()));
		}
		qb.must(QueryBuilders.termQuery(PredictWinConstants.BID_TYPE, PredictWinConstants.BID_TYPE_TOSS));
		qb.must(QueryBuilders.termQuery(PredictWinConstants.PREDICTION, matchWonTeamDetails.getTeamId()));
		qb.must(QueryBuilders.termQuery(PredictWinConstants.IS_BID_ACCEPTED, true));
		qb.mustNot(QueryBuilders.termQuery(PredictWinConstants.IS_WIN, true));

		try {
			long startTime = System.currentTimeMillis();
			int startFrom = 0;
			int numRec = 1000;
			int totalRecordsUpdated = 0;

			SearchResponse searchResponse = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setScroll(new TimeValue(60000)).setQuery(qb)
					.setSize(numRec).execute().actionGet();
			
			Map<String, Object> notificationUtil = new HashMap<>();
			
			notificationUtil.put(NotificationConstants.STORY_ID, matchId);
			notificationUtil.put(NotificationConstants.SLID_KEY, matchId+"_"+PredictWinConstants.BID_WIN_EVENT);
			notificationUtil.put(NotificationConstants.CAT_ID, 9);
			notificationUtil.put(NotificationConstants.MESSAGE, PredictWinConstants.BID_WIN_NOTIFICATION_MESSAGE);

			while (true) {
				if (searchResponse.getHits().getHits().length > 0) {
					Map<String, Object> userWiseCoinMaps = new HashMap<>();
					startFrom += searchResponse.getHits().getHits().length;
					LOG.info("####Index: users_bids " + ", Mapping: " + MappingTypes.MAPPING_REALTIME + "total hits : "
							+ searchResponse.getHits().getTotalHits() + ", Fetched Hits: "
							+ searchResponse.getHits().getHits().length + " , Total fetched : " + startFrom);

					for (SearchHit hit : searchResponse.getHits().getHits()) {
						Map<String, Object> source = hit.getSourceAsMap();

						prepareNotificationTarget(source, notificationMap);

						String userId = (String) source.get(PredictWinConstants.USER_ID);
						int coinsBid = Integer.parseInt((String) source.get(PredictWinConstants.COINS_BID));
						Map<String, Object> map = new HashMap<>();
						map.put(Constants.ROWID, hit.getId());
						map.put(PredictWinConstants.IS_WIN, true);

						int coinsGained = coinsBid * 2;

						map.put(PredictWinConstants.COINS_GAINED, coinsGained);

						records.add(map);

						String ticketId = (String) source.get(PredictWinConstants.TICKET_ID);
						Map<String, Object> ticketCoinMap = new HashMap<>();
						ticketCoinMap.put("ticketid", ticketId);
						ticketCoinMap.put("gaincoin", map.get(PredictWinConstants.COINS_GAINED));
						try {
							if (userWiseCoinMaps.get(userId) != null) {
								((List<Map<String, Object>>) userWiseCoinMaps.get(userId)).add(ticketCoinMap);
							} else {
								userWiseCoinMaps.put(userId, new ArrayList<>(Arrays.asList(ticketCoinMap)));
							}
						} catch (Exception e) {
							LOG.error("Error in creating map for sql post request", e);
						}

						preparePwUsersRankingIncObj(matchId, userId, pwUsersRankingIncObj, pwUsersRankingUpdateObj,
								coinsGained);
					}
					// Create Ranking
					try {
						elasticSearchIndexService.updateDocWithIncrementCounter(Indexes.PW_USER_RANKING,
								MappingTypes.MAPPING_REALTIME, pwUsersRankingIncObj, pwUsersRankingUpdateObj);
					} catch (Exception e) {
						LOG.error("Error while ingesting data in " + Indexes.PW_USER_RANKING + " index", e);
					}
					pwUsersRankingIncObj.clear();
					pwUsersRankingUpdateObj.clear();

					if (!userWiseCoinMaps.isEmpty() && productionMode) {
						Map<String, Object> inputObjectCoinMaps = new HashMap<>();
						inputObjectCoinMaps.put("getdata", userWiseCoinMaps);
						CricketUtils.getBidPointsUpdated(inputObjectCoinMaps);
						userWiseCoinMaps.clear();
					}

					// Send Notification
					cricketNotificationHelper.sendBidWinNotifications(notificationMap, notificationUtil);
					notificationMap.clear();
				}

				if (!records.isEmpty()) {
					int updatedRecords = elasticSearchIndexService.indexOrUpdate(PredictAndWinIndexes.USERS_BIDS,
							MappingTypes.MAPPING_REALTIME, records);
					totalRecordsUpdated = totalRecordsUpdated + updatedRecords;
					records.clear();
				}
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();

				// Break condition: No hits are returned
				if (searchResponse.getHits().getHits().length == 0) {
					break;
				}
			}

			long endTime = System.currentTimeMillis();
			LOG.info("####Total updated records for coinsGained : " + totalRecordsUpdated
					+ ". Excecution Time (Seconds) " + (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("#####Error occured while processing bid details.", e);
		}

	}

	public void index(String indexName, Map<String, Object> record) {
		if (!record.containsKey(Constants.DATE_TIME_FIELD)) {
			record.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		}

		if (record.containsKey(Constants.ROWID)) {
			elasticSearchIndexService.indexOrUpdate(indexName, MappingTypes.MAPPING_REALTIME, ImmutableList.of(record));
		} else {
			elasticSearchIndexService.index(indexName, MappingTypes.MAPPING_REALTIME, ImmutableList.of(record));
		}

	}

	/**
	 * Check if a player has completed fifty, hundred, etc.
	 * 
	 * @param response
	 *            elasticsearch response containing previous <code>Scorecard</code>
	 *            record
	 * @param tempRecord
	 *            current scorecard record
	 * @param pNotification
	 *            player notification
	 * @return true if player has completed a fifty.
	 */
	@SuppressWarnings("unchecked")
	private boolean playerFiftyOrHundered(GetResponse response, Map<String, Object> tempRecord,
			PlayerNotification pNotification) {

		Map<String, Object> responseMap = response.getSource();
		Map<String, Object> currentBatsmanRuns = new HashMap<>();
		Map<String, Object> currentBatsmanBallsPlayed = new HashMap<>();
		if (tempRecord.get("Innings") != null) {
			List<?> innings = ((List<?>) tempRecord.get("Innings"));
			if (!innings.isEmpty()) {
				Map<?, ?> currInnings = (Map<?, ?>) innings.get(innings.size() - 1);
				for (Map<?, ?> batsman : (List<Map<?, ?>>) currInnings.get(IngestionConstants.BATSMEN)) {
					if (batsman.containsKey("Isbatting")) {
						currentBatsmanRuns.put((String) batsman.get("Batsman_Name"), batsman.get("Runs"));
						currentBatsmanBallsPlayed.put((String) batsman.get("Batsman_Name"), batsman.get("Balls"));
					}
				}
			}
		}

		if (responseMap != null && responseMap.get("Innings") != null) {
			int previous_runs = 0;
			List<?> innings = (List<?>) responseMap.get("Innings");
			Map<?, ?> prevInnings = (Map<?, ?>) innings.get(innings.size() - 1);

			for (Map<?, ?> batsman : (List<Map<?, ?>>) prevInnings.get(IngestionConstants.BATSMEN)) {

				if (batsman.containsKey("Isbatting")) {
					previous_runs = Integer.parseInt((String) batsman.get("Runs"));
					String batsManName = (String) batsman.get("Batsman_Name");
					String batsManRuns = (String) currentBatsmanRuns.get(batsManName);
					if (StringUtils.isNotBlank(batsManRuns)
							&& ((previous_runs < 50 && Integer.parseInt(batsManRuns) >= 50)
									|| (previous_runs < 100 && Integer.parseInt(batsManRuns) >= 100))) {

						pNotification.setPlayerName(batsManName);
						pNotification.setRuns(batsManRuns);
						pNotification.setTeamName((String) prevInnings.get("Team_Name"));
						pNotification.setTotalWickets(((String) prevInnings.get("Wickets")));
						pNotification.setTotalRuns((String) prevInnings.get("Total"));
						pNotification.setOvers((String) prevInnings.get(IngestionConstants.OVERS_FIELD));

						pNotification.setSeriesName(
								(String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
										.get(IngestionConstants.SERIES_NAME_FIELD));

						pNotification
								.setTourName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
										.get(IngestionConstants.TOUR_NAME_FIELD));

						pNotification
								.setBallsPlayed((String) currentBatsmanBallsPlayed.get(batsman.get("Batsman_Name")));
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Check if team has scored Fifty, Hundred, etc.
	 * 
	 * @param response
	 *            response containing previous score card.
	 * @param tempRecord
	 *            current scorecard record
	 * @param tNotification
	 *            team notification
	 * @return
	 */
	private boolean teamFiftyOrHundred(GetResponse response, Map<String, Object> tempRecord,
			TeamNotification tNotification) {

		Map<String, Object> responseMap = response.getSource();
		int currentTeamRuns = 0;
		if (tempRecord.get("Innings") != null) {
			List<?> innings = ((List<?>) tempRecord.get("Innings"));

			if (!innings.isEmpty()) {
				Map<?, ?> currInnings = (Map<?, ?>) innings.get(innings.size() - 1);
				tNotification.setTeamName((String) currInnings.get("Team_Name"));
				tNotification.setOvers((String) currInnings.get(IngestionConstants.OVERS_FIELD));
				currentTeamRuns = Integer.parseInt((String) currInnings.get("Total"));
				tNotification.setTotalRuns((String) currInnings.get("Total"));
				if (((String) currInnings.get("Team_Name")).equals((String) tempRecord.get("Team_Home_Name"))) {
					tNotification.setOpponentName((String) tempRecord.get("Team_Away_Name"));
				} else if (((String) currInnings.get("Team_Name")).equals((String) tempRecord.get("Team_Away_Name"))) {
					tNotification.setOpponentName((String) tempRecord.get("Team_Home_Name"));
				}
			}
		}

		if (responseMap != null && responseMap.get("Innings") != null) {
			int previous_team_runs = 0;
			List<?> innings = ((List<?>) responseMap.get("Innings"));

			if (!innings.isEmpty()) {
				Map<?, ?> prevInnings = (Map<?, ?>) innings.get(innings.size() - 1);
				previous_team_runs = Integer.parseInt((String) prevInnings.get("Total"));
				int current_team_runs_factor = currentTeamRuns / 50;
				int previous_team_runs_factor = previous_team_runs / 50;
				if (current_team_runs_factor > previous_team_runs_factor) {
					tNotification.setTotalWickets(((String) prevInnings.get("Wickets")));
					tNotification
							.setSeriesName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
									.get(IngestionConstants.SERIES_NAME_FIELD));
					tNotification
							.setTourName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
									.get(IngestionConstants.TOUR_NAME_FIELD));
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Check if a wicket has fallen
	 * 
	 * @param response
	 *            elasticsearch response containing previous scorecard response.
	 * @param tempRecord
	 *            current scorecard
	 * @param wNotification
	 *            wicket notification
	 * @return true if a wicket has fallen
	 */
	@SuppressWarnings("unchecked")
	private boolean isWicketFell(GetResponse response, Map<String, Object> tempRecord,
			WicketNotification wNotification) {

		Map<String, Object> responseMap = response.getSource();
		int current_wickets = 0;
		int previous_wickets = 0;

		if (responseMap != null && responseMap.get("Innings") != null) {
			List<Map<?, ?>> innings = (List<Map<?, ?>>) responseMap.get("Innings");
			if (!innings.isEmpty()) {
				previous_wickets = Integer.parseInt((String) innings.get(innings.size() - 1).get("Wickets"));
			}
		}

		if (tempRecord != null && tempRecord.get("Innings") != null) {
			List<Map<?, ?>> innings = ((List<Map<?, ?>>) tempRecord.get("Innings"));
			if (!innings.isEmpty()) {
				current_wickets = Integer.parseInt((String) innings.get(innings.size() - 1).get("Wickets"));
			}
		}

		List<Map<?, ?>> innings = ((List<Map<?, ?>>) tempRecord.get("Innings"));
		if (innings != null && !innings.isEmpty()) {

			Map<?, ?> currInnings = innings.get(innings.size() - 1);

			wNotification.setTotalWickets((String) currInnings.get("Wickets"));
			wNotification.setTotalRuns((String) currInnings.get("Total"));
			wNotification.setOvers((String) currInnings.get(IngestionConstants.OVERS_FIELD));

			List<Map<?, ?>> fallOfWickets = (List<Map<?, ?>>) currInnings.get("FallofWickets");
			String playerDismissedName = (String) fallOfWickets.get(fallOfWickets.size() - 1).get("Batsman_Name");
			wNotification.setPlayerName(playerDismissedName);

			for (Map<?, ?> batsman : ((List<Map<?, ?>>) currInnings.get(IngestionConstants.BATSMEN))) {
				if (batsman.get("Batsman_Name").toString().equals(playerDismissedName)) {
					wNotification.setRuns(batsman.get("Runs").toString());
					break;
				}
			}
			wNotification.setTeamName((String) currInnings.get("Team_Name"));
			wNotification.setSeriesName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
					.get(IngestionConstants.SERIES_NAME_FIELD));

			wNotification.setTourName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
					.get(IngestionConstants.TOUR_NAME_FIELD));
		}

		if (current_wickets > previous_wickets) {
			return true;
		} else
			return false;
	}

	@SuppressWarnings("unchecked")
	private boolean isWicketFell2(GetResponse response, Map<String, Object> tempRecord,
			WicketNotification wNotification, FellWicketsDetails fellWicketDetails) {
		LOG.info("####isWicketFell2 ");
		Map<String, Object> responseMap = response.getSource();
		int current_wickets = 0;
		int previous_wickets = 0;

		// Akshay Start
		String matchId = null;
		// Akshay End

		if (responseMap != null) {
			
			if (responseMap.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
				Map<?, ?> matchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
				matchId = (String) matchDetails.get("Id");
				fellWicketDetails.setIsTest("Test".equalsIgnoreCase((String) matchDetails.get("Type")));
				LOG.info("####matchId### " + matchId);
			}
			
			if(responseMap.get("Innings") != null) {
				List<Map<?, ?>> innings = (List<Map<?, ?>>) responseMap.get("Innings");
				if (!innings.isEmpty()) {
					previous_wickets = Integer.parseInt((String) innings.get(innings.size() - 1).get("Wickets"));
				}
			}
		}

		if (tempRecord != null && tempRecord.get("Innings") != null) {
			List<Map<?, ?>> innings = ((List<Map<?, ?>>) tempRecord.get("Innings"));
			if (!innings.isEmpty()) {
				current_wickets = Integer.parseInt((String) innings.get(innings.size() - 1).get("Wickets"));
			}
		}

		List<Map<?, ?>> innings = ((List<Map<?, ?>>) tempRecord.get("Innings"));
		fellWicketDetails.setInning(innings.size());
		
		if (innings != null && !innings.isEmpty()) {

			Map<?, ?> currInnings = innings.get(innings.size() - 1);

			wNotification.setTotalWickets((String) currInnings.get("Wickets"));
			wNotification.setTotalRuns((String) currInnings.get("Total"));
			wNotification.setOvers((String) currInnings.get(IngestionConstants.OVERS_FIELD));

			List<Map<?, ?>> fallOfWickets = (List<Map<?, ?>>) currInnings.get("FallofWickets");
			String playerDismissedName = (String) fallOfWickets.get(fallOfWickets.size() - 1).get("Batsman_Name");
			wNotification.setPlayerName(playerDismissedName);

			for (Map<?, ?> batsman : ((List<Map<?, ?>>) currInnings.get(IngestionConstants.BATSMEN))) {
				if (batsman.get("Batsman_Name").toString().equals(playerDismissedName)) {
					wNotification.setRuns(batsman.get("Runs").toString());
					break;
				}
			}

			wNotification.setTeamName((String) currInnings.get("Team_Name"));
			wNotification.setSeriesName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
					.get(IngestionConstants.SERIES_NAME_FIELD));

			wNotification.setTourName((String) ((Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
					.get(IngestionConstants.TOUR_NAME_FIELD));
		}
		// Akshay Start

		if (innings != null && !innings.isEmpty() && matchId != null) {
			try {
				Map<?, ?> currInnings = innings.get(innings.size() - 1);
				List<Map<?, Object>> fallOfWickets = (List<Map<?, Object>>) currInnings.get("FallofWickets");
				String playerDismissedName = (String) fallOfWickets.get(fallOfWickets.size() - 1).get("Batsman_Name");
				String playerDismissedId = (String) fallOfWickets.get(fallOfWickets.size() - 1).get("Batsman");
				LOG.info("####playerDismissedId### " + playerDismissedId);
				int scoredRunsByPlayerDismissed = Integer.parseInt(wNotification.getRuns());
				LOG.info("####playerDismissedId### " + scoredRunsByPlayerDismissed);
				fellWicketDetails.setMatchId(matchId);
				fellWicketDetails.setPlayerId(playerDismissedId);
				fellWicketDetails.setScoredRunsByPlayer(scoredRunsByPlayerDismissed);
				fellWicketDetails.setPlayerName(playerDismissedName);
				LOG.info(fellWicketDetails);
			} catch (Exception e) {
				LOG.error("Error occured in parsing fall of wickets:", e);
			}
		}

		// Akshay End

		if (current_wickets > previous_wickets) {
			return true;
		} else
			return false;
	}

	/**
	 * Check if the match is on Innings Break.
	 * 
	 * @param response
	 *            elasticsearch response containing previous scorecard
	 * @param temp_record
	 *            current scorecard
	 * @param inbNotification
	 *            innings break notification
	 * @return true if match has entered innings break.
	 */
	private boolean isInningsBreak(GetResponse response, Map<String, Object> temp_record,
			InningsBreakNotification inbNotification) {

		Map<String, Object> responseMap = response.getSource();
		Map<?, ?> prevMatchDetails = null;
		Map<?, ?> currMatchDetails = null;

		if (responseMap != null && temp_record != null) {
			prevMatchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
			currMatchDetails = (Map<?, ?>) temp_record.get(IngestionConstants.MATCHDETAIL_FIELD);

			/*
			 * Checks: 1. previous and current match details are not null. 2. previous
			 * status is not innings break but currently, it is innings break. 3. if 1 and 2
			 * are true, we have an innings break.
			 */
			if ((prevMatchDetails != null && currMatchDetails != null)
					&& (!"Innings Break".equals(prevMatchDetails.get("Status"))
							&& "Innings Break".equals(currMatchDetails.get("Status")))) {

				List<?> innings = (List<?>) temp_record.get("Innings");
				Map<?, ?> currInnings = (Map<?, ?>) innings.get(innings.size() - 1);

				inbNotification.setTeamName((String) currInnings.get("Team_Name"));
				inbNotification.setSeriesName((String) currMatchDetails.get(IngestionConstants.SERIES_NAME_FIELD));
				inbNotification.setTotalWickets((String) currInnings.get("Wickets"));
				inbNotification.setTotalRuns((String) currInnings.get("Total"));
				inbNotification.setOvers((String) currInnings.get(IngestionConstants.OVERS_FIELD));
				inbNotification.setTeamHome((String) temp_record.get("Team_Home_Name"));
				inbNotification.setTeamAway((String) temp_record.get("Team_Away_Name"));
				inbNotification.setTourName((String) currMatchDetails.get(IngestionConstants.TOUR_NAME_FIELD));

				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean isInningsBreak2(GetResponse response, Map<String, Object> tempRecord,
			TeamInningEndDetails teamInningEndDetails) {
		LOG.info("####isInningsBreak2 ");
		Map<String, Object> responseMap = response.getSource();

		// Akshay Start
		String matchId = null;
		if (responseMap != null && responseMap.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
			Map<?, ?> matchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
			matchId = (String) matchDetails.get("Id");
			if ("Test".equalsIgnoreCase((String) matchDetails.get("Type"))) {
				teamInningEndDetails.setIsTest(true);
				teamInningEndDetails.setInning(((List<?>) responseMap.get(IngestionConstants.INNINGS_FIELD)).size());
			}
		}

		// Akshay End

		Map<?, ?> prevMatchDetails = null;
		Map<?, ?> currMatchDetails = null;

		if (responseMap != null && tempRecord != null) {
			prevMatchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
			currMatchDetails = (Map<?, ?>) tempRecord.get(IngestionConstants.MATCHDETAIL_FIELD);

			if (prevMatchDetails != null && currMatchDetails != null
					&& !"Innings Break".equals(prevMatchDetails.get("Status"))
					&& "Innings Break".equals(currMatchDetails.get("Status"))) {

				List<?> innings = (List<?>) tempRecord.get("Innings");
				Map<?, Object> currInnings = (Map<?, Object>) innings.get(innings.size() - 1);
				teamInningEndDetails.setMatchId(matchId);
				teamInningEndDetails.setTeamId((String) currInnings.get("Battingteam"));
				teamInningEndDetails.setScoredRunsByTeam(Integer.valueOf((String) currInnings.get("Total")));
				teamInningEndDetails.setTeamName((String) currInnings.get("Team_Name"));

				return true;
			}
		}
		return false;
	}

	/**
	 * Check if toss has occurred.
	 * 
	 * @param response
	 *            elasticsearch response containing previous scorecard.
	 * @param temp_record
	 *            current scorecard
	 * @param notification
	 *            Toss notification
	 * @return true if toss has occured.
	 */
	private boolean isToss(GetResponse response, Map<String, Object> temp_record, TossNotification notification) {
		Map<String, Object> responseMap = response.getSource();
		Map<?, ?> currMatchDetails = null;

		if (temp_record != null && temp_record.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
			currMatchDetails = (Map<?, ?>) temp_record.get(IngestionConstants.MATCHDETAIL_FIELD);
		}

		if ((responseMap == null || !responseMap.isEmpty()
				|| !((Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD)).containsKey("Prematch"))
				&& currMatchDetails != null && StringUtils.isNotBlank((String) currMatchDetails.get("Prematch"))) {

			String preMatch = (String) currMatchDetails.get("Prematch");
			if (!preMatch.contains("Toss")) {
				return false;
			}
			String[] tossElection = ((String) currMatchDetails.get("Prematch")).split(",");

			notification.setTossWonByName(tossElection[0].split(":")[1].trim());
			notification.setTossElectedTo(tossElection[1].split(":")[1].trim());

			String teamHome = (String) temp_record.get("Team_Home_Name");
			String teamAway = (String) temp_record.get("Team_Away_Name");

			if (notification.getTossWonByName().equals(teamHome)) {
				notification.setTeamName(teamAway);
			} else {
				notification.setTeamName(teamHome);
			}

			notification.setSeriesName((String) currMatchDetails.get(IngestionConstants.SERIES_NAME_FIELD));
			notification.setTourName((String) currMatchDetails.get(IngestionConstants.TOUR_NAME_FIELD));

			return true;
		}
		return false;
	}

	private boolean isToss2(GetResponse response, Map<String, Object> temp_record, TossNotification notification,
			TossWonDetails tossWonDetails) {
		LOG.info("####isToss2 ");
		Map<String, Object> responseMap = response.getSource();
		Map<?, ?> currMatchDetails = null;

		// Akshay Start
		String tossWonByTeamName = null;
		String matchId = null;
		if (responseMap != null && responseMap.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
			Map<?, ?> matchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
			matchId = (String) matchDetails.get("Id");
			tossWonDetails.setMatchId(matchId);
			tossWonByTeamName = (String) matchDetails.get("Tosswonby");
			tossWonDetails.setTeamName(tossWonByTeamName);

		}
		// String tossWonByTeamName = (String) matchDetails.get("Tosswonby");
		// String tossWonByTeamName = "";
		// tossWonDetails.setTeamName(tossWonByTeamName);

		// Akshay End

		if (temp_record != null && temp_record.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
			currMatchDetails = (Map<?, ?>) temp_record.get(IngestionConstants.MATCHDETAIL_FIELD);
		}

		if ((responseMap == null || !responseMap.isEmpty()
				|| !((Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD)).containsKey("Prematch"))
				&& currMatchDetails != null && StringUtils.isNotBlank((String) currMatchDetails.get("Prematch"))) {

			String preMatch = (String) currMatchDetails.get("Prematch");
			if (!preMatch.contains("Toss")) {
				return false;
			}

			String[] tossElection = ((String) currMatchDetails.get("Prematch")).split(",");

			notification.setTossWonByName(tossElection[0].split(":")[1].trim());
			tossWonByTeamName = notification.getTossWonByName();
			notification.setTossElectedTo(tossElection[1].split(":")[1].trim());

			String teamHome = (String) temp_record.get("Team_Home_Name");
			String teamAway = (String) temp_record.get("Team_Away_Name");

			// Akshay Start
			tossWonDetails.setElectedToByTossWonTeam(tossElection[1].split(":")[1].trim());
			if (teamHome.equals(tossWonByTeamName)) {
				tossWonDetails.setTeamId((String) temp_record.get("Team_Home"));

			}
			if (teamAway.equals(tossWonByTeamName)) {
				tossWonDetails.setTeamId((String) temp_record.get("Team_Away"));
			}

			// Aksahy End

			tossWonDetails.setMatchId((String) currMatchDetails.get("Id"));

			if (notification.getTossWonByName().equals(teamHome)) {
				notification.setTeamName(teamAway);
			} else {
				notification.setTeamName(teamHome);
			}

			notification.setSeriesName((String) currMatchDetails.get(IngestionConstants.SERIES_NAME_FIELD));
			notification.setTourName((String) currMatchDetails.get(IngestionConstants.TOUR_NAME_FIELD));

			return true;
		}
		return false;
	}

	/**
	 * Check if match has ended. If match has ended, set the winner of the match.
	 * 
	 * @param response
	 *            elasticsearch response containing previous scorecard
	 * @param currRecord
	 *            current scorecard
	 * @param notification
	 *            Overs notification
	 * @return true if 15/30/45 overs have elapsed.
	 */
	private boolean hasWon(GetResponse response, Map<String, Object> currRecord, MatchWinNotification notification) {
		Map<String, Object> responseMap = response.getSource();

		if (responseMap != null && currRecord != null
				&& StringUtils.isBlank((String) responseMap.get(IngestionConstants.RESULT_FIELD))
				&& StringUtils.isNotBlank((String) currRecord.get(IngestionConstants.RESULT_FIELD))) {

			String result = (String) currRecord.get(IngestionConstants.RESULT_FIELD);
			Map<?, ?> matchDetail = (Map<?, ?>) currRecord.get(IngestionConstants.MATCHDETAIL_FIELD);

			if (result.toLowerCase().contains("draw") || result.toLowerCase().contains("drew")) {
				notification.setIsDraw(true);
				notification.setMatchNumber(
						((String) matchDetail.get(IngestionConstants.MATCH_DETAILS_NUMBER)).split(" ")[0]);
			} else {
				notification.setWinningTeam((String) currRecord.get(IngestionConstants.WINNING_TEAM_NAME));
				notification.setWinMargin((String) currRecord.get(IngestionConstants.WINMARGIN));
			}

			notification.setTeamHome((String) currRecord.get(IngestionConstants.TEAM_HOME_NAME));
			notification.setTeamAway((String) currRecord.get(IngestionConstants.TEAM_AWAY_NAME));

			notification.setSeriesName((String) matchDetail.get(IngestionConstants.SERIES_NAME_FIELD));
			notification.setTourName((String) matchDetail.get(IngestionConstants.TOUR_NAME_FIELD));

			return true;
		}
		return false;
	}

	private boolean hasWon2(GetResponse response, Map<String, Object> currRecord,
			TeamInningEndDetails secondInningEndDetails, MatchWonTeamDetails matchWonTeamDetails) {
		LOG.info("####hasWon2 ");
		Map<String, Object> responseMap = response.getSource();

		// Akshay Start
		if (responseMap != null && responseMap.get(IngestionConstants.MATCHDETAIL_FIELD) != null) {
			Map<?, ?> matchDetails = (Map<?, ?>) responseMap.get(IngestionConstants.MATCHDETAIL_FIELD);
			String matchId = (String) matchDetails.get("Id");
			secondInningEndDetails.setMatchId(matchId);
			matchWonTeamDetails.setMatchId(matchId);
			if ("Test".equalsIgnoreCase((String) matchDetails.get("Type"))) {
				secondInningEndDetails.setIsTest(true);
				secondInningEndDetails.setInning(((List<?>) responseMap.get(IngestionConstants.INNINGS_FIELD)).size());
			}
		}
		// Akshay End

		if (responseMap != null && currRecord != null
				&& StringUtils.isBlank((String) responseMap.get(IngestionConstants.RESULT_FIELD))
				&& StringUtils.isNotBlank((String) currRecord.get(IngestionConstants.RESULT_FIELD))) {

			String result = (String) currRecord.get(IngestionConstants.RESULT_FIELD);

			if (result.toLowerCase().contains("draw") || result.toLowerCase().contains("drew")) {

				List<?> innings = (List<?>) currRecord.get(IngestionConstants.INNINGS_FIELD);

				Map<?, ?> inning = (Map<?, ?>) innings.get(innings.size() - 1);

				int total = Integer.parseInt((String) inning.get("Total"));

				secondInningEndDetails.setScoredRunsByTeam(total);
				secondInningEndDetails.setTeamId((String) inning.get("Battingteam"));
				secondInningEndDetails.setTeamName("");

				matchWonTeamDetails.setIsDraw(true);
				matchWonTeamDetails.setTeamId("");
				matchWonTeamDetails.setTeamName("");
				matchWonTeamDetails.setWinMargin("match draw");
			} else {

				List<?> innings = (List<?>) currRecord.get(IngestionConstants.INNINGS_FIELD);

				Map<?, ?> inning = (Map<?, ?>) innings.get(innings.size() - 1);

				int total = Integer.parseInt((String) inning.get("Total"));

				secondInningEndDetails.setScoredRunsByTeam(total);
				secondInningEndDetails.setTeamId((String) inning.get("Battingteam"));

				matchWonTeamDetails.setIsDraw(false);
				matchWonTeamDetails.setTeamId((String) currRecord.get("Winningteam"));
				matchWonTeamDetails.setTeamName((String) currRecord.get(IngestionConstants.WINNING_TEAM_NAME));
				matchWonTeamDetails.setWinMargin((String) currRecord.get(IngestionConstants.WINMARGIN));
			}

			return true;
		}
		return false;
	}

	/**
	 * Check if 15/30/45 overs have elapsed.
	 * 
	 * @param response
	 *            elasticsearch response containing previous scorecard
	 * @param currRecord
	 *            current scorecard
	 * @param notification
	 *            Overs notification
	 * @return true if 15/30/45 overs have elapsed.
	 */
	private boolean hasOversElapsed(GetResponse response, Map<String, Object> currRecord,
			OversNotification notification) {

		Map<String, Object> responseMap = response.getSource();

		String prevOvers = null;
		String currOvers = null;

		if (responseMap != null && currRecord != null) {
			List<?> prevInnings = (List<?>) responseMap.get(IngestionConstants.INNINGS_FIELD);
			if (prevInnings != null && !prevInnings.isEmpty()) {
				prevOvers = (String) ((Map<?, ?>) prevInnings.get(prevInnings.size() - 1))
						.get(IngestionConstants.OVERS_FIELD);
				/*
				 * Get the integral part of an over. Example: For Overs: 49.5, prevOvers will
				 * have value as 49
				 */
				prevOvers = prevOvers.substring(0, prevOvers.length() - 2);
			}

			List<?> currInnings = null;
			if (currRecord != null) {
				currInnings = (List<?>) currRecord.get(IngestionConstants.INNINGS_FIELD);
				if (currInnings != null && !currInnings.isEmpty()) {
					currOvers = (String) ((Map<?, ?>) currInnings.get(currInnings.size() - 1))
							.get(IngestionConstants.OVERS_FIELD);
					currOvers = currOvers.substring(0, currOvers.length() - 2);
				}
			}

			if (StringUtils.isNotBlank(prevOvers) && StringUtils.isNotBlank(currOvers)) {

				if ((prevOvers.equals("14") && currOvers.equals("15"))
						|| (prevOvers.equals("29") && currOvers.equals("30"))
						|| (prevOvers.equals("44") && currOvers.equals("45"))) {
					Map<?, ?> currInningsData = (Map<?, ?>) currInnings.get(currInnings.size() - 1);

					notification.setTeamName((String) currInningsData.get(IngestionConstants.TEAM_NAME_FIELD));
					notification
							.setSeriesName((String) ((Map<?, ?>) currRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
									.get(IngestionConstants.SERIES_NAME_FIELD));
					notification.setTotalWickets((String) currInningsData.get(IngestionConstants.WICKETS_FIELD));
					notification.setTotalRuns((String) currInningsData.get(IngestionConstants.TOTAL_RUNS_FIELD));
					notification.setOvers((String) currInningsData.get(IngestionConstants.OVERS_FIELD));
					notification.setTeamHome((String) currRecord.get("Team_Home_Name"));
					notification.setTeamAway((String) currRecord.get("Team_Away_Name"));
					notification.setTourName((String) ((Map<?, ?>) currRecord.get(IngestionConstants.MATCHDETAIL_FIELD))
							.get(IngestionConstants.TOUR_NAME_FIELD));
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Insert or delete notification details in match schedule.
	 * 
	 * Also, if the notification data is already in use, clear it to stop any more
	 * notifications from being processed.
	 * 
	 * @param jsonData
	 *            Update request in JSON format
	 * 
	 * @throws IOException
	 *             if an error occurs in parsing json
	 */
	public void updateMatchDetails(String jsonData) throws IOException {

		if (StringUtils.isBlank(jsonData)) {
			LOG.error("Received Invalid update request: " + jsonData);
			throw new DBAnalyticsException("Received an invalid update request: " + jsonData);
		}

		Map<String, Object> updateRequest = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
		});

		String matchId = (String) updateRequest.remove(CricketConstants.QUERY_MATCH_ID);

		if (StringUtils.isBlank(matchId)) {
			LOG.error("Received Invalid update request: " + jsonData);
			throw new DBAnalyticsException("Received an invalid update request: " + jsonData);
		}

		updateRequest.put(Constants.ROWID, matchId);
		updateRequest.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

		if (notificationDataMap.containsKey(matchId)
				&& updateRequest.containsKey(CricketConstants.NOTIFICATION_STATUS_FIELD)
				&& updateRequest.get(CricketConstants.NOTIFICATION_STATUS_FIELD) != null) {

			if (Boolean.parseBoolean(updateRequest.get(CricketConstants.NOTIFICATION_STATUS_FIELD).toString())) {
				notificationDataMap.put(matchId, updateRequest.get(CricketConstants.NOTIFICATION_DETAILS_FIELD));
			} else {
				notificationDataMap.put(matchId, null);
			}
		}

		LOG.info("Updating match details for Match Id " + matchId + ": " + updateRequest.toString());

		elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME,
				ImmutableList.of(updateRequest));
	}

	/**
	 * Method to update widget control flags for cricket.
	 */
	public void updateMatchFlags(String jsonData) throws IOException {
		if (StringUtils.isBlank(jsonData)) {
			LOG.error("Received Invalid update request: " + jsonData);
			throw new DBAnalyticsException("Received an invalid update request: " + jsonData);
		}

		Map<String, Object> updateRequest = mapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
		});

		String matchId = (String) updateRequest.remove(CricketConstants.QUERY_MATCH_ID);

		if (StringUtils.isBlank(matchId)) {
			LOG.error("Received Invalid update request: " + jsonData);
			throw new DBAnalyticsException("Received an invalid update request: " + jsonData);
		}

		updateRequest.put(Constants.ROWID, matchId);
		updateRequest.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

		/*
		 * Check for update to notification details for sending notifications for
		 * cricket.
		 */
		if (notificationDataMap.containsKey(matchId)
				&& updateRequest.containsKey(CricketConstants.NOTIFICATION_STATUS_FIELD)
				&& updateRequest.get(CricketConstants.NOTIFICATION_STATUS_FIELD) != null) {

			if (Boolean.parseBoolean(updateRequest.get(CricketConstants.NOTIFICATION_STATUS_FIELD).toString())) {
				notificationDataMap.put(matchId, updateRequest.get(CricketConstants.NOTIFICATION_DETAILS_FIELD));
			} else {
				notificationDataMap.put(matchId, null);
			}
		}

		LOG.info("Updating match details for Match Id " + matchId + ": " + updateRequest.toString());

		elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME,
				ImmutableList.of(updateRequest));
	}

	/**
	 * Get the notification data for the given match id.
	 * 
	 * @param matchId
	 *            id of the match
	 * 
	 * @return map of notification details.
	 */
	private Map<?, ?> getNotificationData(String matchId) {

		Map<?, ?> notificationData = null;

		if (notificationDataMap.containsKey(matchId)) {
			notificationData = (Map<?, ?>) notificationDataMap.get(matchId);
		} else {
			final GetResponse scheduleResponse = client
					.prepareGet(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME, matchId).execute().actionGet();
			Map<String, Object> responseMap = scheduleResponse.getSource();

			if (responseMap != null && responseMap.get(CricketConstants.NOTIFICATION_STATUS_FIELD) != null) {
				if (Boolean.parseBoolean(responseMap.get(CricketConstants.NOTIFICATION_STATUS_FIELD).toString())) {
					notificationData = (Map<?, ?>) responseMap.get(CricketConstants.NOTIFICATION_DETAILS_FIELD);
					notificationDataMap.put(matchId, notificationData);
				}
			} else {
				notificationDataMap.put(matchId, null);
			}
		}
		return notificationData;
	}

	/**
	 * Utility method for use in restarting matches.
	 * 
	 * @param directory
	 *            to to replay data from.
	 * @param interval
	 *            time difference between two directories.
	 */
	public void reingestDataFromSystem(String directory, long interval) {

		File theDir = new File(directory);
		File[] dirs = theDir.listFiles();
		List<File> dirsList = Arrays.asList(dirs);
		Collections.sort(dirsList, (File o1, File o2) -> o1.getName().compareTo(o2.getName()));
		long startTime = System.currentTimeMillis();

		dirs = dirsList.toArray(new File[dirsList.size()]);

		for (int i = 0; i < dirs.length; i++) {

			if (dirs[i].isDirectory()) {
				LOG.info("Processing directory: " + dirs[i].getName());
				File[] dataFiles = dirs[i].listFiles();
				for (int j = 0; j < dataFiles.length; j++) {
					try {
						String fileName = dataFiles[j].getName();
						LOG.info("Processing file: " + dataFiles[j].getName());
						String indexName = "";

						String data = FileUtils.readFileToString(dataFiles[j]);
						Map<String, Object> record = mapper.readValue(data, new TypeReference<Map<String, Object>>() {
						});

						if (fileName.contains("calendar")) {
							indexName = Indexes.CRICKET_SCHEDULE;
						} else if (fileName.contains("commentary")) {
							indexName = Indexes.CRICKET_COMMENTARY;
						} else {
							indexName = Indexes.CRICKET_SCORECARD;
						}
						ingestData(indexName, fileName, record);
					} catch (Exception e) {
						LOG.error("Some error occured" + e, e);
					}
				}
				try {
					LOG.info("------------------------------------------------");
					LOG.info("Waiting " + interval / 1000 + " seconds to process next directory...");
					Thread.sleep(interval);
					LOG.info("Waiting time completed. Now system will pick up next directory...");
					LOG.info("------------------------------------------------");
				} catch (InterruptedException e) {
					LOG.error("Thread Received interrupt. \nAssuming shutdown Request.\nReturning ...");
					return;
				} catch (Exception e) {
					LOG.error(e);
				}
			}
		}
		LOG.info("Total Execution time in match(Minutes): " + (System.currentTimeMillis() - startTime) / 1000.0 * 60.0);
	}

	public static void shutdown() {

		if (matchSchedulerService != null) {
			LOG.info("Shutting Down Match Scheduler Service.");
			matchSchedulerService.shutdownNow();
		}
	}

	@SuppressWarnings("unchecked")
	public void pushCricketTeamStanding(Map<String, Object> record) {
		try {
			List<Map<String, Object>> iplTeamStanding = new ArrayList<>();
			Map<String, Map<String, Object>> cricketTeamsNames = CricketGlobalVariableUtils.getCricketTeamsNames();

			Map<String, Object> seriesObj = ((Map<String, Map<String, Map<String, Object>>>) record.get("standings"))
					.get("sport").get("series");
			Map<String, Object> stagesObj = ((Map<String, Map<String, List<Map<String, Object>>>>) record
					.get("standings")).get("stages").get("stage").get(0);
			List<Map<String, Object>> entityList = ((List<Map<String, Map<String, List<Map<String, Object>>>>>) stagesObj
					.get("group")).get(0).get("entities").get("entity");

			// Series Data
			String seriesId = seriesObj.get(Constants.ID).toString();
			String seriesName = seriesObj.get(Constants.NAME).toString();
			String seriesShortName = seriesObj.get(Constants.SHORT_NAME).toString();

			// Stage Data
			String stage = stagesObj.get(Constants.NAME).toString();

			for (Map<String, Object> entityMap : entityList) {
				Map<String, Object> map = ((Map<String, Object>) entityMap
						.get(Constants.Cricket.TeamStanding.SPORT_SPECIFIC_KEYS));
				String teamId = entityMap.get(Constants.ID).toString();

				map.put(Constants.Cricket.Team.ID, teamId);
				map.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				map.put(Constants.Cricket.Team.NAME, entityMap.get(Constants.Cricket.Team.NAME));
				map.put(Constants.Cricket.Team.SHORT_NAME, entityMap.get(Constants.Cricket.Team.SHORT_NAME));
				map.put(Constants.Cricket.TeamStanding.POSITION,
						entityMap.get(Constants.Cricket.TeamStanding.POSITION));
				map.put(Constants.ROWID, seriesId + "_" + teamId);

				// Put custom Names
				if (cricketTeamsNames.containsKey(teamId)) {
					Map<String, Object> cricketTeamNames = cricketTeamsNames.get(teamId);
					map.put(Constants.Cricket.Team.CUSTOM_NAME,
							cricketTeamNames.get(Constants.Cricket.Team.CUSTOM_NAME));
					map.put(Constants.Cricket.Team.CUSTOM_SHORT_NAME,
							cricketTeamNames.get(Constants.Cricket.Team.CUSTOM_SHORT_NAME));
				} else {
					map.put(Constants.Cricket.Team.CUSTOM_NAME, entityMap.get(Constants.Cricket.Team.NAME));
					map.put(Constants.Cricket.Team.CUSTOM_SHORT_NAME, entityMap.get(Constants.Cricket.Team.SHORT_NAME));
				}

				// Put Series Data
				map.put(Constants.Cricket.Series.ID, seriesId);
				map.put(Constants.Cricket.Series.NAME, seriesName);
				map.put(Constants.Cricket.Series.SHORT_NAME, seriesShortName);

				// Put Stage Data
				map.put(Constants.Cricket.STAGE, stage);

				iplTeamStanding.add(map);
			}
			elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_TEAM_STANDING, MappingTypes.MAPPING_REALTIME,
					iplTeamStanding);

			LOG.info("Successfully pushed Cricket Team Standing Data");
		} catch (Exception e) {
			LOG.error("Error while pushing Cricket Team Standing Data", e);
			throw new DBAnalyticsException(e);
		}
	}

	private void prepareNotificationTarget(Map<String, Object> source, Map<String, Set<String>> notificationTargetMap) {
		if (source.containsKey("vendorId")) {
			try {
				String vendorId = source.get("vendorId").toString();
				Set<String> usersSet = null;

				if (notificationTargetMap.containsKey(vendorId)) {
					usersSet = notificationTargetMap.get(vendorId);
				} else {
					usersSet = new HashSet<>();
					notificationTargetMap.put(vendorId, usersSet);
				}

				usersSet.add(source.get(PredictWinConstants.USER_ID).toString());
			} catch (Exception e) {
				LOG.error("Error while preparing notification object", e);
			}
		}
	}

	private void preparePwUsersRankingIncObj(String matchId, String userId,
			Map<String, Map<String, Integer>> pwUsersRankingIncObj,
			Map<String, Map<String, Object>> pwUsersRankingUpdateObj, int coinsGained) {

		try {
			String id = matchId + "_" + userId;
			Map<String, Integer> usersGainedCoinMap = null;
			Map<String, Object> usersUpdateDocMap = null;
			int totalCoinsGained = 0;

			if (pwUsersRankingIncObj.containsKey(id)) {
				usersGainedCoinMap = pwUsersRankingIncObj.get(id);
				totalCoinsGained = usersGainedCoinMap.get(Cricket.PredictWinConstants.COINS_GAINED);
			} else {
				usersGainedCoinMap = new HashMap<>();
				pwUsersRankingIncObj.put(id, usersGainedCoinMap);
			}

			if (!pwUsersRankingUpdateObj.containsKey(id)) {
				usersUpdateDocMap = new HashMap<>();

				usersUpdateDocMap.put(PredictWinConstants.MATCH_ID, matchId);
				usersUpdateDocMap.put(PredictWinConstants.USER_ID, userId);
				usersUpdateDocMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

				pwUsersRankingUpdateObj.put(id, usersUpdateDocMap);
			}

			totalCoinsGained += coinsGained;
			usersGainedCoinMap.put(Cricket.PredictWinConstants.COINS_GAINED, totalCoinsGained);
		} catch (Exception e) {
			LOG.error("Error while preparing pw user rankin inc Object", e);
		}
	}

	@SuppressWarnings("unchecked")
	public void updateProbableNextMatchTeamSquads(String matchId, Map<String, Object> tempRecord) {
		try {
			List<Map<String, Object>> teamsSquadsList = new ArrayList<>();

			if (tempRecord.containsKey(IngestionConstants.TEAMS)) {
				Map teamsSquads = mapper.readValue((String) tempRecord.get(IngestionConstants.TEAMS), Map.class);
				Set<String> teams = teamsSquads.keySet();

				for (String teamId : teams) {
					Map<String, Object> teamSqaudsMap = new HashMap<>();
					Map<String, Object> teamSquads = (Map<String, Object>) teamsSquads.get(teamId);

					teamSqaudsMap.put("team", mapper.writeValueAsString(teamSquads));
					teamSqaudsMap.put("id", teamId);
					teamSqaudsMap.put(Constants.ROWID, teamId);
					teamSqaudsMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

					teamsSquadsList.add(teamSqaudsMap);
				}
			}

			elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_PROBABLE_TEAM_SQUADS, MappingTypes.MAPPING_REALTIME,
					teamsSquadsList);
			LOG.info("Successfully updated probable Team Squad for match " + matchId);

		} catch (Exception e) {
			LOG.error("Error while updating probable Team Squad for match " + matchId, e);
		}

	}

	private void holdExecution(long millis) {
		LOG.info("Sleeping for " + (millis / 1000) + " sec before executing update...");
		try {
			Thread.sleep(millis);
		} catch (InterruptedException ex) {
			LOG.error("Received interrupt during execution pause: ", ex);
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized void pushCricketTeamSquads(Map<String, Object> record) {
		try {
			List<Map<String, Object>> squadList = new ArrayList<>();

			Map<?, ?> squads = (Map<?, ?>) record.get("squads");

			Map<?, ?> sport = (Map<?, ?>) squads.get("sport");

			Map<?, ?> seriesObject = null;
			Map<String, Object> seriesData = new HashMap<>();

			/*
			 * Prepare a separate map and use it to copy values to all team maps.
			 */
			if (sport.containsKey("league")) {
				seriesObject = (Map<?, ?>) sport.get("league");
				seriesData.put("leagueId", (String) seriesObject.get(Constants.ID));
				seriesData.put("leagueName", (String) seriesObject.get(Team.NAME));
				seriesData.put("leagueShortName", (String) seriesObject.get(Team.SHORT_NAME));
			} else {
				seriesObject = (Map<?, ?>) sport.get("series");
				seriesData.put("seriesId", (String) seriesObject.get(Constants.ID));
				seriesData.put("seriesName", (String) seriesObject.get(Team.NAME));
				seriesData.put("seriesShortName", (String) seriesObject.get(Team.SHORT_NAME));
			}

			List<Map<?, ?>> teamList = (List<Map<?, ?>>) ((Map<?, ?>) squads.get("teams")).get("team");

			String seriesId = (String) seriesObject.get(Constants.ID);

			/*
			 * Parse each team in the team list.
			 */
			for (Map<?, ?> team : teamList) {
				String teamId = (String) team.get(Constants.ID);

				/*
				 * Skip to be confirmed teams.
				 */
				Map<?, ?> players = (Map<?, ?>) team.get("players");

				if (players == null || players.isEmpty()) {
					LOG.info("Team: " + teamId + ", Series/League: " + seriesId
							+ ", has empty player list. Probably yet to be confirmed.");
					continue;
				}

				Map<String, Object> squad = new HashMap<>();
				squadList.add(squad);

				squad.put(Constants.ROWID, seriesId + "_" + teamId);
				squad.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

				// Add Teams Object
				squad.put("teamId", teamId);
				squad.put("teamName", (String) team.get(Team.NAME));
				squad.put("teamShortName", (String) team.get(Team.SHORT_NAME));

				squad.putAll(seriesData);

				List<Map<String, ?>> playerList = (List<Map<String, ?>>) players.get("player");

				squad.put(Team.SUPPORT_STAFF, team.get(Team.SUPPORT_STAFF));

				List<Map<String, Object>> squadListParsed = new ArrayList<>();
				
				/*
				 * Prepare player list.
				 */
				for (Map<String, ?> player : playerList) {
					Map<String, Object> playerData = new HashMap<>();

					if (player.get(TeamStanding.SPORT_SPECIFIC_KEYS) != null) {
						Map<String, ?> sportSpecificKeys = (Map<String, ?>) player
								.remove(TeamStanding.SPORT_SPECIFIC_KEYS);

						playerData.putAll(sportSpecificKeys);
					}

					playerData.putAll(player);
					squadListParsed.add(playerData);
				}
				squad.put("players", squadListParsed);
			}

			elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_TEAM_SQUADS, MappingTypes.MAPPING_REALTIME,
					squadList);
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}
	
	public static void main(String[] args) throws Exception{
		String filePath = "/run/user/1000/gvfs/sftp:host=10.140.0.95,user=centos/opt/cric-data/2018-05-26T07:50:39Z/7_squad";
		CricketIngestionService service = new CricketIngestionService();
		
		Map<String, Object> record = new GsonBuilder().create()
				.fromJson(new JsonReader(new FileReader(new File(filePath))), Map.class);
		
		service.pushCricketTeamSquads(record);
	}
}
