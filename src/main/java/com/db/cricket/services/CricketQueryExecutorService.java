package com.db.cricket.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Cricket;
import com.db.common.constants.Constants.Cricket.PredictWinConstants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.CommentaryType;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.cricket.model.BidDetailsSummaryRequest;
import com.db.cricket.model.BidDetailsSummaryResponse;
import com.db.cricket.model.Commentary;
import com.db.cricket.model.CommentaryItem;
import com.db.cricket.model.CricketQuery;
import com.db.cricket.model.Inning;
import com.db.cricket.model.LuckyWinnersResponse;
import com.db.cricket.model.Match;
import com.db.cricket.model.MatchWinnerDetailPostRequest;
import com.db.cricket.model.Matchdetail;
import com.db.cricket.model.PredictAndWinPostResponse;
import com.db.cricket.model.RequestForMatchWinnerPost;
import com.db.cricket.model.Schedule;
import com.db.cricket.model.Scorecard;
import com.db.cricket.model.Squad;
import com.db.cricket.model.TicketIdAndGainPointResponse;
import com.db.cricket.model.TicketIdAndGainPointResponseList;
import com.db.cricket.model.TopWinnersListRequest;
import com.db.cricket.model.TopWinnersListResponse;
import com.db.cricket.model.TopWinnersResponse;
import com.db.cricket.predictnwin.constants.PredictAndWinIndexes;
import com.db.cricket.utils.CricketUtils;
import com.db.cricket.utils.LanguageUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CricketQueryExecutorService {

	private static final Logger LOG = LogManager.getLogger(CricketQueryExecutorService.class);

	private static final int DEFAULT_SIZE = 25;

	ElasticSearchIndexService indexService = ElasticSearchIndexService.getInstance();

	private static final LanguageUtils langUtil = LanguageUtils.getInstance();

	private Gson json = new GsonBuilder().setPrettyPrinting().create();

	private ObjectMapper mapper = new ObjectMapper();

	private Client client = null;

	CricketQueryExecutorService() {
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
		if (this.client != null) {
			client.close();
		}
		client = indexService.getClient();
	}

	/**
	 * Gets the score card for the match_Id specified in the query.
	 * 
	 * @param match_Id
	 *            the query json containing match_Id
	 * 
	 * @return the scorecard for the match_Id provided.
	 */
	public Scorecard getScoreCard(CricketQuery cquery) {

		String matchId = cquery.getMatch_Id();
		String lang = cquery.getLang();
				
		long startTime = System.currentTimeMillis();

		GetResponse response = client.prepareGet().setIndex(Indexes.CRICKET_SCORECARD).setId(matchId).execute()
				.actionGet();

		long endTime = System.currentTimeMillis();

		if (response.isExists()) {
			LOG.debug("Result JSON: " + response.getSourceAsString());
			Scorecard scoreCard = json.fromJson(response.getSourceAsString(), Scorecard.class);
			if (scoreCard.getInnings() == null) {
				scoreCard.setInnings(new ArrayList<Inning>());
			}
			
			Matchdetail matchDetail = scoreCard.getMatchdetail();

			if (StringUtils.isNotBlank((String) response.getSourceAsMap().get(IngestionConstants.TEAMS))){
				
				if(cquery.getProbableTeamsSquads() && StringUtils.isBlank(matchDetail.getTosswonby())){
					scoreCard.setTeams(getProbableTeamsSquad(matchDetail.getId(), scoreCard.getTeamAway(), scoreCard.getTeamHome()));
				}else{
					scoreCard.setTeams(
							json.fromJson((String) response.getSourceAsMap().get(IngestionConstants.TEAMS), Map.class));
				}
				
			}else if(cquery.getProbableTeamsSquads()){
				scoreCard.setTeams(getProbableTeamsSquad(matchDetail.getId(), scoreCard.getTeamAway(), scoreCard.getTeamHome()));
			}

			LOG.debug("ScoreCard Json: " + json.toJson(scoreCard));
			LOG.info(
					"Scorecard returned for match " + matchId + ". Execution Time: " + (endTime - startTime) + " ms.");

			langUtil.scoreCardInLanguage(scoreCard, lang);

			return scoreCard;
		} else {
			LOG.warn("No Scorecard found for Id: " + matchId
					+ "; Returning data from widget details. Execution Time: " + (endTime - startTime) + " ms.");
			return getWidgetDetails(cquery);
		}
	}

	/**
	 * The the match widget details
	 * 
	 * @param query
	 *            the query string containing the match_Id to search for.
	 * 
	 * @return An instance of widget details.
	 */
	public Scorecard getWidgetDetails(CricketQuery cquery) {
		String matchId = cquery.getMatch_Id();
		long startTime = System.currentTimeMillis();

		Scorecard scoreCard = new Scorecard();

		GetResponse response = client.prepareGet().setIndex(Indexes.CRICKET_SCHEDULE).setId(matchId).execute()
				.actionGet();

		if (!response.isExists()) {
			LOG.warn("No Records found for Id: " + matchId);
			return new Scorecard();
		}

		LOG.debug("result json: " + response.getSourceAsString());

		LOG.info("Widget query execution took: " + (System.currentTimeMillis() - startTime) + " ms.");

		List<Inning> innings = new ArrayList<>();

		Map<String, Object> sourceMap = response.getSourceAsMap();
		Matchdetail mDetail = new Matchdetail();
		mDetail.setStatus((String) sourceMap.get("matchstatus"));
		mDetail.setVenue((String) sourceMap.get("venue"));
		mDetail.setEquation((String) sourceMap.get("equation"));
		mDetail.setTime((String) sourceMap.get(IngestionConstants.MATCH_TIME_IST_FIELD));
		mDetail.setTourName((String) sourceMap.get("tourname"));
		mDetail.setDate((String) sourceMap.get(IngestionConstants.MATCH_DATE_IST_FIELD));
		mDetail.setType((String) sourceMap.get("matchtype"));
		mDetail.setTosswonby((String) sourceMap.get("toss_won_by_name"));
		mDetail.setNumber((String) sourceMap.get("matchnumber"));
		mDetail.setSeriesName((String) sourceMap.get("seriesname"));
		mDetail.setId((String) sourceMap.get(IngestionConstants.MATCH_ID_FIELD));
		mDetail.setDay((String) sourceMap.get("match_day"));
		mDetail.setSeriesId((String) sourceMap.get(IngestionConstants.SERIES_ID));

		mDetail.setLive((String) sourceMap.get(IngestionConstants.LIVE));
		scoreCard.setMatchdetail(mDetail);
		scoreCard.setResult((String) sourceMap.get("matchresult"));

		String teamaId = (String) sourceMap.get("teama_Id");
		String teambId = (String) sourceMap.get("teamb_Id");
		
		scoreCard.setTeamHome(teamaId);
		scoreCard.setTeamAway(teambId);
		
		String teama = langUtil.namesInLanguage(teamaId, cquery.getLang());
		String teamb = langUtil.namesInLanguage(teambId, cquery.getLang());
		
		/*
		 * For predict n win team name change
		 */
		if (StringUtils.isNotBlank(teama)) {
			scoreCard.setTeamHomeName(teama);
		} else {
			scoreCard.setTeamHomeName((String) sourceMap.get(IngestionConstants.TEAM_A));
		}
		scoreCard.setTeamHomeShortName((String) sourceMap.get("teama_short"));
		
		/*
		 * For predict n win team name change
		 */
		if(StringUtils.isNotBlank(teamb)) {
			scoreCard.setTeamAwayName(teamb);
		}else {
			scoreCard.setTeamAwayName((String) sourceMap.get(IngestionConstants.TEAM_B));
		}
		scoreCard.setTeamAwayShortName((String) sourceMap.get("teamb_short"));

		if (StringUtils.isNotBlank((String) sourceMap.get("current_batting_team"))) {
			scoreCard.setCurrentBattingTeam((String) sourceMap.get("current_batting_team"));
			scoreCard.setCurrentBattingTeamName((String) sourceMap.get("current_batting_team_name"));
		}

		for (int i = 1; i < 5; i++) {

			if (StringUtils.isNotBlank((String) sourceMap.get("inn_score_" + i))) {
				Inning inning = new Inning();
				String inning_score = (String) sourceMap.get("inn_score_" + i);

				if (StringUtils.isBlank(inning_score)) {
					inning.setTotal(0);
				} else {
					inning.setTotal(Integer.parseInt(inning_score.split(" ")[0].split("/")[0]));
				}

				if (StringUtils.isBlank(inning_score)) {
					inning.setOvers("0");
				} else
					inning.setOvers(inning_score.split(" ")[1].substring(1, inning_score.split(" ")[1].length()));

				if (StringUtils.isNotBlank(inning_score)) {
					if (inning_score.split(" ")[0].split("/").length > 1) {
						inning.setWickets(Integer.parseInt(inning_score.split(" ")[0].split("/")[1]));
					} else {
						inning.setWickets(10);
					}
				} else
					inning.setWickets(0);

				inning.setTeamName((String) sourceMap.get("inn_team_" + i + "_name"));

				if (((String) sourceMap.get("inn_team_" + i + "_name")).equals(sourceMap.get("teama"))) {
					inning.setTeamShortName((String) sourceMap.get("teama_short"));
				} else if (((String) sourceMap.get("inn_team_" + i + "_name")).equals(sourceMap.get("teamb"))) {
					inning.setTeamShortName((String) sourceMap.get("teamb_short"));
				}

				inning.setBattingteam((String) sourceMap.get("inn_team_" + i));

				inning.setNumber(String.valueOf(i));
				innings.add(inning);
			}
		}

		if(cquery.getProbableTeamsSquads()){
			scoreCard.setTeams(getProbableTeamsSquad(matchId, teamaId, teambId));
		}

		scoreCard.setInnings(innings);
		
		LOG.info("Total widget execution took: " + (System.currentTimeMillis() - startTime) + " ms.");

		return scoreCard;
	}

	/**
	 * The the list of live/upcoming matches based on the specified query.
	 * 
	 * @param query
	 *            the cricket query object containing query params.
	 * 
	 * @return a match schedule.
	 * 
	 */
	public Schedule getScheduledOrLiveMatches(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		/*
		 * check for all params and build the query for cricket schedule.
		 */
		SortOrder order = SortOrder.ASC;

		if (StringUtils.isNotBlank(query.getSort())) {
			order = SortOrder.fromString(query.getSort());
		}

		BoolQueryBuilder scheduleQuery = QueryBuilders.boolQuery();

		BoolQueryBuilder matchStateQuery = QueryBuilders.boolQuery();

		/*
		 * Check and apply query for id of a match.
		 */

		if (query.getPastMatches()) {
			scheduleQuery.must(QueryBuilders.rangeQuery(IngestionConstants.END_MATCH_DATE_IST_FIELD)
					.lte(DateUtil.getCurrentDate("M/d/yyyy")));
			scheduleQuery.mustNot(QueryBuilders.termQuery(IngestionConstants.UPCOMING,
					CricketConstants.MATCH_UPCOMING_OR_LIVE_STATUS));
			scheduleQuery.must(QueryBuilders.termsQuery(IngestionConstants.MATCH_STATUS,
					CricketConstants.STATUS_MATCH_ENDED, CricketConstants.STATUS_MATCH_ABANDONED));
		}

		if (StringUtils.isNotBlank(query.getMatch_Id())) {
			scheduleQuery.must(QueryBuilders.termQuery(Constants.ROWID, query.getMatch_Id()));
		}

		if (StringUtils.isNotBlank(query.getFrom())) {

			RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(IngestionConstants.MATCH_DATE_IST_FIELD)
					.from(query.getFrom());
			if (StringUtils.isNotBlank(query.getTo())) {
				rangeQuery.to(query.getTo());
			}
			scheduleQuery.must(rangeQuery);
		}

		if (StringUtils.isNotBlank(query.getLive())
				&& CricketConstants.MATCH_UPCOMING_OR_LIVE_STATUS.equals(query.getLive())) {
			matchStateQuery.should(
					new BoolQueryBuilder().must(QueryBuilders.termQuery(IngestionConstants.LIVE, query.getLive()))
							.mustNot(QueryBuilders.termsQuery(IngestionConstants.MATCH_STATUS,
									CricketConstants.STATUS_MATCH_ENDED, CricketConstants.STATUS_MATCH_ABANDONED)));
		}

		if (StringUtils.isNotBlank(query.getUpcoming())
				&& CricketConstants.MATCH_UPCOMING_OR_LIVE_STATUS.equals(query.getUpcoming())) {
			matchStateQuery.should(QueryBuilders.termQuery(IngestionConstants.UPCOMING, query.getUpcoming()));
		}

		if (query.getSeriesId() != null && !query.getSeriesId().isEmpty()) {
			scheduleQuery.must(QueryBuilders.termsQuery(IngestionConstants.SERIES_ID, query.getSeriesId()));
		}

		if (StringUtils.isNotBlank(query.getRecent()) && CricketConstants.YES.equals(query.getRecent())) {
			matchStateQuery.should(QueryBuilders.termQuery(IngestionConstants.RECENT, query.getRecent()));
		}

		/*
		 * Check and apply query for team id.
		 */
		if (StringUtils.isNotBlank(query.getTeamId())) {
			scheduleQuery.must(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("teama_Id", query.getTeamId()))
					.should(QueryBuilders.termQuery("teamb_Id", query.getTeamId())).minimumShouldMatch(1));
		}

		/*
		 * Check and apply query for venue id.
		 */
		if (StringUtils.isNotBlank(query.getVenueId())) {
			scheduleQuery.must(QueryBuilders.termQuery("venue_Id", query.getVenueId()));
		}

		if (matchStateQuery.hasClauses()) {
			scheduleQuery.must(matchStateQuery);
		}

		LOG.debug("Final ES Query for Request: " + scheduleQuery);
		SearchRequestBuilder searchBuilder = client.prepareSearch(Indexes.CRICKET_SCHEDULE)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(scheduleQuery)
				.addSort(IngestionConstants.MATCH_DATE_IST_FIELD, order)
				.addSort(IngestionConstants.MATCH_TIME_IST_FIELD, order);

		/*
		 * Set the page size and corresponding offset for results.
		 */
		int pageSize = DEFAULT_SIZE;

		if (query.getPageSize() != null) {
			pageSize = query.getPageSize();
		}
		searchBuilder.setSize(pageSize);

		int offset = 0;

		if (query.getPageNumber() != null) {
			offset = (query.getPageNumber() - 1) * pageSize;
		}
		searchBuilder.setFrom(offset);

		/*
		 * Response Processing Starts Here.
		 */
		SearchResponse response = searchBuilder.execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		Schedule schedule = new Schedule();

		if (hits.length > 0) {
			List<Match> matches = new ArrayList<>();

			for (SearchHit hit : hits) {
				Match match = json.fromJson(hit.getSourceAsString(), Match.class);
				replaceTeamNames(match, query);
				matches.add(match);
			}
			schedule.setMatches(matches);
			long endTime = System.currentTimeMillis();

			LOG.info("Match Schedule query execution took: " + (endTime - startTime) + " ms.");
		} else {
			LOG.info("No matching records found for query: " + json.toJson(query));
		}

		return schedule;
	}

	private void replaceTeamNames(Match match, CricketQuery cricketQuery) {
		String teamAName = null;
		String teamBName = null;
		String teamAId = match.getTeamaId();
		teamAName = langUtil.namesInLanguage(teamAId, cricketQuery.getLang());
		String teamBId = match.getTeambId();
		teamBName = langUtil.namesInLanguage(teamBId, cricketQuery.getLang());
		if (teamAName != null)
			match.setTeama(teamAName);
		if (teamBName != null)
			match.setTeamb(teamBName);
	}

	/**
	 * Get the commentary for a particular match.
	 * 
	 * @param query
	 *            an instance of cricket query containing the query params.
	 * 
	 * @return a set of commentary.
	 * 
	 */
	public List<Commentary> getMatchCommentary(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		List<Commentary> commentaryList = new ArrayList<>();

		Commentary commentary = null;
		int from, to;

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(Constants.ROWID, query.getMatch_Id() + "_" + query.getInningsNumber()));

		if (StringUtils.isNotBlank(query.getInningsNumber())) {
			builder.must(QueryBuilders.termQuery("inningsNumber", query.getInningsNumber()));
		}

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		for (SearchHit hit : hits) {
			commentary = json.fromJson(hit.getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			int size = commentaries.size();

			if (StringUtils.isNotBlank(query.getFrom())) {
				from = Integer.valueOf(query.getFrom());
			} else {
				from = 0;
			}

			if (query.getCount() != null) {
				to = query.getCount();
			} else {
				to = DEFAULT_SIZE;
			}

			if (size == 0 || from < 0 || from > size - 1 || (from + to) < 0) {
				commentaryList.add(new Commentary());
				continue;
			} else if (to > size || (from + to) > size) {
				to = size - 1 - from;
			} else if (to < 0) {
				to += (size - 1 - from);
			}

			commentary.setCommentary(commentary.getCommentary().subList(from, (from + to)));
			commentaryList.add(commentary);

			LOG.debug("Result Set of Comments: " + json.toJson(commentary));
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Query execution took: " + (endTime - startTime) + " ms.");

		return commentaryList;
	}

	/**
	 * Get the commentary for a particular match by the id of the previous
	 * commentary.
	 * 
	 * @param query
	 *            an instance of cricket query containing the query params.
	 * 
	 * @return a set of commentary corresponding to given query.
	 * 
	 */
	public List<Commentary> getMatchCommentaryUsingId(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		Commentary commentary = null;
		int from = 0, to = DEFAULT_SIZE;

		// Query for the set of commentaries.
		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_MATCH_ID, query.getMatch_Id()))
				.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_INNINGS_NUMBER, query.getInningsNumber()));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		List<Commentary> commentaryList = new ArrayList<>();

		for (SearchHit hit : hits) {
			commentary = json.fromJson(hit.getSourceAsString(), Commentary.class);

			List<CommentaryItem> commentaries = commentary.getCommentary();
			int size = commentaries.size();

			// Initialize start and end points.
			if (StringUtils.isNotBlank(query.getFrom())) {
				from = Integer.valueOf(query.getFrom());
			}

			if (query.getCount() != null) {
				to = query.getCount();
			}

			// Parse props to get the set of commentaries as per query.
			if (size == 0 || from < 0 || from > size || to < 0) {
				commentaryList.add(new Commentary());
			} else if (from == 0) {
				if (to > size) {
					to = size;
				}
				commentary.setCommentary(commentary.getCommentary().subList(from, to));
				commentaryList.add(commentary);
			} else {
				from = size - from;
				int index;

				if (query.getNewRecords()) {
					index = (from - to) < 0 ? 0 : (from - to);
					commentary.setCommentary(commentary.getCommentary().subList(index, from));
					commentaryList.add(commentary);
				} else {
					from++;
					index = (from + to) > size ? size : (from + to);
					commentary.setCommentary(commentary.getCommentary().subList(from, index));
					commentaryList.add(commentary);
				}
			}
		}

		LOG.debug("Result Set of Comments: " + json.toJson(commentary));

		long endTime = System.currentTimeMillis();
		LOG.info("Query execution took: " + (endTime - startTime) + "ms.");

		return commentaryList;
	}

	/**
	 * Get the commentary for the ongoing over for a particular match.
	 * 
	 * @param query
	 *            an instance of cricket query containing the query params.
	 * 
	 * @return a set of commentary corresponding to given query.
	 * 
	 */
	public Commentary getOversCommentary(CricketQuery query) {
		long startTime = System.currentTimeMillis();

		Commentary thisOverCommentary = new Commentary();
		int from = 0, to = DEFAULT_SIZE;

		GetResponse response = client.prepareGet().setIndex(Indexes.CRICKET_COMMENTARY)
				.setId(query.getMatch_Id() + "_" + query.getInningsNumber()).execute().actionGet();

		if (response.isExists()) {
			Commentary commentary = json.fromJson(response.getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			int size = commentaries.size();

			if (size == 0) {
				return thisOverCommentary;
			} else if (to > size) {
				to = size;
			}

			commentaries = commentaries.subList(from, to);
			String over = null;

			for (CommentaryItem item : commentaries) {
				if (item.getIsBall()) {
					over = item.getOver().substring(0, item.getOver().length() - 2);
					break;
				}
			}

			if (StringUtils.isBlank(over)) {
				return thisOverCommentary;
			}

			Iterator<CommentaryItem> iterator = commentaries.iterator();

			while (iterator.hasNext()) {
				if (!iterator.next().getOver().startsWith(over)) {
					iterator.remove();
				}
			}
			thisOverCommentary.setCommentary(commentaries);
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Commentary returned for Match: " + query.getMatch_Id() + ", Inning: " + query.getInningsNumber()
				+ ". Execution Time: " + (endTime - startTime) + " ms.");

		return thisOverCommentary;
	}

	/**
	 * Get a list of matches for which the widgets are enabled.
	 * 
	 * @return A schedule containing the list of active matches i.e. matches for
	 *         which the widgets are enabled.
	 */
	public Schedule getActiveMatches() {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(CricketConstants.FLAG_WIDGET_GLOBAL, true));

		/* Fetch only the match id and flag fields to enable/disable matches. */
		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setFetchSource(new String[] { IngestionConstants.MATCH_ID_FIELD,
						CricketConstants.FLAG_WIDGET_HOME_PAGE, CricketConstants.FLAG_WIDGET_ARTICLE_PAGE,
						CricketConstants.FLAG_WIDGET_SPORTS_ARTICLE_PAGE, CricketConstants.FLAG_WIDGET_CATEGORY_PAGE,
						CricketConstants.FLAG_WIDGET_GLOBAL, IngestionConstants.LIVE, CricketConstants.FLAG_WIDGET_EVENT_ENABLED }, null)
				.setQuery(builder).addSort(IngestionConstants.MATCH_TIME_IST_FIELD, SortOrder.DESC).execute()
				.actionGet();

		SearchHit[] hits = response.getHits().getHits();

		Schedule schedule = new Schedule();

		if (hits.length > 0) {
			List<Match> matches = new ArrayList<>();

			for (SearchHit hit : hits) {

				Match match = json.fromJson(hit.getSourceAsString(), Match.class);
				matches.add(match);
				if (match.getLive() == 1) {
					matches.clear();
					matches.add(match);
					break;
				}
			}
			schedule.setMatches(matches);
		}
		long endTime = System.currentTimeMillis();
		LOG.info("Result returned for active matches query. Execution Time: " + (endTime - startTime) + "ms.");
		return schedule;
	}

	/**
	 * Get the squad list of teams for a given match
	 * 
	 * @param query
	 * @return
	 */
	public Map<?, ?> getSquadDetails(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCORECARD)
				.setTypes(MappingTypes.MAPPING_REALTIME).setFetchSource(IngestionConstants.TEAMS, null)
				.setQuery(QueryBuilders.termQuery(Constants.ROWID, query.getMatch_Id())).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		if (hits.length > 0) {
			return json.fromJson((String) hits[0].getSourceAsMap().get(IngestionConstants.TEAMS), Map.class);
		}

		LOG.info("Time taken to get squad details: " + (System.currentTimeMillis() - startTime) + "ms.");
		return new HashMap<>();
	}

	/**
	 * Method for providing cumulative data for manhattan graph and run rate graph.
	 * 
	 * @param query
	 * @return graph data list.
	 */
	@SuppressWarnings("unchecked")
	public Object getGraphData(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		// Query for the set of commentaries.
		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_MATCH_ID, query.getMatch_Id()))
				.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_INNINGS_NUMBER, query.getInningsNumber()));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		// Prepare linked hash map so that the order of overs is maintained.
		Map<Integer, Object> dataMap = new LinkedHashMap<>();
		List<Map<String, Object>> dataList = new ArrayList<>();

		if (hits.length > 0) {
			Commentary commentary = json.fromJson(hits[0].getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			for (CommentaryItem item : commentaries) {
				if (item.getIsBall()) {

					int over = Integer.parseInt(item.getOver().substring(0, item.getOver().length() - 2)) + 1;

					if (dataMap.containsKey(over)) {
						((List<CommentaryItem>) dataMap.get(over)).add(item);
					} else {
						List<CommentaryItem> list = new ArrayList<>();
						list.add(item);
						dataMap.put(over, list);
					}
				}
			}

			int balls = 0, totalRuns = 0;

			for (Entry<Integer, Object> entry : dataMap.entrySet()) {
				List<CommentaryItem> over = (List<CommentaryItem>) entry.getValue();
				balls += over.size();

				Map<String, Object> overSummary = new HashMap<>();

				int runs = 0, wickets = 0, i = 0;
				for (CommentaryItem item : over) {
					i++;
					runs += Integer.parseInt(item.getRuns());
					if (item.getIsWicket()) {
						wickets++;
					}

					if (item.getSummary() != null || i == over.size() - 1) {
						overSummary.put("Over",
								Integer.parseInt(item.getOver().substring(0, item.getOver().length() - 2)) + 1);
						overSummary.put("Runs", runs);
						overSummary.put("Wickets", wickets);
						overSummary.put("Bowler", item.getBowler());

						totalRuns += runs;
						overSummary.put("RunRate", ((double) totalRuns * 6) / balls);

						dataList.add(overSummary);
					}
				}
			}
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Time to prepare graph data: " + (endTime - startTime) + "ms.");
		return dataList;
	}

	/**
	 * Provides commenatry elements for wickets.
	 * 
	 * @param query
	 * @return
	 */
	public Commentary getWicketsCommentary(CricketQuery query) {
		long startTime = System.currentTimeMillis();

		// Query for the set of commentaries.
		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_MATCH_ID, query.getMatch_Id()))
				.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_INNINGS_NUMBER, query.getInningsNumber()));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();
		Commentary wicketCommentary = new Commentary();

		if (hits.length > 0) {
			Commentary commentary = json.fromJson(hits[0].getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			wicketCommentary
					.setCommentary(commentaries.stream().filter(e -> e.getIsWicket()).collect(Collectors.toList()));
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Time to get Wicket Commentary: " + (endTime - startTime) + " ms.");

		return wicketCommentary;
	}

	/**
	 * Provides commentary elements for boundaries(4's and 6's)
	 * 
	 * @param query
	 * @return
	 */
	public Commentary getBoundariesCommentary(CricketQuery query) {
		long startTime = System.currentTimeMillis();

		// Query for the set of commentaries.
		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_MATCH_ID, query.getMatch_Id()))
				.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_INNINGS_NUMBER, query.getInningsNumber()));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();
		Commentary boundariesCommentary = new Commentary();

		if (hits.length > 0) {
			Commentary commentary = json.fromJson(hits[0].getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			boundariesCommentary.setCommentary(commentaries.stream()
					.filter(e -> (e.getRuns().equals("4") || e.getRuns().equals("6"))).collect(Collectors.toList()));
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Time to get Wicket Commentary: " + (endTime - startTime) + " ms.");

		return boundariesCommentary;
	}

	/**
	 * Get the details of a particular innings.
	 * 
	 * @param query
	 * @return
	 */
	public Inning getInningsDetails(CricketQuery query) {
		long startTime = System.currentTimeMillis();

		Inning inning = new Inning();

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCORECARD)
				.setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(Constants.ROWID, query.getMatch_Id())).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		if (hits.length > 0) {

			Scorecard scoreCard = json.fromJson(hits[0].getSourceAsString(), Scorecard.class);

			List<Inning> innings = scoreCard.getInnings();

			if (!innings.isEmpty()) {
				if (StringUtils.isBlank(query.getInningsNumber())) {
					inning = innings.get(innings.size() - 1);
				} else {
					inning = innings.get(Integer.valueOf(query.getInningsNumber()) - 1);
				}
			}
			scoreCard.setInnings(Arrays.asList(inning));
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Scorecard Query execution took: " + (endTime - startTime) + " ms.");
		return inning;
	}

	public void calculateTopWinnersList(TopWinnersListRequest query) {
		try {
			long startTime = 0;
			String matchId = query.getMatchId();
			List<Map<String, Object>> records = new ArrayList<>();
			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.termQuery("matchId", matchId));
			qb.must(QueryBuilders.termQuery("isWin", true));

			SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("userId").field("userId").size(5000)
							.order(Order.aggregation("coinsGained", Boolean.FALSE))
							.subAggregation(AggregationBuilders.sum("coinsGained").field("coinsGained")))
					// .addSort("coinsGained", SortOrder.DESC)
					.setSize(0).execute().actionGet();

			Terms terms = response.getAggregations().get("userId");
			int rank = 1;
			for (Terms.Bucket bucket : terms.getBuckets()) {

				String userId = bucket.getKeyAsString();
				Map<String, Object> map = new HashMap<>();
				map.put("userId", userId);
				map.put("matchId", matchId);
				map.put("_id", (String) matchId + "_" + userId);
				map.put("rank", rank);
				Sum cointsGainedAgg = bucket.getAggregations().get("coinsGained");
				double coinsGainedValue = cointsGainedAgg.getValue();
				map.put("coinsGained", coinsGainedValue);
				map.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				records.add(map);
				rank++;
			}

			int updatedRecords = indexService.indexOrUpdate(Indexes.PW_USER_RANKING,
					MappingTypes.MAPPING_REALTIME, records);
			long endTime = System.currentTimeMillis();
			LOG.info("Updated top winners records " + updatedRecords + ". Execution time(Seconds): "
					+ (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("Exception in calculating top winners for predict and winners.", e);
		}

	}

	public TopWinnersListResponse getTopWinnersListWithUserRank(TopWinnersListRequest query) {
		long startTime = System.currentTimeMillis();
		TopWinnersListResponse topWinnersListResponse = new TopWinnersListResponse();
		List<TopWinnersResponse> topWinnersList = new ArrayList<TopWinnersResponse>();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.must(QueryBuilders.termQuery("matchId", query.getMatchId()))
				.must(QueryBuilders.existsQuery(PredictWinConstants.RANK));

		SearchResponse response = client.prepareSearch(Indexes.PW_USER_RANKING).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(boolQueryBuilder).setSize(10).addSort(PredictWinConstants.RANK, SortOrder.ASC).execute()
				.actionGet();

		SearchHit[] hits = response.getHits().getHits();

		if (hits != null && hits.length > 0) {
			for (SearchHit hit : response.getHits().getHits()) {
				String userId = hit.getSourceAsMap().get("userId").toString();
				double totalCoinsGained = Double
						.valueOf(hit.getSourceAsMap().get(PredictWinConstants.COINS_GAINED).toString());
				int rank = Integer.valueOf(hit.getSourceAsMap().get(PredictWinConstants.RANK).toString());
				TopWinnersResponse topWinner = new TopWinnersResponse();
				topWinner.setUserId(userId);
				topWinner.setTotalCoinsGained(totalCoinsGained);
				topWinner.setRank(rank);
				topWinnersList.add(topWinner);
			}
		}

		topWinnersListResponse.setTopWinners(topWinnersList);

		// If userId passed in query then return detail of user with self rank
		if (StringUtils.isNotBlank(query.getMatchId()) && StringUtils.isNotBlank(query.getUserId())) {
			GetResponse userRankGetResponse = client.prepareGet().setIndex(Indexes.PW_USER_RANKING)
					.setId(query.getMatchId() + "_" + query.getUserId()).execute().actionGet();
			LuckyWinnersResponse userDetail = new LuckyWinnersResponse();
			userDetail.setUserId(query.getUserId());
			if (userRankGetResponse.isExists()
					&& userRankGetResponse.getSourceAsMap().containsKey(PredictWinConstants.RANK)) {
				userDetail.setTotalCoinsGained(Double.valueOf(
						userRankGetResponse.getSourceAsMap().get(PredictWinConstants.COINS_GAINED).toString()));
				userDetail.setRank((Integer) userRankGetResponse.getSourceAsMap().get(PredictWinConstants.RANK));
			}
			topWinnersListResponse.setUserDetail(userDetail);
		}

		long endTime = System.currentTimeMillis();
		LOG.info("TopWinnersListWithUserRank Query Execution Time: " + (endTime - startTime) + " ms.");
		return topWinnersListResponse;
	}

	public BidDetailsSummaryResponse getBidDetailsSummary(BidDetailsSummaryRequest query) {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery("matchId", query.getMatchId()))
				.must(QueryBuilders.termQuery("ticketId", query.getTicketId()))
				.must(QueryBuilders.termQuery("userId", query.getUserId()));

		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).setSize(11).execute().actionGet();

		BidDetailsSummaryResponse bidsDetailsSummary = new BidDetailsSummaryResponse();
		if (response.getHits().getHits() != null && response.getHits().getHits().length > 0) {
			for (SearchHit hit : response.getHits().getHits()) {
				String ticketId = hit.getSourceAsMap().get("ticketId").toString();
				String userId = hit.getSourceAsMap().get("userId").toString();
				String matchId = hit.getSourceAsMap().get("matchId").toString();
				String bidType = hit.getSourceAsMap().get("bidType").toString();
				String bidTypeId = hit.getSourceAsMap().get("bidTypeId").toString();
				int prediction = Integer.valueOf(hit.getSourceAsMap().get("prediction").toString());
				int coinsBid = Integer.valueOf(hit.getSourceAsMap().get("coinsBid").toString());
				bidsDetailsSummary.setBidType(bidType);
				bidsDetailsSummary.setBidTypeId(bidTypeId);
				bidsDetailsSummary.setCoinsBid(coinsBid);
				bidsDetailsSummary.setMatchId(matchId);
				bidsDetailsSummary.setPrediction(prediction);
				bidsDetailsSummary.setTicketId(ticketId);
				bidsDetailsSummary.setUserId(userId);

			}
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Bids Details Summary Query execution took: " + (endTime - startTime) + " ms.");
		return bidsDetailsSummary;
	}

	/*
	 * public String updatedPointsOnlyForTesting() { String idAfterUpdate = null;
	 * try { idAfterUpdate = CricketUtils.getBidPointsUpdated("abc"); } catch
	 * (Exception e) { LOG.info("Exception in fetching "); } return idAfterUpdate; }
	 */

	public PredictAndWinPostResponse updatedPointsPostAPI(RequestForMatchWinnerPost requestForMatchWinnerPost) {
		PredictAndWinPostResponse postResponse = null;
		List<TicketIdAndGainPointResponse> ticketIdAndGainResponseInList = new ArrayList<TicketIdAndGainPointResponse>();
		Map<String, Object> serviceDataMap = new HashMap<>();

		// coinsBid
		// String includes[] = { "ticketId", "coinsGained" };

		String includes[] = { "ticketId", "coinsBid" };
		String excludes[] = { "vendorId", "pVendorId", "bidType", "bidTypeId", "prediction", "coinsGained",
				"createdDateTime", "datetime", "isWin", "isBidAccepted" };

		BoolQueryBuilder qb = new BoolQueryBuilder();
		qb.must(QueryBuilders.termQuery("matchId", requestForMatchWinnerPost.getMatchId()));
		// qb.must(QueryBuilders.termQuery("isBidAccepted", true));

		SearchRequestBuilder srb1 = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(qb)
				.addAggregation(AggregationBuilders.terms("userId").field("userId")
						.subAggregation(AggregationBuilders.topHits("topStories").fetchSource(includes, excludes)
								.size(300).sort("datetime", SortOrder.DESC)));

		MultiSearchResponse multiResponse = client.prepareMultiSearch().add(srb1).execute().actionGet();
		LOG.info("###Size " + multiResponse.getResponses().length);

		try {
			for (Item response1 : multiResponse.getResponses()) {
				Terms cidsResult = response1.getResponse().getAggregations().get("userId");
				for (Terms.Bucket entry : cidsResult.getBuckets()) {
					Map<String, Object> map = new HashMap<>();
					String userId = (String) entry.getKey();

					map.put("userId", userId);

					TopHits topStories = entry.getAggregations().get("topStories");
					SearchHit[] stories = topStories.getHits().getHits();
					for (SearchHit searchHit : stories) {
						TicketIdAndGainPointResponse ticketIdAndGainResponse = new TicketIdAndGainPointResponse();
						Map<String, Object> source = searchHit.getSourceAsMap();
						String ticketId = (String) source.get("ticketId");
						// int coinsGained = Integer.parseInt((String)
						// source.get("coinsGained"));
						int coinsGained = (Integer) (source.get("coinsBid"));
						ticketIdAndGainResponse.setGaincoin(coinsGained);
						ticketIdAndGainResponse.setTicketid(ticketId);
						ticketIdAndGainResponseInList.add(ticketIdAndGainResponse);
					}
					TicketIdAndGainPointResponseList ticketIdAndGainResponseList = new TicketIdAndGainPointResponseList();
					ticketIdAndGainResponseList.setData(ticketIdAndGainResponseInList);
					serviceDataMap.put(userId, ticketIdAndGainResponseList);
				}
			}
			postResponse = CricketUtils.getBidPointsUpdated(serviceDataMap);
		} catch (Exception e) {
			LOG.error("Exception in bids poins updation for predict and win  " + e.getMessage(), e);
		}

		return postResponse;
	}

	public PredictAndWinPostResponse matchWinnersPOSTAPI(RequestForMatchWinnerPost requestForMatchWinnerPost) {
		PredictAndWinPostResponse postResponse = null;
		List<MatchWinnerDetailPostRequest> matchWinnerDetailsPostList = new ArrayList<MatchWinnerDetailPostRequest>();

		BoolQueryBuilder qb = new BoolQueryBuilder();
		qb.must(QueryBuilders.termQuery("matchId", requestForMatchWinnerPost.getMatchId()));

		SearchResponse searchResponse = client.prepareSearch(PredictAndWinIndexes.USER_WINNERS_LIST).setQuery(qb)
				.addSort("rank", SortOrder.DESC).setSize(requestForMatchWinnerPost.getSize()).execute().actionGet();

		SearchHit[] searchHits = searchResponse.getHits().getHits();

		for (SearchHit hit : searchHits) {
			MatchWinnerDetailPostRequest matchWinnerPostDetail = new MatchWinnerDetailPostRequest();
			Map<String, Object> source = hit.getSourceAsMap();
			String userId = (String) source.get("userId");
			String matchId = (String) source.get("matchId");
			String datetime = (String) source.get("datetime");
			int rank = Integer.valueOf(hit.getSourceAsMap().get("rank").toString());
			if (rank <= 10)
				matchWinnerPostDetail.setWinner_type(0);
			else
				matchWinnerPostDetail.setWinner_type(1);

			matchWinnerPostDetail.setUser_id(userId);
			matchWinnerPostDetail.setMatch_id(matchId);
			matchWinnerPostDetail.setMatch_no(1);
			matchWinnerPostDetail.setMatch_name("Ind V Aus");
			matchWinnerPostDetail.setMatch_datetime(datetime);
			matchWinnerDetailsPostList.add(matchWinnerPostDetail);
		}

		try {
			postResponse = CricketUtils.matchWinnersPostAPI(matchWinnerDetailsPostList);
		} catch (Exception e) {
			LOG.error("Exception in match winners POST API", e);
		}
		return postResponse;
	}

	public Schedule getWidgetConfig() {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(CricketConstants.FLAG_WIDGET_GLOBAL, true));

		/* Fetch only the match id and flag fields to enable/disable matches. */
		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setFetchSource(new String[] { IngestionConstants.MATCH_ID_FIELD, IngestionConstants.FLAG_BHASKAR_APP,
						IngestionConstants.FLAG_BHASKAR_WAP, IngestionConstants.FLAG_BHASKAR_WEB,
						IngestionConstants.FLAG_DIVYA_APP, IngestionConstants.FLAG_DIVYA_WAP,
						IngestionConstants.FLAG_DIVYA_WEB, CricketConstants.FLAG_WIDGET_CATEGORY_PAGE }, null)
				.setQuery(builder).addSort(IngestionConstants.MATCH_TIME_IST_FIELD, SortOrder.ASC).execute()
				.actionGet();

		SearchHit[] hits = response.getHits().getHits();

		Schedule schedule = new Schedule();

		if (hits.length > 0) {
			List<Match> matches = new ArrayList<>();

			for (SearchHit hit : hits) {
				matches.add(json.fromJson(hit.getSourceAsString(), Match.class));
			}
			schedule.setMatches(matches);
		}
		long endTime = System.currentTimeMillis();
		LOG.info("Result returned for active matches query. Execution Time: " + (endTime - startTime) + "ms.");
		return schedule;
	}

	/**
	 * Given the cricket query, provide the commentary for a match and provided
	 * inning.
	 * 
	 * @param query
	 * @return
	 */
	public Commentary getCommentary(CricketQuery query) {
		long startTime = System.currentTimeMillis();

		int type = query.getType();

		// Query for the set of commentaries.
		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_MATCH_ID, query.getMatch_Id()))
				.must(QueryBuilders.termQuery(IngestionConstants.COMMENTARY_INNINGS_NUMBER, query.getInningsNumber()));

		SearchResponse response = client.prepareSearch(Indexes.CRICKET_COMMENTARY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();
		Commentary resultingCommentary = new Commentary();

		if (hits.length > 0) {
			Commentary commentary = json.fromJson(hits[0].getSourceAsString(), Commentary.class);
			List<CommentaryItem> commentaries = commentary.getCommentary();

			/*
			 * Check the value of type and return appropriate set of commentaries.
			 */
			if (type == CommentaryType.SIXES) {

				resultingCommentary.setCommentary(
						commentaries.stream().filter(e -> e.getRuns().equals("6")).collect(Collectors.toList()));
			} else if (type == CommentaryType.FOURS) {

				resultingCommentary.setCommentary(
						commentaries.stream().filter(e -> e.getRuns().equals("4")).collect(Collectors.toList()));
			} else if (type == CommentaryType.BOUNDARIES) {

				resultingCommentary.setCommentary(
						commentaries.stream().filter(e -> (e.getRuns().equals("4") || e.getRuns().equals("6")))
								.collect(Collectors.toList()));
			} else if (type == CommentaryType.WICKETS) {

				resultingCommentary
						.setCommentary(commentaries.stream().filter(e -> e.getIsWicket()).collect(Collectors.toList()));
			} else if (type == CommentaryType.CURRENT_OVER) {

				String over = null;

				for (CommentaryItem item : commentaries) {
					if (item.getIsBall()) {
						over = item.getOver().substring(0, item.getOver().length() - 2);
						break;
					}
				}

				if (StringUtils.isBlank(over)) {
					return resultingCommentary;
				}

				final String overs = over;

				commentaries = commentaries.stream().filter(e -> e.getOver().startsWith(overs))
						.collect(Collectors.toList());

				resultingCommentary.setCommentary(commentaries);
			} else {
				int from = 0, to = DEFAULT_SIZE;

				int size = commentaries.size();

				// Initialize start and end points.
				if (StringUtils.isNotBlank(query.getFrom())) {
					from = Integer.valueOf(query.getFrom());
				}

				if (query.getCount() != null) {
					to = query.getCount();
				}

				// Parse props to get the set of commentaries as per query.
				if (size == 0 || from < 0 || from > size || to < 0) {
					return resultingCommentary;
				} else if (from == 0) {
					if (to > size) {
						to = size;
					}
					resultingCommentary.setCommentary(commentaries.subList(from, to));
				} else {
					from = size - from;
					int index;

					if (query.getNewRecords()) {
						index = (from - to) < 0 ? 0 : (from - to);
						resultingCommentary.setCommentary(commentaries.subList(index, from));
					} else {
						from++;
						index = (from + to) > size ? size : (from + to);
						resultingCommentary.setCommentary(commentaries.subList(from, index));
					}
				}
			}
		}

		long endTime = System.currentTimeMillis();
		LOG.info("Time to get Wicket Commentary: " + (endTime - startTime) + " ms.");

		return resultingCommentary;
	}

	public List<Map<String, Object>> getCricketTeamStanding(Map<String, Object> query) {
		if (query.containsKey(Constants.Cricket.SERIES)) {
			try {
				List<Map<String, Object>> seriesTeamStanding = new ArrayList<>();

				BoolQueryBuilder builder = new BoolQueryBuilder();
				builder.must(QueryBuilders.termQuery(Constants.Cricket.Series.ID, query.get(Constants.Cricket.SERIES)));

				SearchResponse response = client.prepareSearch(Indexes.CRICKET_TEAM_STANDING)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder)
						.addSort(Constants.Cricket.TeamStanding.POSITION, SortOrder.ASC).execute().actionGet();

				SearchHit[] searchHits = response.getHits().getHits();

				for (SearchHit hit : searchHits) {
					seriesTeamStanding.add(hit.getSource());
				}

				return seriesTeamStanding;
			} catch (Exception e) {
				throw new DBAnalyticsException(
						"Failed to get team standings detail for series id: " + query.get(Constants.Cricket.SERIES), e);
			}
		} else {
			throw new DBAnalyticsException("Series Id not found in request");
		}
	}

	public boolean calculateUsersBidRank(String matchId) {
		try {
			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.termQuery("matchId", matchId));

			SearchResponse searchResponse = client.prepareSearch(Indexes.PW_USER_RANKING)
					.setTypes(MappingTypes.MAPPING_REALTIME).setScroll(new TimeValue(60000)).setQuery(qb)
					.addSort(Cricket.PredictWinConstants.COINS_GAINED, SortOrder.DESC).setSize(1000).execute()
					.actionGet();

			int rank = 1;
			List<Map<String, Object>> userBidRankList = new ArrayList<>();

			do {
				for (SearchHit hit : searchResponse.getHits().getHits()) {
					Map<String, Object> userBidMap = hit.getSourceAsMap();

					userBidMap.put(Cricket.PredictWinConstants.RANK, rank);
					userBidMap.put(Constants.ROWID, hit.getId());
					userBidRankList.add(userBidMap);

					++rank;
				}

				indexService.indexOrUpdate(Indexes.PW_USER_RANKING, MappingTypes.MAPPING_REALTIME, userBidRankList);
				userBidRankList.clear();

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();

			} while (searchResponse.getHits().getHits().length != 0);

		} catch (Exception e) {
			throw new DBAnalyticsException("Error encountered while calculating user rank: ",e);
		}
		return true;
	}
	
	private Map<String, Object> getProbableTeamsSquad(String matchId, String teamaId, String teambId) {
		Map<String, Object> temasSquadsObject = new HashMap<>();
		try{
			SearchResponse searchResponse = client.prepareSearch(Indexes.CRICKET_PROBABLE_TEAM_SQUADS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(QueryBuilders.termsQuery("id", Arrays.asList(teamaId, teambId)))
					.execute().actionGet();

			SearchHit[] searchHits = searchResponse.getHits().getHits();

			for (SearchHit hit : searchHits) {				
				Map<String, Object> teamSquadsObject = hit.getSource();

				temasSquadsObject.put(teamSquadsObject.get("id").toString(), mapper.readValue((String) teamSquadsObject.get("team"), Map.class));
			}
			
			return temasSquadsObject;
		}catch(Exception e){
			LOG.debug("Error while retrieving Probabable Teams Squad for match: "+matchId, e);
			return temasSquadsObject;
		}
	}
	
	/**
	 * Get a list of matches for which the widgets are enabled.
	 * 
	 * @return A schedule containing the list of active matches i.e. matches for
	 *         which the widgets are enabled.
	 */
	public Schedule getLastActiveMatch() {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.existsQuery(CricketConstants.FLAG_WIDGET_GLOBAL));

		/*
		 * Get last 2 matches for which widget global field exists. if any of the 2 is
		 * live, return that match, else return the last match.
		 */
		SearchResponse response = client.prepareSearch(Indexes.CRICKET_SCHEDULE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setFetchSource(new String[] { IngestionConstants.MATCH_ID_FIELD,
						CricketConstants.FLAG_WIDGET_HOME_PAGE, CricketConstants.FLAG_WIDGET_ARTICLE_PAGE,
						CricketConstants.FLAG_WIDGET_SPORTS_ARTICLE_PAGE, CricketConstants.FLAG_WIDGET_CATEGORY_PAGE,
						CricketConstants.FLAG_WIDGET_GLOBAL, IngestionConstants.LIVE }, null)
				.setQuery(builder).addSort(IngestionConstants.MATCH_DATE_IST_FIELD, SortOrder.DESC)
				.addSort(IngestionConstants.MATCH_TIME_IST_FIELD, SortOrder.DESC).setSize(2).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		Schedule schedule = new Schedule();

		if (hits.length > 0) {
			if (hits.length == 2) {
				Match m = json.fromJson(hits[1].getSourceAsString(), Match.class);

				if (m.getLive() == 1) {
					schedule.setMatches(Arrays.asList(m));
				} else {
					schedule.setMatches(Arrays.asList(json.fromJson(hits[0].getSourceAsString(), Match.class)));
				}
			} else {
				schedule.setMatches(Arrays.asList(json.fromJson(hits[0].getSourceAsString(), Match.class)));
			}
		}
		
		long endTime = System.currentTimeMillis();
		LOG.info("Result returned for active matches query. Execution Time: " + (endTime - startTime) + "ms.");
		return schedule;
	}

	public Scorecard getScoreCardWithPlayersStats(CricketQuery cquery){
		try{
			String matchId = cquery.getMatch_Id();
			String lang = cquery.getLang();

			long startTime = System.currentTimeMillis();

			GetResponse response = client.prepareGet().setIndex(Indexes.CRICKET_SCORECARD).setId(matchId).execute()
					.actionGet();

			long endTime = System.currentTimeMillis();

			if (response.isExists()) {
				LOG.debug("Result JSON: " + response.getSourceAsString());
				Scorecard scoreCard = json.fromJson(response.getSourceAsString(), Scorecard.class);
				if (scoreCard.getInnings() == null) {
					scoreCard.setInnings(new ArrayList<Inning>());
				}

				Matchdetail matchDetail = scoreCard.getMatchdetail();
				Map<String, Object> teamObject = null;
				
				String seriesId = matchDetail.getSeriesId();

				if (StringUtils.isNotBlank((String) response.getSourceAsMap().get(IngestionConstants.TEAMS))){

					if(cquery.getProbableTeamsSquads() && StringUtils.isBlank(matchDetail.getTosswonby())){
						teamObject = getProbableTeamsSquad(matchDetail.getId(), scoreCard.getTeamAway(), scoreCard.getTeamHome());
					}else{
						teamObject = mapper.readValue((String) response.getSourceAsMap().get(IngestionConstants.TEAMS), new TypeReference<Map<String, Object>>() {
						});
					}

				}else if(cquery.getProbableTeamsSquads()){
					teamObject = getProbableTeamsSquad(matchDetail.getId(), scoreCard.getTeamAway(), scoreCard.getTeamHome());
				}
				
				mergePlayersStatsWithTeam(seriesId, teamObject);

				scoreCard.setTeams(teamObject);

				LOG.debug("ScoreCard Json: " + json.toJson(scoreCard));
				LOG.info(
						"Scorecard returned for match " + matchId + ". Execution Time: " + (endTime - startTime) + " ms.");

				langUtil.scoreCardInLanguage(scoreCard, lang);

				return scoreCard;
			} else {
				LOG.warn("No Scorecard found for Id: " + matchId
						+ "; Returning data from widget details. Execution Time: " + (endTime - startTime) + " ms.");
				return getWidgetDetailsWithPlayersStats(cquery);
			}
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void mergePlayersStatsWithTeam(String seriesId, Map<String, Object> team) {
		try {
			Set<String> teamsId = team.keySet();

			for (String teamId : teamsId) {
				List<Map<String, Object>> teamPlayerStat = getPalyersStats(seriesId, teamId);
				Map<String, Object> teamPlayersObject = (Map<String, Object>) ((Map<String, Object>) team.get(teamId))
						.get("Players");

				for (Map<String, Object> playerStat : teamPlayerStat) {
					String playerId = (String) playerStat.get(IngestionConstants.BATSMAN);
					if (teamPlayersObject.containsKey(playerId)) {
						((Map<String, Object>) teamPlayersObject.get(playerId)).put("stats", playerStat);
					}
				}
			}
		} catch (Exception e) {
			LOG.error("Error While merging player stats with scorecard", e);
		}
	}

	public List<Map<String, Object>> getPalyersStats(String seriesId, String teamId){
		try{
			List<Map<String, Object>> playersStat = new ArrayList<>();

			BoolQueryBuilder cqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(IngestionConstants.SERIES_ID,
							seriesId));

			if(StringUtils.isNotBlank(teamId)){
				
				cqb.must(QueryBuilders.termQuery(IngestionConstants.TEAM_ID,
						teamId));
			}

			SearchResponse ser = client.prepareSearch(Indexes.CRICKET_PLAYER_PERFORMANCE).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(cqb).setSize(1000).execute().actionGet();

			SearchHit[] searchHits = ser.getHits().getHits();

			for (SearchHit hit : searchHits) {		
				Map<String, Object> disabledMatch =  hit.getSource();
				playersStat.add(disabledMatch);
			}

			return playersStat;
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	public Scorecard getWidgetDetailsWithPlayersStats(CricketQuery cquery) {
		String matchId = cquery.getMatch_Id();
		long startTime = System.currentTimeMillis();

		Scorecard scoreCard = new Scorecard();

		GetResponse response = client.prepareGet().setIndex(Indexes.CRICKET_SCHEDULE).setId(matchId).execute()
				.actionGet();

		if (!response.isExists()) {
			LOG.warn("No Records found for Id: " + matchId);
			return new Scorecard();
		}

		LOG.debug("result json: " + response.getSourceAsString());

		LOG.info("Widget query execution took: " + (System.currentTimeMillis() - startTime) + " ms.");

		List<Inning> innings = new ArrayList<>();

		Map<String, Object> sourceMap = response.getSourceAsMap();
		Matchdetail mDetail = new Matchdetail();
		mDetail.setStatus((String) sourceMap.get("matchstatus"));
		mDetail.setVenue((String) sourceMap.get("venue"));
		mDetail.setEquation((String) sourceMap.get("equation"));
		mDetail.setTime((String) sourceMap.get(IngestionConstants.MATCH_TIME_IST_FIELD));
		mDetail.setTourName((String) sourceMap.get("tourname"));
		mDetail.setDate((String) sourceMap.get(IngestionConstants.MATCH_DATE_IST_FIELD));
		mDetail.setType((String) sourceMap.get("matchtype"));
		mDetail.setTosswonby((String) sourceMap.get("toss_won_by_name"));
		mDetail.setNumber((String) sourceMap.get("matchnumber"));
		mDetail.setSeriesName((String) sourceMap.get("seriesname"));
		mDetail.setId((String) sourceMap.get(IngestionConstants.MATCH_ID_FIELD));
		mDetail.setDay((String) sourceMap.get("match_day"));
		mDetail.setSeriesId((String) sourceMap.get(IngestionConstants.SERIES_ID));

		mDetail.setLive((String) sourceMap.get(IngestionConstants.LIVE));
		scoreCard.setMatchdetail(mDetail);
		scoreCard.setResult((String) sourceMap.get("matchresult"));

		String teamaId = (String) sourceMap.get("teama_Id");
		String teambId = (String) sourceMap.get("teamb_Id");
		String seriesId = mDetail.getSeriesId();

		scoreCard.setTeamHome(teamaId);
		scoreCard.setTeamAway(teambId);

		String teama = langUtil.namesInLanguage(teamaId, cquery.getLang());
		String teamb = langUtil.namesInLanguage(teambId, cquery.getLang());

		/*
		 * For predict n win team name change
		 */
		if(StringUtils.isNotBlank(teama)) {
			scoreCard.setTeamHomeName(teama);
		}else {
			scoreCard.setTeamHomeName((String) sourceMap.get(IngestionConstants.TEAM_A));
		}
		scoreCard.setTeamHomeShortName((String) sourceMap.get("teama_short"));

		/*
		 * For predict n win team name change
		 */
		if(StringUtils.isNotBlank(teamb)) {
			scoreCard.setTeamAwayName(teamb);
		}else {
			scoreCard.setTeamAwayName((String) sourceMap.get(IngestionConstants.TEAM_B));
		}
		scoreCard.setTeamAwayShortName((String) sourceMap.get("teamb_short"));

		if (StringUtils.isNotBlank((String) sourceMap.get("current_batting_team"))) {
			scoreCard.setCurrentBattingTeam((String) sourceMap.get("current_batting_team"));
			scoreCard.setCurrentBattingTeamName((String) sourceMap.get("current_batting_team_name"));
		}

		for (int i = 1; i < 5; i++) {

			if (StringUtils.isNotBlank((String) sourceMap.get("inn_score_" + i))) {
				Inning inning = new Inning();
				String inningScore = (String) sourceMap.get("inn_score_" + i);

				if (StringUtils.isBlank(inningScore)) {
					inning.setTotal(0);
				} else {
					inning.setTotal(Integer.parseInt(inningScore.split(" ")[0].split("/")[0]));
				}

				if (StringUtils.isBlank(inningScore)) {
					inning.setOvers("0");
				} else
					inning.setOvers(inningScore.split(" ")[1].substring(1, inningScore.split(" ")[1].length()));

				if (StringUtils.isNotBlank(inningScore)) {
					if (inningScore.split(" ")[0].split("/").length > 1) {
						inning.setWickets(Integer.parseInt(inningScore.split(" ")[0].split("/")[1]));
					} else {
						inning.setWickets(10);
					}
				} else
					inning.setWickets(0);

				inning.setTeamName((String) sourceMap.get("inn_team_" + i + "_name"));

				if (((String) sourceMap.get("inn_team_" + i + "_name")).equals(sourceMap.get("teama"))) {
					inning.setTeamShortName((String) sourceMap.get("teama_short"));
				} else if (((String) sourceMap.get("inn_team_" + i + "_name")).equals(sourceMap.get("teamb"))) {
					inning.setTeamShortName((String) sourceMap.get("teamb_short"));
				}

				inning.setBattingteam((String) sourceMap.get("inn_team_" + i));

				inning.setNumber(String.valueOf(i));
				innings.add(inning);
			}
		}

		if (cquery.getProbableTeamsSquads()) {
			Map<String, Object> teamObject = getProbableTeamsSquad(matchId, sourceMap.get("teama_Id").toString(),
					sourceMap.get("teamb_Id").toString());

			mergePlayersStatsWithTeam(seriesId, teamObject);

			scoreCard.setTeams(teamObject);
		}

		scoreCard.setInnings(innings);

		LOG.info("Total widget execution took: " + (System.currentTimeMillis() - startTime) + " ms.");

		return scoreCard;
	}
	
	public List<String> matchDisabledIds(String matchId) {
		
		long startTime = System.currentTimeMillis();
		List<String> matchDisabledIdsList = new ArrayList<>();

		try {

			BoolQueryBuilder cqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(IngestionConstants.MATCH_ID_FIELD, matchId))
					.must(QueryBuilders.termQuery(IngestionConstants.ISENABLED, false));

			SearchResponse ser = client.prepareSearch(Indexes.CRICKET_MATCH_DISABLED_IDS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(cqb).setSize(100).execute().actionGet();

			SearchHit[] searchHits = ser.getHits().getHits();

			for (SearchHit hit : searchHits) {
				Map<String, Object> matchDisabledIds = hit.getSource();

				if (matchDisabledIds.containsKey(Constants.ID)) {
					matchDisabledIdsList.add((String) matchDisabledIds.get(Constants.ID));
				}
			}
			return matchDisabledIdsList;
		} catch (Exception e) {
			throw new DBAnalyticsException("Failed to get disabled ids for match id: " + matchId, e);
		} finally {
			LOG.info("Total execution time: " + (System.currentTimeMillis() - startTime) + " ms. Response: "
					+ matchDisabledIdsList);
		}
	}
	
	/**
	 * The set of squads for given query
	 * 
	 * @param query
	 * @return
	 */
	public Object getSquads(CricketQuery query) {

		Map<String, Squad> squads = new HashMap<>();

		BoolQueryBuilder builder = QueryBuilders.boolQuery();

		if (query.getSeriesId() != null && !query.getSeriesId().isEmpty()) {
			builder.must(QueryBuilders.termsQuery("seriesId", query.getSeriesId()));
		}

		if (query.getLeagueId() != null && !query.getLeagueId().isEmpty()) {
			builder.must(QueryBuilders.termsQuery("seriesId", query.getSeriesId()));
		}

		try {
			SearchResponse response = client.prepareSearch(Indexes.CRICKET_TEAM_SQUADS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).execute().actionGet();

			for (SearchHit hit : response.getHits().getHits()) {
				Squad squad = json.fromJson(hit.getSourceAsString(), Squad.class);
				squads.put(squad.getTeamId(), squad);
			}
		} catch (Exception e) {
			LOG.error("Failed to retrieve squads for series id: " + query.getSeriesId(), e);
		}

		return squads;
	}
	
	public static void main(String[] args) {
		CricketQuery query = new CricketQuery();
		query.setSeriesId(Arrays.asList(new String[] {"2952"}));

		Gson gson = new GsonBuilder().create();
		System.out.println(gson.toJson(new CricketQueryExecutorService().getSquads(query)));
	}
}
