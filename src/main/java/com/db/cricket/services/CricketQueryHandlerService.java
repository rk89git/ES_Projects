package com.db.cricket.services;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants.CricketConstants.CommentaryType;
import com.db.common.exception.DBAnalyticsException;
import com.db.cricket.model.BidDetailsSummaryRequest;
import com.db.cricket.model.BidDetailsSummaryResponse;
import com.db.cricket.model.CricketQuery;
import com.db.cricket.model.PredictAndWinPostResponse;
import com.db.cricket.model.RequestForMatchWinnerPost;
import com.db.cricket.model.TopWinnersListRequest;
import com.db.cricket.model.TopWinnersListResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class CricketQueryHandlerService {

	private CricketQueryExecutorService executorService = new CricketQueryExecutorService();

	private Gson gson = new GsonBuilder().create();

	public String getScoreCard(String query) {
		CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

		if (cquery == null || StringUtils.isBlank(cquery.getMatch_Id())) {
			throw new DBAnalyticsException("Invalid Request. Required Parameter \'Match_Id\' is missing.");
		}

		return gson.toJson(executorService.getScoreCard(cquery));
	}

	public String getWidgetDetails(String query) {
		CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

		if (cquery == null || StringUtils.isBlank(cquery.getMatch_Id())) {
			throw new DBAnalyticsException("Invalid Request. Required Parameter \'Match_Id\' is missing.");
		}

		return gson.toJson(executorService.getWidgetDetails(cquery));
	}

	public String getScheduledOrLiveMatches(String query) {

		if (StringUtils.isBlank(query) || gson.fromJson(query, Map.class).isEmpty()) {
			throw new DBAnalyticsException(
					"Invalid Request. Atleast one of \'Match_Id\', \'live\',\'upcoming\' or [\'from\' and \'to\'] should be present.");
		}

		return gson.toJson(executorService.getScheduledOrLiveMatches(gson.fromJson(query, CricketQuery.class)));
	}

	public String getMatchCommentary(String query) {

		CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

		if (StringUtils.isBlank(query) || gson.fromJson(query, Map.class).isEmpty()
				|| StringUtils.isBlank(cquery.getMatch_Id())) {
			throw new DBAnalyticsException("Invalid Request. Missing Required Parameter \'Match_Id\'");
		}

		if (cquery.getType() == CommentaryType.ALL) {
			return gson.toJson(executorService.getMatchCommentaryUsingId(cquery));
		} else {
			return gson.toJson(executorService.getCommentary(cquery));
		}
	}

	public String getActiveMatches() {
		return gson.toJson(executorService.getActiveMatches());
	}

	public String getMatchSquads(String query) {

		CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

		if (StringUtils.isBlank(query) || gson.fromJson(query, Map.class).isEmpty()
				|| StringUtils.isBlank(cquery.getMatch_Id())) {
			throw new DBAnalyticsException("Invalid Request. Missing Required Parameter \'Match_Id\'");
		}

		return gson.toJson(executorService.getSquadDetails(cquery));
	}

	public String getInningsDetails(String queryJson) {
		CricketQuery cquery = gson.fromJson(queryJson, CricketQuery.class);

		if (StringUtils.isBlank(cquery.getMatch_Id())) {
			throw new DBAnalyticsException("Invalid Request. Missing Required Parameter \'Match_Id\'");
		}

		return gson.toJson(executorService.getInningsDetails(cquery));
	}

	public TopWinnersListResponse getTopWinnersList(String queryJson) {
		TopWinnersListRequest cquery = gson.fromJson(queryJson, TopWinnersListRequest.class);

		if (StringUtils.isBlank(cquery.getMatchId())) {
			throw new DBAnalyticsException(
					"Invalid Request. Missing Required Parameter to get TopWinners  \'matchId\'");
		}
		// executorService.getTopWinnersList(cquery);
		return executorService.getTopWinnersListWithUserRank(cquery);
	}

	public BidDetailsSummaryResponse getBidDetailsSummary(String queryJson) {
		BidDetailsSummaryRequest cquery = gson.fromJson(queryJson, BidDetailsSummaryRequest.class);

		if (StringUtils.isBlank(cquery.getMatchId()) && StringUtils.isBlank(cquery.getUserId())
				&& StringUtils.isBlank(cquery.getTicketId())) {
			throw new DBAnalyticsException("Invalid Request. Missing Required Parameter to get Bid Details Summary ");
		}

		return executorService.getBidDetailsSummary(cquery);
	}

	public PredictAndWinPostResponse updatedPointsPostAPI(String queryJson) {
		RequestForMatchWinnerPost cquery = gson.fromJson(queryJson, RequestForMatchWinnerPost.class);

		if (StringUtils.isBlank(cquery.getMatchId())) {
			throw new DBAnalyticsException("Invalid Request. Missing matchId for updatedPointsPostAPI ");
		}

		return executorService.updatedPointsPostAPI(cquery);
	}

	public PredictAndWinPostResponse matchWinnersPOSTAPI(String queryJson) {
		RequestForMatchWinnerPost cquery = gson.fromJson(queryJson, RequestForMatchWinnerPost.class);

		if (StringUtils.isBlank(cquery.getMatchId())) {
			throw new DBAnalyticsException("Invalid Request. Missing matchId for matchWinnerPostAPI ");
		}

		return executorService.matchWinnersPOSTAPI(cquery);
	}

	public String getWidgetConfig() {
		return gson.toJson(executorService.getWidgetConfig());
	}

	public List<Map<String, Object>> getCricketTeamStanding(Map<String, Object> requestBody) {
		return executorService.getCricketTeamStanding(requestBody);
	}
	
	public boolean calculateUsersBidRank(String matchId){
		return executorService.calculateUsersBidRank(matchId);
	}
	
	public String lastActiveMatch() {
		return gson.toJson(executorService.getLastActiveMatch());
	}
	
	public String getScoreCardWithPlayersStats(String query) {
		try{
			CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

			if (cquery == null || StringUtils.isBlank(cquery.getMatch_Id())) {
				throw new DBAnalyticsException("Invalid Request. Required Parameter \'Match_Id\' is missing.");
			}

			return gson.toJson(executorService.getScoreCardWithPlayersStats(cquery));
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	public String getPalyersStats(String seriesId, String teamId) {
		try{
			List<Map<String, Object>> playersStatList = executorService.getPalyersStats(seriesId, teamId);
			if(playersStatList.isEmpty()){
				return gson.toJson(playersStatList);
			}else{
				return "No Data Available";
			}
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	public String matchDisabledIds(String matchId) {
		try{
			return gson.toJson(executorService.matchDisabledIds(matchId));
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	public String getSquads(String query) {

		CricketQuery cquery = gson.fromJson(query, CricketQuery.class);

		if (StringUtils.isBlank(query) || gson.fromJson(query, Map.class).isEmpty()) {
			throw new DBAnalyticsException("Invalid Request. Atleast one of \'seriesId\' or 'leagueId' should be present.");
		}

		return gson.toJson(executorService.getSquads(cquery));
	}
}
