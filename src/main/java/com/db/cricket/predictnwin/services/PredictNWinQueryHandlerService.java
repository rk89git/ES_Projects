package com.db.cricket.predictnwin.services;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.common.exception.DBAnalyticsException;
import com.db.cricket.model.BidDetailsSummaryRequest;
import com.db.cricket.model.BidDetailsSummaryResponse;
import com.db.cricket.model.CricketQuery;
import com.db.cricket.model.PredictAndWinPostResponse;
import com.db.cricket.model.RequestForMatchWinnerPost;
import com.db.cricket.model.TopWinnersForPredictAndWin;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class PredictNWinQueryHandlerService {

	private Gson gson = new GsonBuilder().create();

	@Autowired
	private PredictNWinQueryExecutorService executor = new PredictNWinQueryExecutorService();

	public String getTransactionHistory(String queryJson) {

		CricketQuery query = gson.fromJson(queryJson, CricketQuery.class);

//		if (StringUtils.isBlank(query.getUserId())) {
//			throw new DBAnalyticsException("Missing User Id");
//		}

		if (query.getSummary()) {
			return gson.toJson(executor.getTransactionSummary(query));
		} else if (query.getGrouped()) {
			return gson.toJson(executor.getTransactionsGrouped(query));
		} else if (query.getTicketSummary()) {
			return gson.toJson(executor.getTransactionsTicketSummary(query));
		}

		return gson.toJson(executor.getTransactionHistory(query));
	}
	
	public void topWinnersForPredictAndWin(String queryJson) {

		TopWinnersForPredictAndWin query = gson.fromJson(queryJson, TopWinnersForPredictAndWin.class);

		if (StringUtils.isBlank(query.getMatchId())) {
			throw new DBAnalyticsException("Missing match Id");
		}
		executor.topWinnersForPredictAndWin(query);
	}
	
//	public TopWinnersListResponse getTopWinnersList(String queryJson) {
//		TopWinnersListRequest cquery = gson.fromJson(queryJson, TopWinnersListRequest.class);
//
//		if (StringUtils.isBlank(cquery.getMatchId())) {
//			throw new DBAnalyticsException(
//					"Invalid Request. Missing Required Parameter to get TopWinners  \'matchId\'");
//		}
//
//		return executor.getTopWinnersList(cquery);
//	}
	
	public BidDetailsSummaryResponse getBidDetailsSummary(String queryJson) {
		BidDetailsSummaryRequest cquery = gson.fromJson(queryJson, BidDetailsSummaryRequest.class);

		if (StringUtils.isBlank(cquery.getMatchId()) && StringUtils.isBlank(cquery.getUserId())
				&& StringUtils.isBlank(cquery.getTicketId())) {
			throw new DBAnalyticsException("Invalid Request. Missing Required Parameter to get Bid Details Summary ");
		}

		return executor.getBidDetailsSummary(cquery);
	}

	public PredictAndWinPostResponse updatedPointsPostAPI(String queryJson) {
		RequestForMatchWinnerPost cquery = gson.fromJson(queryJson, RequestForMatchWinnerPost.class);

		if (StringUtils.isBlank(cquery.getMatchId()))  {
			throw new DBAnalyticsException("Invalid Request. Missing matchId for updatedPointsPostAPI ");
		}
		
		return executor.updatedPointsPostAPI(cquery);
	}
	
	public PredictAndWinPostResponse matchWinnersPOSTAPI(String queryJson) {
		RequestForMatchWinnerPost cquery = gson.fromJson(queryJson, RequestForMatchWinnerPost.class);

		if (StringUtils.isBlank(cquery.getMatchId())) {
			throw new DBAnalyticsException("Invalid Request. Missing matchId for matchWinnerPostAPI ");
		}

		return executor.matchWinnersPOSTAPI(cquery);
	}
	
	public void notifyLazyUsers(String dryRun) {
		try{
			executor.notifyLazyUsers(dryRun);
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
}
