package com.db.cricket.predictnwin.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.Cricket.PredictWinConstants;
import com.db.common.constants.Constants.NotificationConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.cricket.model.BidDetailsSummaryRequest;
import com.db.cricket.model.BidDetailsSummaryResponse;
import com.db.cricket.model.CricketQuery;
import com.db.cricket.model.MatchWinnerDetailPostRequest;
import com.db.cricket.model.PredictAndWinPostResponse;
import com.db.cricket.model.RequestForMatchWinnerPost;
import com.db.cricket.model.TicketIdAndGainPointResponse;
import com.db.cricket.model.TicketIdAndGainPointResponseList;
import com.db.cricket.model.TopWinnersForPredictAndWin;
import com.db.cricket.predictnwin.constants.PredictAndWinIndexes;
import com.db.cricket.services.CricketNotificationService;
import com.db.cricket.utils.CricketUtils;
import com.db.cricket.utils.LanguageUtils;
import com.google.gson.GsonBuilder;

@Service
public class PredictNWinQueryExecutorService {

	private static final Logger LOG = LogManager.getLogger(PredictNWinQueryExecutorService.class);

	ElasticSearchIndexService indexService = ElasticSearchIndexService.getInstance();

	private LanguageUtils langUtil = LanguageUtils.getInstance();

	private Client client = null;
	
	@Autowired
	private CricketNotificationService cricketNotificationHelper = new CricketNotificationService();

	/**
	 * Base Constructor. Initialize based on defaults.
	 */
	public PredictNWinQueryExecutorService() {
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
	 * Get a users transaction history.
	 * 
	 * @param query
	 * @return
	 */
	public List<Map<String, Object>> getTransactionHistory(CricketQuery query) {

		long startTime = System.currentTimeMillis();

		BoolQueryBuilder txQuery = new BoolQueryBuilder();

		if (StringUtils.isNotBlank(query.getMatch_Id())) {
			txQuery.must(QueryBuilders.termQuery("matchId", query.getMatch_Id()));
		}

		if (StringUtils.isNotBlank(query.getUserId())) {
			txQuery.must(QueryBuilders.termQuery("userId", query.getUserId()));
		}

		if (StringUtils.isNotBlank(query.getTicketId())) {
			txQuery.must(QueryBuilders.termQuery("ticketId", query.getTicketId()));
		}

//		txQuery.must(QueryBuilders.termQuery("isBidAccepted", true));

		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(txQuery).execute().actionGet();

		List<Map<String, Object>> transactions = new ArrayList<>();

		for (SearchHit hit : response.getHits().getHits()) {
			Map<String, Object> transaction = hit.getSourceAsMap();
			transactions.add(transaction);
		}

		if (StringUtils.isNotBlank(query.getLang())) {
			langUtil.bidsInLanguage(transactions, query.getLang());
		}

		LOG.info("Query Execution Time: " + (System.currentTimeMillis() - startTime) + "ms.");
		return transactions;
	}

	public List<Object> getTransactionSummary(CricketQuery query) {
		List<Object> summary = new ArrayList<>();

		Map<String, Map<String, Object>> matchWiseSummary = new TreeMap<>();

		/*
		 * Get the sum of coins gained and coins bid for the given user.
		 */
		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setQuery(QueryBuilders.termQuery("userId", query.getUserId())).setTypes(MappingTypes.MAPPING_REALTIME)
				.addAggregation(AggregationBuilders.terms("matchId").field("matchId")
						.subAggregation(AggregationBuilders.sum("bid").field("coinsBid"))
						.subAggregation(AggregationBuilders.sum("gain").field("coinsGained")).size(60))
				.execute().actionGet();

		Terms terms = response.getAggregations().get("matchId");

		/*
		 * Build a match id to summary record map.
		 */
		for (Terms.Bucket bucket : terms.getBuckets()) {
			String matchId = bucket.getKeyAsString();

			Map<String, Object> record = new HashMap<>();
			record.put("matchId", matchId);
			record.put("coinsBid", ((Sum) bucket.getAggregations().get("bid")).getValue());
			record.put("coinsGained", ((Sum) bucket.getAggregations().get("gain")).getValue());

			matchWiseSummary.put(matchId, record);
		}

		MultiGetResponse matchDetails = client.prepareMultiGet()
				.add(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME, matchWiseSummary.keySet()).execute()
				.actionGet();

		/*
		 * For every match id, populate the match number and team names
		 */
		matchDetails.forEach(e -> {
			if (e.getResponse().isExists()) {
				Map<String, Object> record = matchWiseSummary.get(e.getId());
				Map<String, Object> source = e.getResponse().getSourceAsMap();
				record.put("matchnumber", source.get("matchnumber"));
				record.put("match", source.get("teama_short") + " vs " + source.get("teamb_short"));
			}
		});

		/*
		 * If Summary is not empty, return the list of records.
		 */
		if (!matchWiseSummary.isEmpty()) {
			summary = new ArrayList<>(matchWiseSummary.values());
		}

		return summary;
	}

	@SuppressWarnings("unchecked")
	public Object getTransactionsGrouped(CricketQuery query) {
		BoolQueryBuilder builder = QueryBuilders.boolQuery();

		if (StringUtils.isNotBlank(query.getUserId())) {
			builder.must(QueryBuilders.termQuery("userId", query.getUserId()));
		}

		if (StringUtils.isNotBlank(query.getMatch_Id())) {
			builder.must(QueryBuilders.termQuery("matchId", query.getMatch_Id()));
		}

//		builder.must(QueryBuilders.termQuery("isBidAccepted", true));

		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).setSize(500).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();

		Map<String, Map<String, Map<String, Object>>> formattedMap = new HashMap<>();

		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();

			if (formattedMap.containsKey(source.get("bidType"))) {
				Map<String, Map<String, Object>> recordMap = formattedMap.get(source.get("bidType"));

				if (recordMap.containsKey(source.get("bidTypeId"))) {
					Map<String, Object> record = recordMap.get(source.get("bidTypeId"));
					record.put("count", (Integer) record.get("count") + 1);
					((List<Object>) record.get("data")).add(source);
				} else {
					List<Object> list = new ArrayList<>();
					Map<String, Object> record = new HashMap<>();
					record.put("count", 1);
					list.add(source);
					record.put("data", list);
					recordMap.put(source.get("bidTypeId").toString(), record);
				}
			} else {

				Map<String, Map<String, Object>> recordMap = new HashMap<>();
				List<Object> list = new ArrayList<>();
				Map<String, Object> record = new HashMap<>();
				record.put("count", 1);
				list.add(source);
				record.put("data", list);
				recordMap.put(source.get("bidTypeId").toString(), record);
				formattedMap.put((String) source.get("bidType"), recordMap);
			}

		}

		return formattedMap;
	}

	public Object getTransactionsTicketSummary(CricketQuery query) {
		List<Object> summary = new ArrayList<>();

		Map<String, Map<String, Object>> matchTicketSummary = new TreeMap<>();

		BoolQueryBuilder builder = QueryBuilders.boolQuery();

		if (StringUtils.isNotBlank(query.getUserId())) {
			builder.must(QueryBuilders.termQuery("userId", query.getUserId()));
		}

		if (StringUtils.isNotBlank(query.getMatch_Id())) {
			builder.must(QueryBuilders.termQuery("matchId", query.getMatch_Id()));
		}

		/*
		 * Get the sum of coins gained and coins bid for the given user.
		 */
		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setQuery(QueryBuilders.termQuery("userId", query.getUserId())).setTypes(MappingTypes.MAPPING_REALTIME)
				.addAggregation(AggregationBuilders.terms("ticketId").field("ticketId")
						.subAggregation(AggregationBuilders.sum("bid").field("coinsBid"))
						.subAggregation(AggregationBuilders.sum("gain").field("coinsGained")).size(500)
						.order(Terms.Order.term(true)))
				.execute().actionGet();

		Terms terms = response.getAggregations().get("ticketId");

		/*
		 * Build a match id to summary record map.
		 */
		for (Terms.Bucket bucket : terms.getBuckets()) {
			String matchId = bucket.getKeyAsString();

			Map<String, Object> record = new HashMap<>();
			record.put("ticketId", matchId);
			record.put("coinsBid", ((Sum) bucket.getAggregations().get("bid")).getValue());
			record.put("coinsGained", ((Sum) bucket.getAggregations().get("gain")).getValue());

			matchTicketSummary.put(matchId, record);
		}

		MultiGetResponse matchDetails = client.prepareMultiGet()
				.add(Indexes.CRICKET_SCHEDULE, MappingTypes.MAPPING_REALTIME, matchTicketSummary.keySet()).execute()
				.actionGet();

		/*
		 * For every match id, populate the match number and team names
		 */
		matchDetails.forEach(e -> {
			if (e.getResponse().isExists()) {
				Map<String, Object> record = matchTicketSummary.get(e.getId());
				Map<String, Object> source = e.getResponse().getSourceAsMap();
				record.put("matchnumber", source.get("matchnumber"));
				record.put("match", source.get("teama_short") + " vs " + source.get("teamb_short"));
			}
		});

		/*
		 * If Summary is not empty, return the list of records.
		 */
		if (!matchTicketSummary.isEmpty()) {
			summary = new ArrayList<>(matchTicketSummary.values());
		}

		return summary;
	}

	public void topWinnersForPredictAndWin(TopWinnersForPredictAndWin topWinnersForPredictAndWin) {
		try {
			long startTime = 0;
			String matchId = topWinnersForPredictAndWin.getMatchId();
			boolean isBidAccepted = topWinnersForPredictAndWin.getIsBidAccepted();
			List<Map<String, Object>> records = new ArrayList<>();
			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.termQuery("matchId", matchId));
			qb.must(QueryBuilders.termQuery("isBidAccepted", isBidAccepted));
			qb.must(QueryBuilders.termQuery("isWin", true));

			SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("userId").field("userId").size(10)
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
				map.put("totalCoinsGained", coinsGainedValue);
				map.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				records.add(map);
				rank++;
			}

			int updatedRecords = indexService.indexOrUpdate(PredictAndWinIndexes.USER_WINNERS_LIST,
					MappingTypes.MAPPING_REALTIME, records);
			long endTime = System.currentTimeMillis();
			LOG.info("Updated top winners records " + updatedRecords + ". Execution time(Seconds): "
					+ (endTime - startTime) / (1000.0));
		} catch (Exception e) {
			LOG.error("Exception in calculating top winners for predict and winners.", e);
		}

	}

	// public TopWinnersListResponse getTopWinnersList(TopWinnersListRequest query)
	// {
	// long startTime = System.currentTimeMillis();
	// TopWinnersListResponse topWinnersListResponse = new TopWinnersListResponse();
	// List<TopWinnersResponse> topWinnersList = new
	// ArrayList<TopWinnersResponse>();
	// List<LuckyWinnersResponse> luckyWinnersList = new
	// ArrayList<LuckyWinnersResponse>();
	// SearchResponse response =
	// client.prepareSearch(PredictAndWinIndexes.USER_WINNERS_LIST)
	// .setTypes(MappingTypes.MAPPING_REALTIME)
	// .setQuery(QueryBuilders.termQuery("matchId",
	// query.getMatchId())).setSize(11).execute().actionGet();
	//
	// SearchHit[] hits = response.getHits().getHits();
	//
	// if (response.getHits().getHits() != null &&
	// response.getHits().getHits().length > 0) {
	// for (SearchHit hit : response.getHits().getHits()) {
	// String userId = hit.getSourceAsMap().get("userId").toString();
	// double totalCoinsGained =
	// Double.valueOf(hit.getSourceAsMap().get("totalCoinsGained").toString());
	// int rank = Integer.valueOf(hit.getSourceAsMap().get("rank").toString());
	// TopWinnersResponse topWinner = new TopWinnersResponse();
	// topWinner.setUserId(userId);
	// topWinner.setTotalCoinsGained(totalCoinsGained);
	// topWinner.setRank(rank);
	// topWinnersList.add(topWinner);
	//
	// LuckyWinnersResponse luckyWinner = new LuckyWinnersResponse();
	// luckyWinner.setUserId(userId);
	// luckyWinner.setTotalCoinsGained(totalCoinsGained);
	// luckyWinner.setRank(rank);
	// luckyWinnersList.add(luckyWinner);
	//
	// }
	// }
	// topWinnersListResponse.setLuckyWinners(luckyWinnersList);
	// topWinnersListResponse.setTopWinners(topWinnersList);
	//
	// long endTime = System.currentTimeMillis();
	// LOG.info("Top winners fetching Query execution took: " + (endTime -
	// startTime) + " ms.");
	// return topWinnersListResponse;
	// }

	public BidDetailsSummaryResponse getBidDetailsSummary(BidDetailsSummaryRequest query) {
		long startTime = System.currentTimeMillis();

		BoolQueryBuilder builder = new BoolQueryBuilder();
		builder.must(QueryBuilders.termQuery("matchId", query.getMatchId()))
				.must(QueryBuilders.termQuery("ticketId", query.getTicketId()))
				.must(QueryBuilders.termQuery("userId", query.getUserId()));

		SearchResponse response = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(builder).setSize(11).execute().actionGet();

		SearchHit[] hits = response.getHits().getHits();
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
				bidsDetailsSummary.setBidType(bidTypeId);
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
		try {
			SearchResponse srb1 = client.prepareSearch(PredictAndWinIndexes.USERS_BIDS)
					.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("userId").field("userId")
							.subAggregation(AggregationBuilders.topHits("topStories").fetchSource(includes, excludes)
									.size(300).sort("datetime", SortOrder.DESC)))
					.execute().actionGet();

			Terms terms = srb1.getAggregations().get("userId");
			for (Terms.Bucket entry : terms.getBuckets()) {
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

			postResponse = CricketUtils.getBidPointsUpdated(serviceDataMap);
		} catch (Exception e) {
			LOG.info("###Exception in bids poins updation for predict and win  " + e.getMessage());
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
			LOG.info("Exception in match winners POST API  ");
		}
		return postResponse;
	}
	
	public void notifyLazyUsers(String dryRun){
		try{
			
			LOG.info("Sending Lazy Ntoification to users");
			
			Set<String> activeUsersSet = getActiveUsers();

			Map<String, Set<String>> notificationTargetMap = new HashMap<>();

			BoolQueryBuilder qb = new BoolQueryBuilder();

			qb.must(QueryBuilders.termQuery(Constants.NOTIFICATION_STATUS, Constants.NOTIFICATION_ON));

			SearchResponse searchResponse = client.prepareSearch(Indexes.USER_REGISTRATION)
					.setScroll(new TimeValue(60000)).setQuery(qb).setSize(1000).execute().actionGet();
			
			Map<String, Object> notificationUtil = new HashMap<>();
			
			notificationUtil.put(NotificationConstants.SLID_KEY, PredictWinConstants.LAZY_BID_NOTIFICATION);
			notificationUtil.put(NotificationConstants.AUTO_NOTIFICATION_KEY,  DateUtil.getCurrentDateTime());
			notificationUtil.put(NotificationConstants.CAT_ID, 8);
			notificationUtil.put(NotificationConstants.MESSAGE, PredictWinConstants.LAZY_NOTIFICATION_MESSAGE);

			if(Boolean.parseBoolean(dryRun)){
				notificationUtil.put(NotificationConstants.DRY_RUN, "true");
			}

			do {
				for (SearchHit hit : searchResponse.getHits().getHits()) {
					Map<String, Object> userDetails = hit.getSourceAsMap();

					String userId = (String)userDetails.get("uniqueUserId");

					if(!activeUsersSet.contains(userId) && StringUtils.isNotBlank((String)userDetails.get("vendorId"))){
						String vendorId = (String)userDetails.get("vendorId");

						if(!notificationTargetMap.containsKey(vendorId)){
							notificationTargetMap.put(vendorId, new HashSet<String>());
						}

						notificationTargetMap.get(vendorId).add(userId);
					}
				}

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();

				cricketNotificationHelper.sendBidWinNotifications(notificationTargetMap, notificationUtil);
				notificationTargetMap.clear();

			} while (searchResponse.getHits().getHits().length != 0);

		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}

	}
	
	private Set<String> getActiveUsers(){
		try{
			Set<String> activeUsersSet = new HashSet<>();

			BoolQueryBuilder qb = new BoolQueryBuilder();

			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(DateUtil.getPreviousDate()).lte(DateUtil.getCurrentDate())
					.format("yyyy_MM_dd"));

			SearchResponse searchResponse =  client.prepareSearch(PredictAndWinIndexes.USERS_BIDS).setScroll(new TimeValue(60000))
					.setQuery(qb).setSize(1000).execute().actionGet();

			do {
				for (SearchHit hit : searchResponse.getHits().getHits()) {
					Map<String, Object> usersBidDetails = hit.getSourceAsMap();
					
					if(usersBidDetails.containsKey("userId")){
						activeUsersSet.add((String)usersBidDetails.get("userId"));
					}
				}

				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();

			} while (searchResponse.getHits().getHits().length != 0);

			return activeUsersSet;
	
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}

	public static void main(String[] args) {
		/*CricketQuery query = new CricketQuery();
		query.setMatch_Id("1234");
		query.setUserId("38");
		query.setTicketSummary(true);
		System.out.println(new GsonBuilder().create()
				.toJson(new PredictNWinQueryExecutorService().getTransactionsTicketSummary(query)));*/
		
		new PredictNWinQueryExecutorService().notifyLazyUsers("false");
	}
}
