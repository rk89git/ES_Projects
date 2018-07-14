package com.db.comment.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.comment.model.Comment;
import com.db.comment.model.CommentQuery;
import com.db.comment.model.CommentReport;
import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CommentStatus;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.notification.v1.model.NotificationQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class CommentQueryExecutor {

	private static Logger log = LogManager.getLogger(CommentQueryExecutor.class);

	private Client client = ElasticSearchIndexService.getInstance().getClient();

	private Gson gson = new GsonBuilder().serializeNulls().create();
	@Autowired
	ElasticSearchIndexService elasticSearchIndexService;
	
	public Map<String, Object> getComments(CommentQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> fmap = new HashMap<>();
		List<Comment> records = new ArrayList<Comment>();

		int size = query.getSize();
		String currentDateTime = DateUtil.getCurrentDateTime();
		try {
			BoolQueryBuilder fnqb = new BoolQueryBuilder();
			BoolQueryBuilder cqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));

			if (!StringUtils.isBlank(query.getDatetime())) {
				currentDateTime = query.getDatetime();
			}
			if (query.isReply() == true) {
				cqb.must(QueryBuilders.termQuery(Constants.IS_REPLY, true));
				cqb.must(QueryBuilders.termQuery(Constants.IN_REPLY_TO_COMMENT_ID, query.getId()));
			} else {
				if(StringUtils.isNotBlank(query.getPost_id())){
					cqb.must(QueryBuilders.termQuery(Constants.POST_ID, query.getPost_id()));
				}				
				cqb.must(QueryBuilders.termQuery(Constants.IS_REPLY, false));
			}
			cqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lt(currentDateTime));
			fnqb.must(cqb);
			fnqb.mustNot(QueryBuilders.termsQuery(Constants.STATUS, Arrays.asList(CommentStatus.SPAMS, CommentStatus.SOFT_DELETE)));//Filter out Spam comments

			SearchResponse ser = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).setQuery(fnqb).setSize(size).execute()
					.actionGet();			
			long totalCount = ser.getHits().getTotalHits();
			long overAllCount = getTotalCommentCount(query); 

			SearchHit[] searchHits = ser.getHits().getHits();
			for (SearchHit hit : searchHits) {
				Comment realtimeRecord = gson.fromJson(hit.getSourceAsString(), Comment.class);
				if (StringUtils.isNotBlank(realtimeRecord.getModified_description())) {
					realtimeRecord.setDescription(realtimeRecord.getModified_description().trim());
				}
				records.add(realtimeRecord);

			}

			fmap.put("overAllCount", overAllCount);
			fmap.put("totalComment", totalCount);
			fmap.put("comments", records);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getDbComments.", e);
		}
		log.info("Comments returned for post_id " + query.getPost_id() + ", channel_slno " + query.getChannel_slno()
		+ ", comment_id " + query.getId() + " ; Execution Time:(Seconds) "
		+ (System.currentTimeMillis() - startTime) / 1000.0 + " size :" + records.size());
		return fmap;
	}



	public List<CommentReport> getDayWiseReport(CommentQuery query) {
		List<CommentReport> result = new ArrayList<>();
		try {
			if (StringUtils.isBlank(query.getEndDate())) {
				query.setEndDate(DateUtil.getCurrentDate("yyyy-MM-dd"));
			}
			if (StringUtils.isBlank(query.getStartDate())) {
				// set last 15 days
				query.setStartDate(DateUtil.addHoursToCurrentTime(-15 * 24));
			}

			BoolQueryBuilder reportBoolQuery = new BoolQueryBuilder();
			reportBoolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(query.getStartDate())
					.to(query.getEndDate()));
			//reportBoolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno().split(",")));
			SearchResponse sr = client.prepareSearch(Indexes.DB_COMMENT).setQuery(reportBoolQuery).setSize(0)
					.addAggregation(AggregationBuilders.dateHistogram("COMMENT_COUNT").field(Constants.DATE_TIME_FIELD)
							.dateHistogramInterval(DateHistogramInterval.DAY).format("yyyy-MM-dd")
							.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC))
					.execute().actionGet();

			Histogram commentAggregationResult = sr.getAggregations().get("COMMENT_COUNT");
			for (Histogram.Bucket bucket : commentAggregationResult.getBuckets()) {
				CommentReport commentReport = new CommentReport();
				commentReport.setDate(bucket.getKeyAsString());
				commentReport.setCommentCount(bucket.getDocCount());
				result.add(commentReport);
			}

		} catch (Exception e) {
			log.error("Error occured while retreiving comment report.", e);
			throw new DBAnalyticsException("Error occured while retreiving comment report.", e);
		}

		return result;

	}

	public Map<String, Object> getMostEngagedComment(CommentQuery query) {
		long startTime = System.currentTimeMillis();
		int size = query.getSize();
		List<Comment> records = new ArrayList<Comment>();
		Map<String, Object> finalrecords = new HashMap<>();

		try {
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			boolQueryBuilder.must(QueryBuilders.termQuery(Constants.POST_ID, query.getPost_id()));
			boolQueryBuilder.must(QueryBuilders.termQuery(Constants.IS_REPLY, false));
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.STATUS, Arrays.asList(CommentStatus.SPAMS, CommentStatus.SOFT_DELETE)));//Filter out spam comments

			SearchResponse ser = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.addSort(Constants.REPLY_COUNT, SortOrder.DESC).addSort(Constants.LIKE_COUNT, SortOrder.DESC)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).setQuery(boolQueryBuilder).setSize(size)
					.execute().actionGet();

			long totalCount = ser.getHits().getTotalHits();
			long overAllCount = getTotalCommentCount(query);
			SearchHit[] searchHits = ser.getHits().getHits();
			for (SearchHit hit : searchHits) {				
				Comment realtimeRecord = gson.fromJson(hit.getSourceAsString(), Comment.class);
				realtimeRecord.setDescription(realtimeRecord.getModified_description());
				records.add(realtimeRecord);
			}
			finalrecords.put("overAllCount", overAllCount);
			finalrecords.put("totalComment", totalCount);
			finalrecords.put("comments", records);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getMostEngagedComments.", e);
		}
		log.info("Comments returned for post_id " + query.getPost_id() + ", channel_slno " + query.getChannel_slno()
		+ ", Execution Time:(Seconds) " + (System.currentTimeMillis() - startTime) / 1000.0 + "size"
		+ finalrecords.size());
		return finalrecords;
	}

	public Map<String, Object> getMostEngagedArticle(CommentQuery query) {

		long startTime = System.currentTimeMillis();
		Map<String, Object> finalrecords = new HashMap<>();
		try {
			String startDate = query.getStartDate();
			if (StringUtils.isBlank(query.getEndDate())) {
				startDate = DateUtil.addHoursToCurrentTime(-24);
			}
			String endDate = query.getEndDate();
			if (StringUtils.isBlank(query.getEndDate())) {
				endDate = DateUtil.getCurrentDateTime();
			}

			int size = query.getSize();

			String include[] = { Constants.URL, Constants.POST_ID };

			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
					.must(QueryBuilders.termQuery(Constants.HOST, query.getHost()));
			boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(startDate).to(endDate));
			boolQueryBuilder.mustNot(QueryBuilders.termsQuery(Constants.STATUS, Arrays.asList(CommentStatus.SPAMS, CommentStatus.SOFT_DELETE)));//Filter out Spam comments;

			SearchResponse searchResponse = client.prepareSearch(Indexes.DB_COMMENT)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQueryBuilder).setSize(size)
					.addAggregation(
							AggregationBuilders.terms("post_id").field(Constants.POST_ID).size(1).subAggregation(
									AggregationBuilders.topHits("top").fetchSource(include, new String[] {}).size(1)))
					.execute().actionGet();

			Terms commentsAggration = searchResponse.getAggregations().get("post_id");
			for (Terms.Bucket commentBucket : commentsAggration.getBuckets()) {
				TopHits topHits = commentBucket.getAggregations().get("top");
				long totalCount = topHits.getHits().getTotalHits();
				finalrecords.put("totalComment : ", totalCount);
				finalrecords.put("url : ",topHits.getHits().getHits()[0].getSource().get(Constants.URL).toString());
				finalrecords.put("post_id: ",topHits.getHits().getHits()[0].getSource().get(Constants.POST_ID).toString());
			}
		}catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getMostEngagedArticle.", e);
		}
		log.info("Comments returned for host " + query.getHost()+ ", channel_slno " + query.getChannel_slno()
		+ ", Execution Time:(Seconds) " + (System.currentTimeMillis() - startTime) / 1000.0 + "size"
		+ finalrecords.size());
		return finalrecords;
	}


	public Map<String, Object> getTopReviews(NotificationQuery query) {
		Map<String,Object> response=new HashMap<>();
		QueryBuilder queryBuilder=query.getQueryBuilder();
		SearchResponse searchResponse=elasticSearchIndexService.getSearchResponse(Indexes.DB_COMMENT,MappingTypes.MAPPING_REALTIME,queryBuilder,query.getFrom(),query.getSize(),query.getSort(),null,null,null,null);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		List<Map<String,Object>> reviews = Arrays.stream(searchHits).map(searchHit->searchHit.getSource()).collect(Collectors.toList());
		response.put("reviews",reviews);
		return response;
	}

	public Map<String, Object> getRecentReviews(NotificationQuery query) {
		Map<String,Object> response=new HashMap<>();
		QueryBuilder queryBuilder=query.getQueryBuilder();
		SearchResponse searchResponse=elasticSearchIndexService.getSearchResponse(Indexes.DB_COMMENT,MappingTypes.MAPPING_REALTIME,queryBuilder,query.getFrom(),query.getSize(),query.getSort(),null,null,null,null);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		List<Map<String,Object>> reviews = Arrays.stream(searchHits).map(searchHit->searchHit.getSource()).collect(Collectors.toList());
		response.put("reviews",reviews);
		return response;
	}


	public Map<String, Object> getCommentsById(CommentQuery query) {

		long startTime = System.currentTimeMillis();
		Map<String, Object> finalMap = new HashMap<>();
		List<Comment> records = new ArrayList<Comment>();
		int size = query.getSize();
		String currentDateTime = DateUtil.getCurrentDateTime();

		try {
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
			boolQueryBuilder.must(QueryBuilders.constantScoreQuery(
					QueryBuilders.boolQuery().must(QueryBuilders.termQuery(Constants.POST_ID, query.getPost_id()))
					.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lt(currentDateTime))	
					.must(QueryBuilders.termQuery(Constants.IS_REPLY, false))))
					.mustNot(QueryBuilders.termsQuery(Constants.STATUS, Arrays.asList(CommentStatus.SPAMS, CommentStatus.SOFT_DELETE)));//Filter out spam comments
			boolQueryBuilder.should(QueryBuilders.matchQuery(Constants.ID, query.getId()));			

			SearchResponse ser = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.addSort(Constants._SCORE, SortOrder.DESC).addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC)
					.setQuery(boolQueryBuilder).setSize(size).execute().actionGet();

			long totalCount = ser.getHits().getTotalHits();
			long overAllCount = getTotalCommentCount(query);
			SearchHit[] searchHits = ser.getHits().getHits();
			for (SearchHit hit : searchHits) {				
				Comment realtimeRecord = gson.fromJson(hit.getSourceAsString(), Comment.class);
				if (StringUtils.isNotBlank(realtimeRecord.getModified_description())) {
					realtimeRecord.setDescription(realtimeRecord.getModified_description().trim());
				}
				records.add(realtimeRecord);
			}
			finalMap.put("overAllCount", overAllCount);
			finalMap.put("totalComment", totalCount);
			finalMap.put("comments", records);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getCommentsThroughNotification.", e);
		}
		log.info("Comments returned for post_id " + query.getPost_id() + ", channel_slno " + query.getChannel_slno()
		+ ", comment_id " + query.getId() + " ; Execution Time:(Seconds) "
		+ (System.currentTimeMillis() - startTime) / 1000.0 + "size" + records.size());
		return finalMap;
	}


	public Map<String, Object> getAgreeOrDisagreeComments(CommentQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> finalMap = new HashMap<>();
		String currentDateTime = DateUtil.getCurrentDateTime();
		int size = query.getSize();
		Boolean value = query.isAgree();		
		if (!StringUtils.isBlank(query.getDatetime())) { currentDateTime =
				query.getDatetime(); 
		}		 
		try {
			BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
			boolQueryBuilder.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			boolQueryBuilder.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lt(currentDateTime));

			if (StringUtils.isNotBlank(query.getPost_id())) {
				boolQueryBuilder.must(QueryBuilders.termQuery(Constants.POST_ID, query.getPost_id()));
			}
			if ((value != null) && (value == true || value == false)) {
				boolQueryBuilder.must(QueryBuilders.termQuery(Constants.ISAGREE, value));
			}

			SearchResponse searchResponse = client.prepareSearch(Indexes.DB_COMMENT)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQueryBuilder)
					.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).setSize(0)
					.addAggregation(
							AggregationBuilders.terms("isAgree").field(Constants.ISAGREE)
							.subAggregation(AggregationBuilders.topHits("top")
									.sort(Constants.DATE_TIME_FIELD, SortOrder.DESC).size(size)))
					.execute().actionGet();

			Terms isAgreeAggration = searchResponse.getAggregations().get("isAgree");

			ArrayList<Map<String, Object>> agreeList = new ArrayList<Map<String, Object>>();
			ArrayList<Map<String, Object>> disagreeList = new ArrayList<Map<String, Object>>();

			for (Terms.Bucket isAgreeBucket : isAgreeAggration.getBuckets()) {
				String key = isAgreeBucket.getKeyAsString();
				TopHits topHits = isAgreeBucket.getAggregations().get("top");
				SearchHit[] searchHits = topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					Map<String, Object> decesionMap = hit.getSource();
					switch (key) {
					case "true":
						agreeList.add(decesionMap);
						break;
					case "false":
						disagreeList.add(decesionMap);
						break;
					}
				}
			}
			finalMap.put("agree", agreeList);
			finalMap.put("disagree", disagreeList);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getAgreeOrDisagreeComments.", e);
		}
		log.info("Comments returned for post_id " + query.getPost_id() + ", channel_slno " + query.getChannel_slno()
		+ ", comment_id " + query.getId() + " ; Execution Time:(Seconds) "
		+ (System.currentTimeMillis() - startTime) / 1000.0 + "size" + finalMap.size());
		return finalMap;
	}

	private long getTotalCommentCount(CommentQuery query) {

		BoolQueryBuilder cqb = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		if (StringUtils.isNotBlank(query.getPost_id())) {
			cqb.must(QueryBuilders.termQuery(Constants.POST_ID, query.getPost_id()));
		}
		SearchResponse ser = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
				.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).setQuery(cqb).execute().actionGet();
		long totalCount = ser.getHits().getTotalHits();
		return totalCount;
	}

	public static void main(String[] args) throws Exception {
		Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
		CommentQueryExecutor cqes = new CommentQueryExecutor();
		CommentQuery query = new CommentQuery();

		query.setPost_id("-121923801");
		query.setChannel_slno("521,960");
		//query.setStoryid("2543162786");
		query.setSize(5);
		//query.setAgree(false);
		//query.setHost("1");
		//query.setSortField(Constants.REPLY_COUNT);
		query.setId("1b47001f9f13ab469b395d721946cb05");
		//query.setDatetime("2017-11-02T14:34:55Z");
		query.setReply(false);
		query.setStartDate("2017-11-18");
		query.setEndDate("2017-11-24");
		System.out.println(" " + gson.toJson(cqes.getDayWiseReport(query)));

	}
	
	public String pushSpamWords(Map<String, Object> jsonObject) throws Exception{
		try {
			long status_flag = Constants.CommentStatus.SPAMS;
			boolean isEnable = true;
			List<Map<String, Object>> spamList = new ArrayList<Map<String, Object>>();

			if(jsonObject.containsKey(Constants.Comment.SPAMS)){
				@SuppressWarnings("unchecked")
				List<String> spamWords = (List<String>)jsonObject.get(Constants.Comment.SPAMS);

				if(jsonObject.containsKey(Constants.Comment.SPAM_WORD_STATUS)){
					status_flag = (int) jsonObject.get(Constants.Comment.SPAM_WORD_STATUS);
				}

				if(jsonObject.containsKey(Constants.Comment.SPAM_WORD_ISENABLE)){
					isEnable = Boolean.parseBoolean(jsonObject.get(Constants.Comment.SPAM_WORD_ISENABLE).toString());
				}

				for(String spamWord: spamWords){
					Map<String, Object> spamData = new HashMap<String, Object>();

					spamData.put(Constants.Comment.SPAM_WORD_STATUS, status_flag);
					spamData.put(Constants.Comment.SPAM_WORD, spamWord.toLowerCase());
					spamData.put(Constants.Comment.SPAM_WORD_ISENABLE, isEnable);
					spamData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					spamData.put(Constants.ROWID, spamWord.hashCode()+"");

					spamList.add(spamData);
				}

				elasticSearchIndexService.indexOrUpdate(Indexes.COMMENT_SPAM_WORDS, MappingTypes.MAPPING_REALTIME, spamList);
				
				log.info("Pushed Spam words successfully");

			}else{
				log.info("No spam words found");
			}
			return "Spam words added successfully";
		} catch (Exception e) {
			log.error("Error while pushing spam words");
			throw new RuntimeException(e);
		}
	}
	
	public String deleteComment(Map<String, Object> jsonObject){
		
		if(jsonObject.containsKey("id")){
			Map<String, Object> record = new HashMap<>();
			
			record.put(Constants.ROWID, jsonObject.get("id"));
			record.put(Constants.STATUS, CommentStatus.SOFT_DELETE);
			
			elasticSearchIndexService.indexOrUpdate(Indexes.DB_COMMENT, MappingTypes.MAPPING_REALTIME, record);
			
			log.info("Comment status successfully updated as soft delete for id "+ jsonObject.get("id"));
			
			BoolQueryBuilder cqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.ID, jsonObject.get("id")))
					.must(QueryBuilders.termQuery(Constants.IS_REPLY, true));
			
			SearchResponse ser = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(cqb).execute().actionGet();

			SearchHit[] searchHits = ser.getHits().getHits();
			for (SearchHit hit : searchHits) {		
				Map<String, Object> commentSource = (Map<String, Object>)hit.getSourceAsMap();
				
				if(commentSource.get(Constants.IN_REPLY_TO_COMMENT_ID) != null && StringUtils.isNotBlank((String)commentSource.get(Constants.IN_REPLY_TO_COMMENT_ID))){
					String parentCommentId = commentSource.get(Constants.IN_REPLY_TO_COMMENT_ID).toString();
					
					Map<String, Map<String, Integer>> decrementCounterMap = new HashMap<>();
					Map<String, Integer> decrementMap = new HashMap<>();
					
					decrementMap.put(Constants.REPLY_COUNT, -1);
					
					decrementCounterMap.put(parentCommentId, decrementMap);
					
					elasticSearchIndexService.incrementCounter(Indexes.DB_COMMENT, MappingTypes.MAPPING_REALTIME, decrementCounterMap);
					
					log.info("Reply Count successfully updated in comment parent id "+ parentCommentId+ " for comment id "+jsonObject.get("id"));
				}
			}
			
			return "Successfully deleted";
		}else{
			throw new DBAnalyticsException("Comment Id not found");
		}
	}
	
	public String deleteUser(Map<String, Object> jsonObject){
		
		if(jsonObject.containsKey("id")){
			Map<String, Object> record = new HashMap<>();
			
			record.put(Constants.ROWID, jsonObject.get("id"));
			record.put(Constants.STATUS, CommentStatus.SOFT_DELETE);
			
			elasticSearchIndexService.indexOrUpdate(Indexes.DB_USER, MappingTypes.MAPPING_REALTIME, record);
			
			return "Successfully deleted";
		}else{
			throw new DBAnalyticsException("User Id not found");
		}
	}
}
