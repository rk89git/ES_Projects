package com.db.recommendation.services;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.model.UserPersonalizationQuery;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.DateUtil;
import com.db.common.utils.TimerArrayList;
import com.db.recommendation.model.RecArticle;
import com.db.recommendation.model.RecQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class RecommendationQueryExecutor {

	private Client client = null;

	private static Logger log = LogManager.getLogger(RecommendationQueryExecutor.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private DBConfig config = DBConfig.getInstance();
	
	/**
	 * Map Object to auto expire after one hour
	 */
	private Map<String, TimerArrayList<String>> cacheDefaultRecProductStories = new HashMap<String, TimerArrayList<String>>();
	private Map<String, TimerArrayList<String>> cacheMostViewedStories = new HashMap<String, TimerArrayList<String>>();
	
	Set<String> wordsSet = new HashSet<String>();

	public RecommendationQueryExecutor() {
		initializeClient();
		loadStopwords();
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	/**
	 * Returns related article based on story id
	 * 
	 *
	 * @return
	 */
	private void getRelatedArticleForRecoRecommendation(RecQuery recQuery, Map<String, RecArticle> resultMap) {
		String queryTimeTo = DateUtil.getCurrentDateTime();
		String queryTimeFrom = null;
		long startTime = System.currentTimeMillis();
		GetResponse response = client.prepareGet().setIndex(Indexes.RECOMMENDATION_STORY_DETAIL)
				.setId(recQuery.getStory_id())
				// .setFields(Constants.KEYWORDS,
				// Constants.TITLE,Constants.PEOPLE)
				.execute().actionGet();
		String keyWords = null;
		String title = null;
		List<String> peoples = null;
		if (response.isExists()) {
			Map<String, Object> responseMap = response.getSource();
			keyWords = responseMap.get("keywords").toString();

			title = responseMap.get(Constants.TITLE).toString();
			if ((responseMap.containsKey(Constants.PEOPLE))) {
				peoples = (List<String>) (responseMap.get(Constants.PEOPLE));
			}

			String delimiters = "\\s+|,\\s*|\\.\\s*";
			keyWords = org.springframework.util.StringUtils
					.collectionToCommaDelimitedString(new HashSet<String>(Arrays.asList(keyWords.split(delimiters))));
		} else {
			log.warn("Story not found for id" + recQuery.getStory_id() + "; Result size of related article : "
					+ resultMap.size());
			return;
		}

		if (StringUtils.isBlank(keyWords)) {
			log.warn("Keyword not found for story " + recQuery.getStory_id() + "; Result size of related article : "
					+ resultMap.size());
			return;
		}

		long totalHits = 0;
		try {
			// Bool Query with all conditions
			BoolQueryBuilder boolQueryReco = new BoolQueryBuilder();

			RangeQueryBuilder rangeQuery = null;
			if (ifCustomRuleExistThenSetTime(keyWords) > 0) {
				// In case of custom rule .Time set according to custom rule.
				queryTimeFrom = DateUtil.addHoursToCurrentTime(ifCustomRuleExistThenSetTime(keyWords));
			} else {
				// Time set for last 10 days for all case except custom .
				queryTimeFrom = DateUtil.addHoursToCurrentTime(Constants.RELATED_ARTICLE_TIME_RANGE_IN_HOURS);
			}
			rangeQuery = QueryBuilders.rangeQuery(Constants.PUBLISHED_DATE).from(queryTimeFrom).to(queryTimeTo);
			boolQueryReco.must(rangeQuery)
					.must(new BoolQueryBuilder().should(QueryBuilders.matchQuery("keywords", keyWords))
							// .should(QueryBuilders.termsQuery(Constants.PEOPLE,
							// peoples).boost(5))
							.minimumShouldMatch(1))
					// QueryBuilders.matchQuery("keywords",
					// keyWords).minimumShouldMatch("2"))
					.must(QueryBuilders.termQuery(Constants.BRAND_ID, recQuery.getBrand_id()))
					.mustNot(QueryBuilders.termQuery(Constants.STORY_ID, recQuery.getStory_id()));
			BoolQueryBuilder boolFilter = new BoolQueryBuilder()
										.must(QueryBuilders.termQuery(Constants.BRAND_ID, recQuery.getBrand_id()))
										.must(QueryBuilders.rangeQuery(Constants.PUBLISHED_DATE).from(queryTimeFrom).to(queryTimeTo))
										.mustNot(QueryBuilders.termQuery(Constants.STORY_ID, recQuery.getStory_id()));
	

			BoolQueryBuilder scoreContributorQuery = new BoolQueryBuilder()
					.should(QueryBuilders.matchQuery("keywords", keyWords).minimumShouldMatch("25%"));
			if (peoples.size() > 0) {
				scoreContributorQuery.should(QueryBuilders.termsQuery(Constants.PEOPLE, peoples).boost(2))
						.minimumShouldMatch(1);
			}

			BoolQueryBuilder filteredQueryBuilder = new BoolQueryBuilder().filter(boolFilter).must(scoreContributorQuery);

			// Decay function to change scoring to fetch recent published
			// articles
			DecayFunctionBuilder decayFunctionBuilder = ScoreFunctionBuilders
					.gaussDecayFunction(Constants.PUBLISHED_DATE, "now", "2d","1d",0.5);

			// V0.2: Query with inclusive People boost and recent published
			// articles boosting
			FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(filteredQueryBuilder,decayFunctionBuilder);

			SearchResponse sr = client.prepareSearch(Indexes.RECOMMENDATION_STORY_DETAIL)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
					.setQuery(functionScoreQueryBuilder).setSize(Constants.RELATED_ARTICLE_SIZE).execute().actionGet();

			SearchHit[] searchHits = sr.getHits().getHits();
			totalHits = sr.getHits().getTotalHits();

			for (SearchHit searchHit : searchHits) {
				String jsonSource = searchHit.getSourceAsString();
				RecArticle recArticle = gson.fromJson(jsonSource, RecArticle.class);
				recArticle.setUrl(recArticle.getUrl() + "?ref=rlreco");
				if (!recArticle.getTitle().equals(title))
					resultMap.put(recArticle.getTitle(), recArticle);
			}

		} catch (Exception e) {
			log.error(e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of related article : " + resultMap.size() + ", Keywords of story [" + keyWords
				+ "] , Total Hits: " + totalHits + ", Execution Time (seconds): " + (endTime - startTime) / 1000.0);
	}

	private Integer ifCustomRuleExistThenSetTime(String keywords) {
		int timeForCustomRule = 0;
		List<String> keywordsList = Arrays.asList(keywords.split("\\s*,\\s*"));
		List<String> lowerCaseKeyWordsList = new ArrayList<String>();
		ListIterator<String> iterator = keywordsList.listIterator();
		while (iterator.hasNext()) {
			lowerCaseKeyWordsList.add(iterator.next().toLowerCase());
		}

		for (Map.Entry<String, Integer> entry : Constants.CUSTOM_RULE_MAP.entrySet()) {

			if (lowerCaseKeyWordsList.contains(entry.getKey())) {
				timeForCustomRule = entry.getValue();
				break;

			}
		}
		return timeForCustomRule;

	}

	/**
	 * Returns list of Story Details based on story id
	 * 
	 * @param
	 * @return
	 */
	private void getStoryDetailsFromStoryIds(List<String> stroyIds, Map<String, RecArticle> resultMap, String tracker) {
		RecArticle recArticle = null;
		try {

			MultiGetRequest multiGetRequest = new MultiGetRequest();
			for (String storyId : stroyIds) {
				multiGetRequest.add(Indexes.RECOMMENDATION_STORY_DETAIL, MappingTypes.MAPPING_REALTIME, storyId);
			}
			if (stroyIds.size() > 0) {
				MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();
				for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
					if (multiGetItemResponse.getResponse().isExists()) {
						String jsonSource = multiGetItemResponse.getResponse().getSourceAsString();
						recArticle = gson.fromJson(jsonSource, RecArticle.class);
						recArticle.setUrl(recArticle.getUrl() + tracker);
						resultMap.put(recArticle.getTitle(), recArticle);
					}
				}

			}
		} catch (Exception e) {
			log.error("Error while retrieving story details from storyIds in reco_story_details table ", e);
		}
	}

	/**
	 * 
	 * 
	 * Returns Related Articles list
	 * 
	 * @param
	 * @return
	 */
	public List<RecArticle> getRelatedArticles(RecQuery query) {
		Map<String, RecArticle> resultMap = new LinkedHashMap<>();
		List<RecArticle> result = new ArrayList<RecArticle>();
		long startTime = System.currentTimeMillis();

		try {
			getRelatedArticleForRecoRecommendation(query, resultMap);
		} catch (IndexNotFoundException ime) {
			log.error("Error occured while getting related articles.", ime);
		} catch (Exception e) {
			log.error("Error occured while getting related articles.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Related articles for story id is [" + query.getStory_id() + "] ,brand id is [" + query.getBrand_id()
				+ "] Result size: " + resultMap.size() + ", Execution Time (seconds): "
				+ (endTime - startTime) / 1000.0);
		result.addAll(resultMap.values());
		return result;
	}

	private GetResponse getResultFromUserProfile(String sessionId) {
		GetResponse response = null;
		try {
			response = client.prepareGet().setIndex(Indexes.RECOMMENDATION_USER_PROFILE_STATS).setId(sessionId)
					.execute().actionGet();
		} catch (Exception e) {
			log.error(e);
		}
		return response;
	}

	public List<RecArticle> getUserArticles(RecQuery query) {
		Map<String, RecArticle> resultMap = new LinkedHashMap<>();
		List<RecArticle> result = new ArrayList<RecArticle>();
		long startTime = System.currentTimeMillis();
		List<String> storyIdResult = new ArrayList<String>();
		List<String> readStoryListOfToday = new ArrayList();
		int personalizedV1Size = 0;
		try {

			/**
			 * TODO: Comment below line Remove current day stories in version
			 * 0.2
			 */
			// readStoryListOfToday=getViewdStoriesFromDailyTable(query.getSessionId(),
			// query.getBrand_id());
			// Return Default stories if no session_id
			if (StringUtils.isBlank(query.getSessionId())) {
				fillDefaultStories(query, storyIdResult, null, readStoryListOfToday);
				// storyIdResult = getDefaultRecUserStoryId(query,
				// query.getSize());
			} else if (StringUtils.isNotBlank(query.getSessionId())) {
				List<String> userSectionList = new ArrayList<String>();
				List<String> readStoryListOfLast7days = new ArrayList<String>();
				GetResponse response = getResultFromUserProfile(query.getSessionId());
				/*
				 * response = client.prepareGet().setIndex(Indexes.
				 * RECOMMENDATION_USER_PROFILE_STATS)
				 * .setId(query.getSessionId()).execute().actionGet();
				 */
				// readStories = getStoryIdHistoryOfUser(query.getSession_id(),
				// 15);
				if (!response.isExists()) {
					fillDefaultStories(query, storyIdResult, null, readStoryListOfToday);
				} else {
					if (response.getSource().get(Constants.SECTION) != null) {
						userSectionList = (List<String>) response.getSource().get(Constants.SECTION);
					}
					if (response.getSource().get(Constants.STORY_ID) != null) {
						readStoryListOfLast7days = (List<String>) response.getSource().get(Constants.STORY_ID);
					}
					log.info("User section list for session id " + query.getSessionId() + " is " + userSectionList);
					if (readStoryListOfLast7days.size() >= query.getStoryCountQualifier()) {
						storyIdResult = getUserPersolizedStoryIds(query, query.getSize(), userSectionList,
								readStoryListOfLast7days, readStoryListOfToday);
					}

					personalizedV1Size = storyIdResult.size();
					// log.info("User personalized story ids for session id
					// " + query.getSessionId() + " are"
					// + storyIdResult);
				}

				if (storyIdResult.size() < query.getSize()) {
					fillDefaultStories(query, storyIdResult, readStoryListOfLast7days, readStoryListOfToday);
				}
			}
			getStoryDetailsFromStoryIds(storyIdResult, resultMap, "?ref=ubreco");
		} catch (Exception e) {
			log.error(e);
		}
		long endTime = System.currentTimeMillis();
		log.info("User articles for session id is [" + query.getSessionId() + "] ,brand id is [" + query.getBrand_id()
				+ "]; V1: " + personalizedV1Size + ", Result size: " + resultMap.size() + ", Execution Time (seconds): "
				+ (endTime - startTime) / 1000.0);

		
		Set<RecArticle> uniqueArticles = new LinkedHashSet<>();
	
		// New changes  for Fashion mobile
		if (Constants.FASHION_101_BRAND_ID_ENGLISH.equals(query.getBrand_id())) {
			uniqueArticles.addAll(getMostViewedArticles(query));
		}
		uniqueArticles.addAll(resultMap.values());
		result.addAll(uniqueArticles);
		return result;
	}

	/**
	 * @param query
	 * @param storyIdResult
	 * @param readStoryListOfLastWeek
	 */
	private void fillDefaultStories(RecQuery query, List<String> storyIdResult, List<String> readStoryListOfLastWeek,
			List<String> readStoryListOfToday) {
		List<String> mostViewedStories = getDefaultStoriesFromCache(query);
		int sizeForCacheData = query.getSize() - storyIdResult.size();
		if (mostViewedStories.size() > 0) {
			// Remove stories to avoid duplicacy which are already in result
			mostViewedStories.removeAll(storyIdResult);
			// Remove stories which user has read today
			mostViewedStories.removeAll(readStoryListOfToday);

		}
		if (readStoryListOfLastWeek != null && mostViewedStories.size() > 0) {
			// Remove stories which user has read in last week
			mostViewedStories.removeAll(readStoryListOfLastWeek);
		}
		if (mostViewedStories.size() > 0) {
			if (mostViewedStories.size() > sizeForCacheData)
				storyIdResult.addAll(mostViewedStories.subList(0, sizeForCacheData));
			else
				storyIdResult.addAll(mostViewedStories);

		}
	}

	private List<String> getDefaultStoriesFromCache(RecQuery query) {
		List<String> storyIdsList = new ArrayList<String>();

		// START: Fill cache
		if (cacheDefaultRecProductStories.get(query.getBrand_id()) == null
				|| cacheDefaultRecProductStories.get(query.getBrand_id()).size() == 0) {
			// Fetch extra 20 stories to remove read stories
			int size = query.getSize() + 20;

			log.info("Filling Stories of REC-PROD user articles with new values. Brand:" + query.getBrand_id());
			String queryDate = DateUtil.getCurrentDate();

			String queryTimeTo = DateUtil.getCurrentDateTime();
			String queryTimeFrom = DateUtil
					.addHoursToCurrentTime(Constants.DEFAULT_MOST_TRENDING_ARTICLE_TIME_RANGE_IN_HOUR);
			// Prepare query dynamically based on run time parameters
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			boolQuery.must(QueryBuilders.termQuery(Constants.BRAND_ID, query.getBrand_id()))
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

			SearchResponse sr = client.prepareSearch(Constants.REC_DAILY_LOG_INDEX_PREFIX + queryDate)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
					.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID).size(size))
					.execute().actionGet();

			TimerArrayList<String> tempResultForCache = new TimerArrayList<String>();
			if (sr.getHits().getTotalHits() > 0) {
				Terms result = sr.getAggregations().get("StoryIds");
				// Create a list of size given in input query
				for (Terms.Bucket entry : result.getBuckets()) {
					tempResultForCache.add(entry.getKeyAsString());
				}
			}

			// START: Fail-over Logic to Fill The Stories from rec_story_detail
			// index
			if (tempResultForCache.size() < size) {
				String failOverQueryTimeFor = DateUtil.getCurrentDateTime();
				String failOverQueryTimeFrom = DateUtil
						.addHoursToCurrentTime(Constants.MOST_TRENDING_FAILOVER_ARTICLE_TIME_RANGE_IN_HOUR);
				BoolQueryBuilder boolQueryForFailOver = new BoolQueryBuilder();
				boolQueryForFailOver.must(QueryBuilders.termQuery(Constants.BRAND_ID, query.getBrand_id()))
						.must(QueryBuilders.rangeQuery(Constants.PUBLISHED_DATE).from(failOverQueryTimeFrom)
								.to(failOverQueryTimeFor));

				SearchResponse searchResponseForFailOver = client.prepareSearch(Indexes.RECOMMENDATION_STORY_DETAIL)
						.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000))
						.setQuery(boolQueryForFailOver).setSize(size)
						.addSort(Constants.VIEWS, SortOrder.DESC).execute().actionGet();

				SearchHit[] searchHitsForFailOver = searchResponseForFailOver.getHits().getHits();
				for (SearchHit searchHit : searchHitsForFailOver) {
					if (!searchHit.getSource().containsKey(Constants.STORY_ID)) {
						continue;
					}
					Object obj = searchHit.getSource().get(Constants.STORY_ID);
					if (obj != null) {
						String storyId = (String.valueOf(obj));// .toString()));
						if (StringUtils.isNotBlank(storyId)) {
							tempResultForCache.add((String) searchHit.getSource().get(Constants.STORY_ID));
						}
					}
				}
			}
			// END: Fail-over Logic to Fill The Stories from rec_story_detail
			// index
			cacheDefaultRecProductStories.put(query.getBrand_id(), tempResultForCache);
		}
		storyIdsList.addAll(cacheDefaultRecProductStories.get(query.getBrand_id()));
		// log.info("Result size of most viewed stories: " + storyIdsList.size()
		// + ", Execution Time (seconds): "
		// + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " +
		// storyIdsList);

		return storyIdsList;

	}

	public List<RecArticle> getcollaborativefilteredArticles(RecQuery query) {
		long startTime = System.currentTimeMillis();
		List<String> colloborativeStoryIdsList = new ArrayList<String>();
		Map<String, RecArticle> resultMap = new LinkedHashMap<>();
		List<RecArticle> result2 = new ArrayList<RecArticle>();
		try {
			String queryDate = DateUtil.getCurrentDate();
			// START: Fill cache
			if (query.getBrand_id() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID, query.getStory_id()));
				boolQuery.must(QueryBuilders.termQuery(Constants.BRAND_ID, query.getBrand_id()));

				SearchResponse sr = client.prepareSearch("rec_log2_" + queryDate)
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_DETAIL).setQuery(boolQuery).setSize(0)
						.addAggregation(AggregationBuilders.significantTerms("aggstoryidss").field(Constants.STORY_ID)
								.minDocCount(Constants.COLLOBORATING_MINIMUM_DOC_COUNT).size(query.getSize()))
						.execute().actionGet();
				if (sr.getHits().getTotalHits() > 0) {
					SignificantTerms aggregationResult = sr.getAggregations().get("aggstoryidss");
					for (SignificantTerms.Bucket entry : aggregationResult.getBuckets()) {
						if (!entry.getKeyAsString().equals(query.getStory_id()))
							colloborativeStoryIdsList.add(entry.getKeyAsString());
					}
				}

			}
			getStoryDetailsFromStoryIds(colloborativeStoryIdsList, resultMap, "?ref=ubreco");
		} catch (Exception e) {
			log.error("Error in colloborative story fetching " + e);
		}
		result2.addAll(resultMap.values());

		log.info("Result size of colloborative stories: " + colloborativeStoryIdsList.size()
				+ ", Execution Time (seconds): " + (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: "
				+ colloborativeStoryIdsList);
		return result2;

	}

	private List<String> getUserPersolizedStoryIds(RecQuery query, int size, List<String> userTopViewSection,
			List<String> storyList, List<String> viewedStoryListFromDailyTable) {
		List<String> storyIdsList = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
		try {
			// START: Fill cache
			if (query.getBrand_id() != null && userTopViewSection.size() > 0) {
				String queryDate = DateUtil.getCurrentDate();
				// String lastDate=DateUtil.getPreviousDate();
				String queryTimeTo = DateUtil.getCurrentDateTime();
				String queryTimeFrom = DateUtil
						.addHoursToCurrentTime(Constants.USER_PERSONALIZED_ARTICLE_TIME_RANGE_IN_HOUR);
				// Prepare query dynamically based on run time parameters
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.BRAND_ID, query.getBrand_id()))
						.must(QueryBuilders.termsQuery(Constants.SECTION, userTopViewSection))
						.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(queryTimeFrom).to(queryTimeTo));

				if (storyList.size() > 0) {
					boolQuery.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID, storyList));
				}
				if (viewedStoryListFromDailyTable.size() > 0)
					boolQuery.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID, viewedStoryListFromDailyTable));

				SearchResponse sr = client.prepareSearch(Constants.REC_DAILY_LOG_INDEX_PREFIX + queryDate)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
						.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID).size(size))
						.execute().actionGet();

				// if(sr.getHits().getHits().length >0){
				Terms result = sr.getAggregations().get("StoryIds");
				for (Terms.Bucket entry : result.getBuckets()) {
					storyIdsList.add(entry.getKeyAsString());
				}
				// }
			}

		} catch (Exception e) {
			log.error("Exceeption in fetching data from trending  " + e);
		}
		log.info("Result size of most viewed stories: " + storyIdsList.size() + ", Execution Time (seconds): "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + ", Result: " + storyIdsList);

		return storyIdsList;

	}

	public List<RecArticle> getMostViewedArticles(RecQuery query) {
		List<String> storyIds = null;
		Map<String, RecArticle> resultMap = new LinkedHashMap<>();
		List<RecArticle> results = new ArrayList<>();
		try {
			if (cacheMostViewedStories.get(query.getBrand_id()) == null
					|| cacheMostViewedStories.get(query.getBrand_id()).size() == 0) {
				storyIds = new ArrayList<String>();
				Set<String> uniqueStories = new LinkedHashSet<>();

				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.BRAND_ID, query.getBrand_id()));

				String todayDate = DateUtil.getCurrentDate();
				String yesterdayDate = DateUtil.getPreviousDate();

				SearchResponse sr = client.prepareSearch(Constants.REC_DAILY_LOG_INDEX_PREFIX + todayDate)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
						.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID).size(15))
						.execute().actionGet();

				if (sr.getHits().getTotalHits() > 0) {
					Terms result = sr.getAggregations().get("StoryIds");
					// Create a list of size given in input query
					for (Terms.Bucket entry : result.getBuckets()) {
						uniqueStories.add(entry.getKeyAsString());
					}
				}

				SearchResponse sr1 = client.prepareSearch(Constants.REC_DAILY_LOG_INDEX_PREFIX + yesterdayDate)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
						.addAggregation(AggregationBuilders.terms("StoryIds").field(Constants.STORY_ID).size(40))
						.execute().actionGet();

				if (sr1.getHits().getTotalHits() > 0) {
					Terms result = sr1.getAggregations().get("StoryIds");
					// Create a list of size given in input query
					for (Terms.Bucket entry : result.getBuckets()) {
						uniqueStories.add(entry.getKeyAsString());
					}
				}
				storyIds.addAll(uniqueStories);

			} else {
				storyIds = cacheMostViewedStories.get(query.getBrand_id());
			}
			getStoryDetailsFromStoryIds(storyIds, resultMap, "?ref=mvreco");
			results.addAll(resultMap.values());
		} catch (Exception e) {
			log.error("An error occurred while fetching most viewed stories for brand id: " + query.getBrand_id(), e);
		}
		return results;
	}

	/**
	 * Get recommended stories based on keywords.
	 * 
	 * @param query
	 *            query for getting recommendations
	 * @return list of recommended story ids.
	 */
	public List<String> getKeywordRecommendation(UserPersonalizationQuery query) {

		List<String> divyaHostList = Arrays.asList("5", "7", "20", "21", "22", "23");
		List<String> dbHostList = Arrays.asList("1", "2", "15", "16", "17", "97");
		List<String> result = new ArrayList<>();
		List<String> keywordList = new ArrayList<>();
		String keywords = "";
		String title = "";

		String endDate = DateUtil.getCurrentDateTime();
		String startDate = DateUtil.addHoursToCurrentTime(-168);

		if (query.getKeywords() != null) {
			keywords = query.getKeywords();
		}

		if (StringUtils.isBlank(query.getTitle())) {
			title = getTitleForStoryid(query);
		} else {
			title = query.getTitle();
		}

		title = title + " " + keywords;
		if (title != null && !title.isEmpty()) {

			title = title.replaceAll("[\\p{Punct}\\s]+", " ").trim();

			StringTokenizer tkr = new StringTokenizer(title);
			String word = "";
			while (tkr.hasMoreTokens()) {
				word = tkr.nextToken();
				if (!keywordList.contains(word)) {
					keywordList.add(word);
				}
				keywordList.removeAll(wordsSet);
			}
			BoolQueryBuilder bqb = new BoolQueryBuilder();

			bqb.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(startDate.replaceAll("_", "-"))
					.to(endDate.replaceAll("_", "-")));

			if (query.getDomain_identifier() == 1) {
				bqb.must(QueryBuilders.termsQuery(Constants.HOST, divyaHostList));
			} else {
				bqb.must(QueryBuilders.termsQuery(Constants.HOST, dbHostList));
			}
			if (!keywordList.isEmpty()) {
				bqb.must(QueryBuilders.matchQuery(Constants.TITLE, keywordList));
				bqb.should(QueryBuilders.matchQuery(Constants.OTHER, keywordList));
				bqb.should(QueryBuilders.matchQuery(Constants.KEYWORDS, keywordList));
				bqb.should(QueryBuilders.matchQuery(Constants.ORGANIZATION, keywordList));
				bqb.should(QueryBuilders.matchQuery(Constants.LOCATION, keywordList));
				bqb.should(QueryBuilders.matchQuery(Constants.EVENT, keywordList));
			}
			if (query.getFlag_v() != null) {
				bqb.must(QueryBuilders.termQuery(Constants.FLAG_V, query.getFlag_v()));
			}
			if (query.getCat_id() != null) {
				bqb.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, query.getCat_id()));
			}
			if (!StringUtils.isBlank(query.getStoryid())) {
				bqb.mustNot(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
			}
			int count = 10;
			if (query.getCount() != 0) {
				count = query.getCount();
			}

			SearchResponse res = client.prepareSearch(Indexes.STORY_UNIQUE_DETAIL)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb).setFetchSource(false).setSize(count)
					.execute().actionGet();

			for (SearchHit hit : res.getHits().getHits()) {
				result.add(hit.getId());
			}
			log.info("Result Set: " + result);

		} else {
			log.info("empty Title");
		}
		return result;
	}	
			
	private String getTitleForStoryid(UserPersonalizationQuery query) {
	String title = null;
	GetResponse response = client
			.prepareGet(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME, query.getStoryid())
			.setFetchSource(new String[] { Constants.TITLE, Constants.KEYWORDS }, null).execute().actionGet();
	
	if(response.isExists()) {
		Map<String, Object> source = response.getSourceAsMap();
		title = source.get(Constants.TITLE) + (String) source.get(Constants.KEYWORDS);
	}
	else {
		log.error("No Story Found for storyID:" + query.getStoryid());
		return title;
	}		
	return title;}


	private void loadStopwords() {
		try {
			wordsSet.addAll(Files.readAllLines(Paths.get(config.getProperty("recommendation.stopwords.file")),
					Charset.defaultCharset()));			
			log.info("Successfully loaded stopwords. Dataset Size: " + wordsSet.size());
		} catch (Exception e) {
			log.error("Could not load stopwords file", e);
		}

	}
	public static void main(String[] args) throws Exception {
		System.out.println("###########Starting from main ");
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		RecommendationQueryExecutor queryExecutorService = new RecommendationQueryExecutor();
		// String story_id = "1a95361d-1301-461b-8e85-a3eb4102b7da";
		// String story_id = "1b04d566-20a8-407e-8299-759bcb7eae4a" ;
		// String story_id = "da8e1959-8699-424c-8f5e-2659aff0c0b3";

		// For people testing
		// String story_id = "528f2844-6a8c-4fd5-8da9-b644ffe53c01";

		// String story_id ="bdd9fd56-410a-4cfc-803b-ebfbb0fb032a" ;
		// String brand_id = "REC-FASHION-9069";
		// String session_id ="8bc13d7909ce454e781a03bc3f89bfd8";

		String story_id = "24965da9-7d03-4f79-8d99-f83fbac8c537";
		String brand_id = "REC-DIVYAMARATHI-5483";
		// String story_id = "71b8f585-ff06-4095-8ee3-b36256a85e50";
		// String story_id = "538b9edf-557c-492a-85bd-3376e04821c7" ;
		// String story_id = "b7ea07f1-d921-4e73-8d03-4d44883f0b06";
		// String story_id ="d6594b40-6d74-43ee-8247-4a075bf8a372";
		// String brand_id = "REC-DAILYB-4444";
		// String session_id = "dcb0208b98d53769be8b9f4a372a8c23";
		// String session_id = "975edae96324d2ea7141885dc31bb492";
		// /String session_id = "02c5f0c8fd1ed21c0be27b2075c30ea4"; // my id
		int size = 4;
		//RecQuery query = new RecQuery();
		UserPersonalizationQuery query = new UserPersonalizationQuery();
		query.setTitle("अलवर। रेलवे स्टेशन के पार्सल कार्यालय के पास स्थित साइकिल स्टैंड का ठेका बुधवार को समाप्त हो गया है। रेलवे की  ");
		query.setStoryid("8348262");
		// List<RecArticle> recArticle =
		// queryExecutorService.getRecArticles(query);
		//List<RecArticle> relatedArticles = queryExecutorService.getRelatedArticles(query);
		// List<RecArticle> relatedArticles =
		// queryExecutorService.getUserArticles(query);

		// List<RecArticle> relatedArticles =
		// queryExecutorService.getRelatedArticles(query);

		//System.out.println("$$#####****######" + relatedArticles.size());
		//for (RecArticle article : relatedArticles) {
			//System.out.println(article.getTitle());
			//System.out.println(article.getPublishedDate());
			//System.out.println(article.getStory_id());//}		
		
		System.out.println(" " + gson.toJson(queryExecutorService.getKeywordRecommendation(query)));
		
	}

}
