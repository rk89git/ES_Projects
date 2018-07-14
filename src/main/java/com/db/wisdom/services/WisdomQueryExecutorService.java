

package com.db.wisdom.services;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.db.comment.model.Comment;
import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants.SessionTypeConstants;
import com.db.common.constants.Constants.HostType;
import com.db.common.constants.Constants.VideoReportConstants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.GenericQuery;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.wisdom.model.AdMetrics;
import com.db.wisdom.model.AuditSiteModel;
import com.db.wisdom.model.Competitor;
import com.db.wisdom.model.CompetitorStory;
import com.db.wisdom.model.EODFlickerDetail;
import com.db.wisdom.model.FacebookInsights;
import com.db.wisdom.model.FacebookInsightsPage;
import com.db.wisdom.model.FacebookPageInsights;
import com.db.wisdom.model.FrequencyDetailResponse;
import com.db.wisdom.model.FrequencyDetailResponse.Category;
import com.db.wisdom.model.FrequencyDetailResponse.Source;
import com.db.wisdom.model.KRAreport;
import com.db.wisdom.model.SlideDetail;
import com.db.wisdom.model.SourcePerformance;
import com.db.wisdom.model.StoryDetail;
import com.db.wisdom.model.StoryPerformance;
import com.db.wisdom.model.Timeline;
import com.db.wisdom.model.TopAuthorsResponse;
import com.db.wisdom.model.TrendingEntities;
import com.db.wisdom.model.UserSession;
import com.db.wisdom.model.VideoReport;
import com.db.wisdom.model.WisdomArticleDiscovery;
import com.db.wisdom.model.WisdomArticleRating;
import com.db.wisdom.model.WisdomPage;
import com.db.wisdom.model.WisdomQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WisdomQueryExecutorService {

	private Client client = null;

	private static Logger log = LogManager.getLogger(WisdomQueryExecutorService.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private List<String> hostTypeList = Arrays.asList(HostType.MOBILE, HostType.WEB);
	
	private Connection connection;
	
	static String ip="10.140.0.6";

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public WisdomQueryExecutorService() {
		try {
			initializeClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public Map<String, Object> getAuthorsMonthlyData(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> authorsMap = new HashMap<>();
		// try {
		BoolQueryBuilder qb = getQuery(query);
		String startPubDate = DateUtil.getCurrentDate().replaceAll("_", "-");

		try {
			startPubDate = DateUtil.getFirstDateOfMonth();
		} catch (ParseException e) {
			log.info(e);
		}
		String endPubDate = DateUtil.getCurrentDateTime();
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			startPubDate = query.getStartPubDate();
			endPubDate = query.getEndPubDate();
		}
		int daysCount = DateUtil.getNumDaysBetweenDates(startPubDate, endPubDate) + 1;

		QueryBuilder fqb = qb.filter(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(startPubDate).lte(endPubDate));
		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(fqb)
				.addAggregation(AggregationBuilders.terms("uid").field(Constants.UID).size(100)
						.order(Order.aggregation("tuvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.subAggregation(AggregationBuilders.cardinality("stories").field(Constants.STORY_ID_FIELD)))
				.setSize(0).execute().actionGet();
		if (res.getHits().getTotalHits() == 0) {
			throw new DBAnalyticsException("No author found with id " + query.getUid());
		}
		Terms authorBuckets = res.getAggregations().get("uid");
		for (Terms.Bucket author : authorBuckets.getBuckets()) {
			TopAuthorsResponse authorDetail = new TopAuthorsResponse();
			Sum tpvs_agg = author.getAggregations().get("tpvs");
			long tpvs = ((long) tpvs_agg.getValue());
			Sum tuvs_agg = author.getAggregations().get("tuvs");
			long tuvs = ((long) tuvs_agg.getValue());
			Cardinality stories = author.getAggregations().get("stories");
			long storyCount = stories.getValue();
			long avgStoryCount = 0;
			long avgPvs = 0;
			long avgUvs = 0;
			if (daysCount != 0) {
				avgStoryCount = storyCount / daysCount;
			}
			if (storyCount != 0) {
				avgPvs = tpvs / storyCount;
				avgUvs = tuvs / storyCount;
			}
			authorDetail.setAvgPvs(avgPvs);
			authorDetail.setAvgUvs(avgUvs);
			authorDetail.setAvgStoryCount(avgStoryCount);
			authorDetail.setCurrentMonthPvs(tpvs);
			authorsMap.put(author.getKeyAsString(), authorDetail);
		}
		/*
		 * } catch (Exception e) { e.printStackTrace();
		 * log.error("Error while AuthorsMonthlyData.", e); }
		 */
		log.info("Retrieving AuthorsMonthlyData; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return authorsMap;
	}

	public List<TopAuthorsResponse> getTopAuthorsList(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<TopAuthorsResponse> records = new ArrayList<>();		
		try {
			String startPubDate = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endPubDate = DateUtil.getCurrentDateTime();
			if (query.getStartPubDate() != null) {
				startPubDate = query.getStartPubDate();
			}
			if (query.getEndPubDate() != null) {
				endPubDate = query.getEndPubDate();
			}

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(startPubDate).lte(endPubDate))
					.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			if (query.getUid() != null) {
				boolQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
			}
			String[] includes = { Constants.AUTHOR_NAME };
			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("author_id").field(Constants.UID).size(100)
							.order(Order.aggregation("tuvs", false))
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
							.subAggregation(
									AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
											AggregationBuilders.sum("uvs").field(Constants.UVS))))
					.setSize(0).execute().actionGet();
			Terms authorBuckets = res.getAggregations().get("author_id");
			for (Terms.Bucket author : authorBuckets.getBuckets()) {
				TopAuthorsResponse authorDetail = new TopAuthorsResponse();
				authorDetail.setMpvs(0);
				authorDetail.setWpvs(0);
				authorDetail.setUid(author.getKeyAsString());
				Sum tpvs = author.getAggregations().get("tpvs");
				authorDetail.setTotalpvs(((long) tpvs.getValue()));
				Sum tuvs = author.getAggregations().get("tuvs");
				authorDetail.setTotaluvs(((long) tuvs.getValue()));
				Cardinality storyCount = author.getAggregations().get("storyCount");
				authorDetail.setStory_count((int)storyCount.getValue());
				TopHits topHits = author.getAggregations().get("top");
				authorDetail.setAuthor_name(
						topHits.getHits().getHits()[0].getSource().get(Constants.AUTHOR_NAME).toString());
				Terms hostType = author.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						authorDetail.setMpvs((int) pvs.getValue());
						authorDetail.setMuvs((int) uvs.getValue());
					} else if (host.getKey().equals(HostType.WEB)) {
						authorDetail.setWpvs((int) pvs.getValue());
						authorDetail.setWuvs((int) uvs.getValue());
					}
				}

				List<String> author_id = new ArrayList<>();
				int authorId = Integer.parseInt(author.getKeyAsString());
				author_id.add(String.valueOf(authorId));
				query.setUid(author_id);
				Map<String, Object> sharbilityMap = getOverAllSharability(query);
				authorDetail.setSharability((Double)sharbilityMap.get(Constants.SHAREABILITY));
				authorDetail.setShares((Long)sharbilityMap.get(Constants.SHARES));
				authorDetail.setSessions(getSessionsCount(query));
				records.add(authorDetail);				
				records.sort(new Comparator<TopAuthorsResponse>() {
					@Override
					public int compare(TopAuthorsResponse o1, TopAuthorsResponse o2) {
						return -o1.getSessions().compareTo(o2.getSessions());
					}
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving top authors.", e);
		}
		log.info("Retrieving top authors; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<StoryDetail> getAuthorStoriesList(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<StoryDetail> records = new ArrayList<>();
		// try {
		String startPubDate = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endPubDate = DateUtil.getCurrentDateTime();
		if (query.getStartPubDate() != null) {
			startPubDate = query.getStartPubDate();
		}
		if (query.getEndPubDate() != null) {
			endPubDate = query.getEndPubDate();
		}

		String includes[] = { "story_pubtime", "author_name", "title" };
		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(startPubDate).lte(endPubDate))
						.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList))
						.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
						.must(QueryBuilders.termsQuery(Constants.UID, query.getUid())))
				.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(100)
						.order(Order.aggregation("tuvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
								.size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
						.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
								.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS))))
				.addAggregation(AggregationBuilders.sum("authorPvs").field(Constants.PVS)).setSize(0).execute()
				.actionGet();
		if (res.getHits().getTotalHits() == 0) {
			throw new DBAnalyticsException("No author found with id " + query.getUid());
		}
		Sum author_tpvs = res.getAggregations().get("authorPvs");
		long authorTpvs = (long) author_tpvs.getValue();
		Terms storyBuckets = res.getAggregations().get("stories");
		for (Terms.Bucket story : storyBuckets.getBuckets()) {
			StoryDetail storyDetail = new StoryDetail();
			storyDetail.setStoryid(story.getKeyAsString());
			Sum tpvs = story.getAggregations().get("tpvs");
			storyDetail.setTotalpvs(((long) tpvs.getValue()));
			Sum tuvs = story.getAggregations().get("tuvs");
			storyDetail.setTotaluvs(((long) tuvs.getValue()));
			TopHits topHits = story.getAggregations().get("top");
			Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
			storyDetail.setAuthor_name(source.get(Constants.AUTHOR_NAME).toString());
			storyDetail.setStory_pubtime(source.get(Constants.STORY_PUBLISH_TIME).toString());
			storyDetail.setTitle(source.get(Constants.TITLE).toString());
			Terms hostType = story.getAggregations().get("host_type");
			for (Terms.Bucket host : hostType.getBuckets()) {
				Sum pvs = host.getAggregations().get("pvs");
				Sum uvs = host.getAggregations().get("uvs");
				if (host.getKey().equals(HostType.MOBILE)) {
					storyDetail.setMpvs((long) pvs.getValue());
					storyDetail.setMuvs((long) uvs.getValue());
				} else if (host.getKey().equals(HostType.WEB)) {
					storyDetail.setWpvs((long) pvs.getValue());
					storyDetail.setWuvs((long) uvs.getValue());
				}
			}
			// storyDetail.setAuthor_tpv(authorTpvs);
			records.add(storyDetail);
		}
		/*
		 * } catch (Exception e) { e.printStackTrace();
		 * log.error("Error while retrieving top authors stories.", e); }
		 */
		log.info("Retrieving top authors stories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	/*
	 * public List<StoryDetail> getTopStories(WisdomQuery query) { long
	 * startTime = System.currentTimeMillis(); List<StoryDetail> records = new
	 * ArrayList<>(); try { String startDatetime =
	 * DateUtil.getCurrentDate().replaceAll("_", "-"); String endDatetime =
	 * DateUtil.getCurrentDateTime(); // if (query.getStartDatetime() != null) {
	 * // startDatetime = query.getStartDatetime(); // }
	 * 
	 * String includes[] = { "author_name", "title" }; SearchResponse res =
	 * client.prepareSearch(Indexes.STORY_DETAIL).setTypes(MappingTypes.
	 * MAPPING_REALTIME) .setQuery(QueryBuilders.boolQuery()
	 * .must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(
	 * startDatetime) .lte(endDatetime))
	 * .must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList))
	 * .must(QueryBuilders.termsQuery(Constants.HOST, query.getHost())))
	 * .addAggregation(AggregationBuilders.terms("stories").field(Constants.
	 * STORY_ID_FIELD) .size(query.getCount()).order(Order.aggregation("tpvs",
	 * false))
	 * .subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
	 * .subAggregation(AggregationBuilders.topHits("top").fetchSource(
	 * includes, new String[] {}) .setSize(1))
	 * .subAggregation(AggregationBuilders.terms("host_type").field(Constants.
	 * HOST_TYPE).size(5)
	 * .subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)))
	 * .subAggregation(AggregationBuilders.max("slide").field(Constants.PGTOTAL)
	 * ) .subAggregation(AggregationBuilders.max("version").field(Constants.
	 * VERSION))) .setSize(0).execute().actionGet(); Terms storyBuckets =
	 * res.getAggregations().get("stories"); for (Terms.Bucket story :
	 * storyBuckets.getBuckets()) { StoryDetail storyDetail = new StoryDetail();
	 * storyDetail.setStoryid(story.getKey()); Sum tpvs =
	 * story.getAggregations().get("tpvs"); storyDetail.setTotalpvs(new
	 * Double(tpvs.getValue()).longValue()); TopHits topHits =
	 * story.getAggregations().get("top"); Map<String, Object> source =
	 * topHits.getHits().getHits()[0].getSource();
	 * storyDetail.setAuthor_name(source.get(Constants.AUTHOR_NAME).toString());
	 * storyDetail.setTitle(source.get(Constants.TITLE).toString()); Max slide =
	 * story.getAggregations().get("slide"); storyDetail.setSlideCount(new
	 * Double(slide.getValue()); Max version =
	 * story.getAggregations().get("version"); storyDetail.setVersion(new
	 * Double(version.getValue()); Terms hostType =
	 * story.getAggregations().get("host_type"); for (Terms.Bucket host :
	 * hostType.getBuckets()) { Sum pvs = host.getAggregations().get("pvs"); if
	 * (host.getKey().equals(HostType.MOBILE)) storyDetail.setMpvs(new
	 * Double(pvs.getValue()).longValue()); else if
	 * (host.getKey().equals(HostType.WEB)) storyDetail.setWpvs(new
	 * Double(pvs.getValue()).longValue()); } records.add(storyDetail); } }
	 * catch (Exception e) { e.printStackTrace();
	 * log.error("Error while retrieving top stories.", e); }
	 * log.info("Retrieving top stories; Execution Time:(Seconds) " +
	 * (System.currentTimeMillis() - startTime) / 1000.0); return records; }
	 */

	public List<StoryDetail> getStoryDetail(WisdomQuery query) {
		query.setEnableDate(false);
		// query.setStoryDetail(true);
		return getStoriesList(query);
	}

	public List<SlideDetail> getSlideWisePvs(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<SlideDetail> records = new ArrayList<>();
		String[] fields = { "apvs", "ipvs", "mpvs", "wpvs", "pgno" };
		if (query.getField() != null) {
			fields = query.getField().split(",");
		}
		// Commented code is to remove slides greater than than max slide count
		// from story_detail.

		SearchResponse sr = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
				.addAggregation(AggregationBuilders.max("slideCount").field(Constants.PGTOTAL)).setSize(0).execute()
				.actionGet();

		SearchResponse res = client.prepareSearch(Indexes.STORY_SEQUENCE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
				.addSort(Constants.PGNO, SortOrder.ASC).setFetchSource(fields, new String[] {}).setSize(50).execute()
				.actionGet();

		Double slideCount = ((Max) sr.getAggregations().get("slideCount")).getValue();
		for (SearchHit hit : res.getHits().getHits()) {
			SlideDetail slide = new SlideDetail();
			Map<String, Object> source = hit.getSource();
			if ((Integer) source.get(Constants.PGNO) <= slideCount) {
				slide.setSlideNo((Integer) source.get(Constants.PGNO));
				long mpvs = 0;

				if (query.getExcludeTracker() == null) {
					if (source.get(Constants.UCBPVS) != null)
						mpvs = mpvs + ((Integer) source.get(Constants.UCBPVS)).longValue();
				}

				if (source.get(Constants.MPVS) != null) {
					mpvs = mpvs + ((Integer) source.get(Constants.MPVS)).longValue();
					slide.setMpvs(mpvs);
				}

				if (source.get(Constants.WPVS) != null)
					slide.setWpvs(((Integer) source.get(Constants.WPVS)).longValue());

				if ((Integer) source.get(Constants.PGNO) != 0) {
					if (source.get(Constants.APVS) != null)
						slide.setApvs(((Integer) source.get(Constants.APVS)).longValue());

					if (source.get(Constants.IPVS) != null)
						slide.setIpvs(((Integer) source.get(Constants.IPVS)).longValue());
				}

				records.add(slide);
			}
		}

		log.info("Retrieving getSlideWisePvs; Slide Count: " + records.size() + "; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<StoryPerformance> getStoryPerformance(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<StoryPerformance> records = new ArrayList<>();
		try {

			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
					.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

			if (query.getExcludeTracker() != null) {
				qb.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, query.getExcludeTracker()));
			}

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.dateHistogram("days").field(Constants.DATE_TIME_FIELD)
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
							.subAggregation(AggregationBuilders.max("version").field(Constants.VERSION)))
					.setSize(0).execute().actionGet();

			Histogram interval = res.getAggregations().get("days");
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					StoryPerformance performance = new StoryPerformance();
					Sum tpvs = bucket.getAggregations().get("tpvs");
					performance.setTpvs(((Double) tpvs.getValue()).longValue());
					Sum tuvs = bucket.getAggregations().get("tuvs");
					performance.setTuvs(((Double) tuvs.getValue()).longValue());
					Max version = bucket.getAggregations().get("version");
					performance.setVersion((int) version.getValue());
					performance.setDate(bucket.getKeyAsString());
					records.add(performance);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getPerformanceGraph.", e);
		}
		log.info("Retrieving getPerformanceGraph; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public SourcePerformance getFacebookStoryPerformance(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		SourcePerformance sourcePerformance = new SourcePerformance();
		List<StoryPerformance> records = new ArrayList<>();
		// try {
		long overall_total_reach = 0;
		long overall_unique_reach = 0;
		long overall_link_clicks = 0;
		long shares = 0;

		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.FB_INSIGHTS_HISTORY, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(Constants.BHASKARSTORYID, query.getStoryid()))
				.addAggregation(AggregationBuilders.dateHistogram("days").field(Constants.CRON_DATETIME)
						.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.max("total_reach").field(Constants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.max("link_clicks").field(Constants.LINK_CLICKS)))
				.addAggregation(AggregationBuilders.max("overall_total_reach").field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.max("overall_link_clicks").field(Constants.LINK_CLICKS))
				.addAggregation(AggregationBuilders.max("overall_unique_reach").field(Constants.UNIQUE_REACH))
				.addAggregation(AggregationBuilders.max("shares").field(Constants.SHARES)).setSize(0).execute()
				.actionGet();
		if (res.getHits().getTotalHits() == 0) {
			throw new DBAnalyticsException("No story found with id " + query.getStoryid());
		}

		Histogram interval = res.getAggregations().get("days");
		for (Histogram.Bucket bucket : interval.getBuckets()) {
			if(bucket.getDocCount()>0){
				long total_reach = 0;
				long link_clicks = 0;
				double ctr = 0.0;
				StoryPerformance performance = new StoryPerformance();

				Max agg_total_reach = bucket.getAggregations().get("total_reach");			
				total_reach = ((Double) agg_total_reach.getValue()).longValue();

				Max agg_link_clicks = bucket.getAggregations().get("link_clicks");
				link_clicks = ((Double) agg_link_clicks.getValue()).longValue();

				if (total_reach != 0) {
					ctr = (agg_link_clicks.getValue() / agg_total_reach.getValue()) * 100;
				}

				performance.setDate(bucket.getKeyAsString());
				performance.setTotal_reach(total_reach);
				performance.setLink_clicks(link_clicks);
				performance.setCtr(ctr);
				records.add(performance);
			}
		}
		Max total_reach_agg = res.getAggregations().get("overall_total_reach");
		overall_total_reach = (long) total_reach_agg.getValue();
		Max unique_reach_agg = res.getAggregations().get("overall_unique_reach");
		overall_unique_reach = (long) unique_reach_agg.getValue();
		Max link_clicks_agg = res.getAggregations().get("overall_link_clicks");
		overall_link_clicks = (long) link_clicks_agg.getValue();
		Max shares_agg = res.getAggregations().get("shares");
		shares = (long) shares_agg.getValue();
		double ctr = 0.0;
		if (overall_total_reach != 0) {
			ctr = (link_clicks_agg.getValue() / overall_total_reach) * 100;
		}

		sourcePerformance.setCtr(ctr);
		sourcePerformance.setLink_clicks(overall_link_clicks);
		sourcePerformance.setPerformance(records);
		sourcePerformance.setShares(shares);
		sourcePerformance.setTotal_reach(overall_total_reach);
		sourcePerformance.setUnique_reach(overall_unique_reach);
		// }
		// catch (Exception e) {
		// e.printStackTrace();
		// log.error("Error while getFacebookPerformanceGraph.", e);
		// }
		log.info("Retrieving getFacebookPerformanceGraph; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return sourcePerformance;
	}

	public SourcePerformance getTrackerwisePerformanceGraph(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		SourcePerformance sourcePerformance = new SourcePerformance();
		List<StoryPerformance> records = new ArrayList<>();
		long total_pvs = 0L;
		long total_uvs = 0L;
		try {

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(QueryBuilders.boolQuery()
							.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
							.must(QueryBuilders.termsQuery(Constants.TRACKER, query.getTracker()))
							.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList)))
					.addAggregation(AggregationBuilders.dateHistogram("days").field(Constants.DATE_TIME_FIELD)
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)))
					.setSize(0).execute().actionGet();

			Histogram interval = res.getAggregations().get("days");
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					StoryPerformance performance = new StoryPerformance();
					Sum tpvs = bucket.getAggregations().get("tpvs");
					performance.setDate(bucket.getKeyAsString());
					performance.setTpvs(((Double) tpvs.getValue()).longValue());
					total_pvs += ((Double) tpvs.getValue()).longValue();
					Sum tuvs = bucket.getAggregations().get("tuvs");
					performance.setTuvs(((Double) tuvs.getValue()).longValue());
					total_uvs += ((Double) tuvs.getValue()).longValue();
					records.add(performance);
				}
			}
			sourcePerformance.setPerformance(records);
			sourcePerformance.setPvs(total_pvs);
			sourcePerformance.setUvs(total_uvs);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getTrackerwisePerformanceGraph.", e);
		}
		log.info("Retrieving getTrackerwisePerformanceGraph; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return sourcePerformance;
	}

	public TreeMap<Integer, StoryDetail> getVersionwisePerformance(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		TreeMap<Integer, StoryDetail> records = new TreeMap<>();
		try {
			String includes[] = { Constants.TITLE, Constants.PGTOTAL };
			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
					.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

			if (query.getExcludeTracker() != null) {
				qb.mustNot(QueryBuilders.termsQuery(Constants.TRACKER, query.getExcludeTracker()));
			}

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("version").field(Constants.VERSION).size(50)
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1))
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
											AggregationBuilders.sum("uvs").field(Constants.UVS)))
							.subAggregation(AggregationBuilders.terms("sourceTraffic").field(Constants.REF_PLATFORM)
									.size(5).order(Order.aggregation("pvs", false)).subAggregation(
											AggregationBuilders.sum("pvs").field(Constants.PVS))
									.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS)))
							.subAggregation(AggregationBuilders.terms("pageViewsTracker").field(Constants.TRACKER)
									.size(5).order(Order.aggregation("pvs", false))
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
									.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS))))
					.setSize(0).execute().actionGet();

			Terms versionBuckets = res.getAggregations().get("version");
			for (Terms.Bucket version : versionBuckets.getBuckets()) {
				StoryDetail storyDetail = new StoryDetail();
				Map<String, Map<String, Long>> sourceTraffic = new LinkedHashMap<>();
				Map<String, Map<String, Long>> pageViewsTracker = new LinkedHashMap<>();
				Long tpvs = 0L;
				Long tuvs = 0L;
				Long mpvs = 0L;
				Long wpvs = 0L;
				Long apvs = 0L;
				Long ipvs = 0L;
				Long muvs = 0L;
				Long wuvs = 0L;
				Long auvs = 0L;
				Long iuvs = 0L;
				int slideCount = 0;
				Long slideDepth = 0L;
				int versionNo = Integer.valueOf(version.getKeyAsString());
				storyDetail.setVersion(versionNo);
				Sum totalPvs = version.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				Sum totalUvs = version.getAggregations().get("tuvs");
				tuvs = Double.valueOf(totalUvs.getValue()).longValue();
				storyDetail.setTotalpvs(tpvs);
				storyDetail.setTotaluvs(tuvs);
				TopHits topHits = version.getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				if (source.get(Constants.TITLE) != null)
					storyDetail.setTitle(source.get(Constants.TITLE).toString());
				// if(source.get(Constants.PGTOTAL)==null)
				if (source.get(Constants.PGTOTAL) != null) {
					slideCount = Integer.valueOf(source.get(Constants.PGTOTAL).toString());
					storyDetail.setSlideCount(slideCount);
				}
				Terms hostType = version.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						mpvs = (long) pvs.getValue();
						muvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.WEB)) {
						wpvs = (long) pvs.getValue();
						wuvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.ANDROID)) {
						apvs = (long) pvs.getValue();
						auvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.IPHONE)) {
						ipvs = (long) pvs.getValue();
						iuvs = (long) uvs.getValue();
					}
				}
				if (slideCount != 0 && tuvs != 0) {
					slideDepth = (long) ((tpvs.doubleValue() / (tuvs * slideCount)) * 100);
				}
				storyDetail.setSlideDepth(slideDepth);
				Terms traffic = version.getAggregations().get("sourceTraffic");
				for (Terms.Bucket trafficS : traffic.getBuckets()) {
					Map<String, Long> map = new HashMap<>();
					Sum pvs = trafficS.getAggregations().get("pvs");
					Sum uvs = trafficS.getAggregations().get("uvs");
					map.put("pvs", (long) pvs.getValue());
					map.put("uvs", (long) uvs.getValue());
					sourceTraffic.put(trafficS.getKeyAsString(), map);
				}
				Terms pvTracker = version.getAggregations().get("pageViewsTracker");
				for (Terms.Bucket tracker : pvTracker.getBuckets()) {

					Map<String, Long> map = new HashMap<>();
					Sum pvs = tracker.getAggregations().get("pvs");
					Sum uvs = tracker.getAggregations().get("uvs");
					map.put("pvs", (long) pvs.getValue());
					map.put("uvs", (long) uvs.getValue());
					pageViewsTracker.put(tracker.getKeyAsString(), map);
				}
				storyDetail.setMpvs(mpvs);
				storyDetail.setWpvs(wpvs);
				storyDetail.setApvs(apvs);
				storyDetail.setIpvs(ipvs);
				storyDetail.setMuvs(muvs);
				storyDetail.setWuvs(wuvs);
				storyDetail.setAuvs(auvs);
				storyDetail.setIuvs(iuvs);
				storyDetail.setSourceWiseTraffic(sourceTraffic);
				storyDetail.setPageViewsTracker(pageViewsTracker);
				records.put(versionNo, storyDetail);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getVersionwisePerformance.", e);
		}
		log.info("Retrieving getVersionwisePerformance; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}


	public List<Timeline> getStoryTimeline(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<Timeline> records = new ArrayList<>();
		try {
			// String indexName = "realtime_" + DateUtil.getCurrentDate();
			MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();

			if (query.getFlickerTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getFlickerTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(Constants.DATE_TIME_FIELD, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)));

			}

			if (query.getRhsTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getRhsTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(Constants.DATE_TIME_FIELD, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)));

			}

			if (query.getForganicTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getForganicTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(Constants.DATE_TIME_FIELD, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)));

			}

			if (query.getFpaidTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getFpaidTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(Constants.DATE_TIME_FIELD, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)));

			}

			MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
			int counter = 0;
			for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
				Timeline timeline = new Timeline();
				SearchResponse response = item.getResponse();
				if (response.getHits().getHits().length > 0) {
					if (counter == 0) {
						timeline.setTracker("Flicker");
					} else if (counter == 1) {
						timeline.setTracker("RHS");
					} else if (counter == 2) {
						timeline.setTracker("F-Organic");
					} else if (counter == 3) {
						timeline.setTracker("F-Paid");
					}
					timeline.setDatetime(
							response.getHits().getHits()[0].getSource().get(Constants.DATE_TIME_FIELD).toString());
					Sum tpvs = response.getAggregations().get("tpvs");
					timeline.setTpvs(((Double) tpvs.getValue()).longValue());
					Sum tuvs = response.getAggregations().get("tuvs");
					timeline.setTuvs(((Double) tuvs.getValue()).longValue());
					records.add(timeline);
				}
				counter++;

			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getStoryTimeline.", e);
		}
		log.info("Retrieving getStoryTimeline; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public WisdomPage getTopStoryListing(WisdomQuery query) {
		long start = System.currentTimeMillis();
		List<StoryDetail> stories = new ArrayList<>();
		WisdomPage page = new WisdomPage();
		if (query.getCount() > 100) {
			stories = getBulkStoriesList(query);
		} else {
			stories = getStoriesList(query);

		}		
		page = getTopBar(query);
		page.setStories(stories);
		if(query.getStoryid()!=null){
			page.setSessions(stories.get(0).getTotaluvs());
		}
		log.info(
				"Total execution time for Top Story Listing (Seconds): " + (System.currentTimeMillis() - start) / 1000);
		return page;
	}

	private WisdomPage getTopBar(WisdomQuery query) {
		long start = System.currentTimeMillis();
		WisdomPage page = new WisdomPage();
		long totalPvs = 0;
		long totalUpvs = 0;
		String[] indexName = new String[2];
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
		}
		else{
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate());
		}	

		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(getQuery(query))
				.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
				.addAggregation(AggregationBuilders.sum("tupvs").field(Constants.UVS))
				.setSize(0).execute().actionGet();


		Sum pvs_agg = res.getAggregations().get("tpvs");
		totalPvs = (long) pvs_agg.getValue();

		Sum upvs_agg = res.getAggregations().get("tupvs");
		totalUpvs = (long) upvs_agg.getValue();

		Map<String, Object> sharbilityMap = getOverAllSharability(query);

		page.setCurrentDayPvs(totalPvs);
		page.setCurrentDayUvs(totalUpvs);
		page.setShareability((Double)sharbilityMap.get(Constants.SHAREABILITY));
		page.setShares((Long)sharbilityMap.get(Constants.SHARES));
		page.setSessions(getSessionsCount(query));
		log.info("Total execution time for Top Story Listing top bar (Seconds): " + (System.currentTimeMillis() - start) / 1000);
		return page;
	}

	public WisdomPage getEditorDashboard(WisdomQuery query) {
		long start = System.currentTimeMillis();
		WisdomPage page = new WisdomPage();
		List<StoryDetail> stories = getStoriesList(query);
		page = getTopBar(query);
		page.setStories(stories);
		if(query.getStoryid()!=null){
			page.setSessions(stories.get(0).getTotaluvs());
		}
		/*List<Integer> storyIds = new ArrayList<>();
        Long currentDayPvs = 0L;

		for (StoryDetail story : currentDayStories) {
			currentDayPvs += story.getTotalpvs();
			storyIds.add(Integer.valueOf(story.getStoryid()));
		}
		BoolQueryBuilder boolQueryBuilder = getQuery(query);
		boolQueryBuilder.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyIds));
		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(boolQueryBuilder)
				.setSize(0).execute().actionGet();
		page.setCurrentDayPvs(currentDayPvs);
		page.setStories(currentDayStories);
		Map<String, Object> sharbilityMap = getOverAllSharability(query);
		page.setShareability((Double)sharbilityMap.get(Constants.SHAREABILITY));
		page.setShares((Long)sharbilityMap.get(Constants.SHARES));
		page.setSessions(getSessionsCount(query));*/

		log.info("Total execution time for editor dashboard (Seconds): " + (System.currentTimeMillis() - start) / 1000);
		return page;
	}

	/*private WisdomPage getTopStoriesListingTopBar(WisdomQuery query) {

	Boolean prevDay = false;
	long startTime = System.currentTimeMillis();
	String histogramField = Constants.DATE_TIME_FIELD;
	BoolQueryBuilder qb = new BoolQueryBuilder();
	WisdomPage page = new WisdomPage();
	long mpvs = 0;
	long wpvs = 0;
	long apvs = 0;
	long ipvs = 0;
	Long tpvs = 0L;
	long muvs = 0;
	long wuvs = 0;
	long auvs = 0;
	long iuvs = 0;
	Long tuvs = 0L;
	Long socialTraffic = 0L;
	Long storyCount = 0L;
	BoolQueryBuilder bqb = new BoolQueryBuilder();
	String[] indexName = new String[2];
	if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
		indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
	}
	else{
		indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate());
	}
	 *//**
	 * If input query has filter based on publish date then set histogram
	 * field to story_pubtime and set datetime enable false
	 *//*
	if (query.getStartPubDate() != null) {
		histogramField = Constants.STORY_PUBLISH_TIME;
		query.setEnableDate(false);
		// Return previous day data only if date range is < 1 day
		if (DateUtil.getNumDaysBetweenDates(query.getStartPubDate(), query.getEndPubDate()) < 1) {
			prevDay = true;

			// for current date set previous day date range to current time
			// only
			if (query.getStartPubDate().contains(DateUtil.getCurrentDate().replaceAll("_", "-"))) {
				query.setStartPubDate(null);
				query.setEndPubDate(null);
				bqb = getQuery(query).must(QueryBuilders.boolQuery()
						.should(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME)
								.gte(DateUtil.getPreviousDate().replaceAll("_", "-"))
								.lte(DateUtil.addHoursToTime(DateUtil.getCurrentDateTime(), -24)))
						.should(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME)
								.gte(DateUtil.getCurrentDate().replaceAll("_", "-"))
								.lte(DateUtil.getCurrentDateTime())));
				query.setEndPubDate(DateUtil.getCurrentDateTime());
			} else {
				query.setStartPubDate(DateUtil.getPreviousDate(query.getStartPubDate(), "yyyy-MM-dd"));
				bqb = getQuery(query);
			}
		} else {
			bqb = getQuery(query);
		}
}

	else {
		// Set default interval if there is no date range in query.
		if (query.getStartDate() == null && query.getEndDate() == null) {
			query.setStartDate(DateUtil.getCurrentDate().replaceAll("_", "-"));
			query.setEndDate(DateUtil.getCurrentDateTime());
		}
		// Return previous day data only if date range is < 1 day
		if (DateUtil.getNumDaysBetweenDates(query.getStartDate(), query.getEndDate()) < 1) {
			prevDay = true;

			// for current day set previous day pvs upto current time only
			// from "story_detail_hourly" index
			if (query.getStartDate().contains(DateUtil.getCurrentDate().replaceAll("_", "-"))) {
				query.setStartDate(DateUtil.getPreviousDate(query.getStartDate(), "yyyy-MM-dd"));
				query.setEndDate(DateUtil.addHoursToTime(DateUtil.getCurrentDateTime(), -24));
				bqb = getQuery(query);
				SearchResponse prevDayRes = client.prepareSearch(Indexes.STORY_DETAIL_HOURLY)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb)
						.addAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(Constants.TRACKER, query.getSocialTracker()))
								.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS)))
						.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.addAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
										AggregationBuilders.sum("uvs").field(Constants.UVS)))
						.addAggregation(
								AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
						.setSize(0).execute().actionGet();
				Filter filter = prevDayRes.getAggregations().get("socialTraffic");
				Sum socialTrafficAgg = filter.getAggregations().get("tpvs");
				socialTraffic = (int) socialTrafficAgg.getValue()).longValue();
				Sum totalPvs = prevDayRes.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				Sum totalUvs = prevDayRes.getAggregations().get("tuvs");
				tuvs = Double.valueOf(totalUvs.getValue()).longValue();
				Cardinality storyCountAggregation = prevDayRes.getAggregations().get("storyCount");
				// Max slide = bucket.getAggregations().get("slide");
				// slideCount = (int) slide.getValue();
				Terms hostType = prevDayRes.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						mpvs = (int) pvs.getValue()).longValue();
						muvs = (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.WEB)) {
						wpvs = (int) pvs.getValue()).longValue();
						wuvs = (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.ANDROID)) {
						apvs = (int) pvs.getValue()).longValue();
						auvs = (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.IPHONE)) {
						ipvs = (int) pvs.getValue()).longValue();
						iuvs = (int) uvs.getValue()).longValue();
					}
				}
				page.setPreviousDayPvs(tpvs);
				page.setPreviousDayStoryCount(new Long(storyCountAggregation.getValue()));
				page.setPreviousDayMPvs(mpvs);
				page.setPreviousDayWPvs(wpvs);
				page.setPreviousDayAPvs(apvs);
				page.setPreviousDayIPvs(ipvs);
				page.setPreviousDayUvs(tuvs);
				page.setPreviousDayMUvs(muvs);
				page.setPreviousDayWUvs(wuvs);
				page.setPreviousDayAUvs(auvs);
				page.setPreviousDayIUvs(iuvs);
				page.setPreviousDaySocialTraffic(socialTraffic);

				// reset start and end date for current day data calculation
				query.setStartDate(DateUtil.getCurrentDate().replaceAll("_", "-"));
				query.setEndDate(DateUtil.getCurrentDateTime());
			} else {
				query.setStartDate(DateUtil.getPreviousDate(query.getStartDate(), "yyyy-MM-dd"));
			}
			}
		bqb = getQuery(query);
	}

	try {
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(bqb)
				.addAggregation(AggregationBuilders.dateHistogram("days").field(histogramField)
						.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(Constants.TRACKER, query.getSocialTracker()))
								.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS)))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
										AggregationBuilders.sum("uvs").field(Constants.UVS)))
						.subAggregation(
								AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD)))
				.setSize(0).execute().actionGet();
		Histogram interval = res.getAggregations().get("days");
		if (prevDay) {
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					mpvs = 0;
					wpvs = 0;
					apvs = 0;
					ipvs = 0;
					tpvs = 0L;
					muvs = 0;
					wuvs = 0;
					auvs = 0;
					iuvs = 0;
					tuvs = 0L;
					socialTraffic = 0L;
					String bucketDate = bucket.getKeyAsString();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date date = sdf.parse(bucketDate);
					String date1 = sdf.format(date);
					Filter filter = bucket.getAggregations().get("socialTraffic");
					Sum socialTrafficAgg = filter.getAggregations().get("tpvs");
					socialTraffic = (int) socialTrafficAgg.getValue()).longValue();
					Sum totalPvs = bucket.getAggregations().get("tpvs");
					tpvs = Double.valueOf(totalPvs.getValue()).longValue();
					Sum totalUvs = bucket.getAggregations().get("tuvs");
					tuvs = Double.valueOf(totalUvs.getValue()).longValue();
					Cardinality storyCountAggregation = bucket.getAggregations().get("storyCount");
					Terms hostType = bucket.getAggregations().get("host_type");
					for (Terms.Bucket host : hostType.getBuckets()) {
						Sum pvs = host.getAggregations().get("pvs");
						Sum uvs = host.getAggregations().get("uvs");
						if (host.getKey().equals(HostType.MOBILE)) {
							mpvs = (int) pvs.getValue()).longValue();
							muvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.WEB)) {
							wpvs = (int) pvs.getValue()).longValue();
							wuvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.ANDROID)) {
							apvs = (int) pvs.getValue()).longValue();
							auvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.IPHONE)) {
							ipvs = (int) pvs.getValue()).longValue();
							iuvs = (int) uvs.getValue()).longValue();
						}
					}
					if ((query.getEndDate() != null && query.getEndDate().contains(date1))
							|| (query.getEndPubDate() != null && query.getEndPubDate().contains(date1))) {
						page.setCurrentDayPvs(tpvs);
						page.setCurrentDayStoryCount(new Long(storyCountAggregation.getValue()));
						page.setCurrentDayMPvs(mpvs);
						page.setCurrentDayWPvs(wpvs);
						page.setCurrentDayAPvs(apvs);
						page.setCurrentDayIPvs(ipvs);
						page.setCurrentDaySocialTraffic(socialTraffic);
						page.setCurrentDayUvs(tuvs);
						page.setCurrentDayMUvs(muvs);
						page.setCurrentDayWUvs(wuvs);
						page.setCurrentDayAUvs(auvs);
						page.setCurrentDayIUvs(iuvs);
					} else {
						page.setPreviousDayPvs(tpvs);
						page.setPreviousDayStoryCount(new Long(storyCountAggregation.getValue()));
						page.setPreviousDayMPvs(mpvs);
						page.setPreviousDayWPvs(wpvs);
						page.setPreviousDayAPvs(apvs);
						page.setPreviousDayIPvs(ipvs);
						page.setPreviousDayUvs(tuvs);
						page.setPreviousDayMUvs(muvs);
						page.setPreviousDayWUvs(wuvs);
						page.setPreviousDayAUvs(auvs);
						page.setPreviousDayIUvs(iuvs);
						page.setPreviousDaySocialTraffic(socialTraffic);
					}
			}
			}
		}

		// if no of days in date range are greater than 1 then no need to
		// set previous day data.
		// Add pvs for all days.
		else {
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					Filter filter = bucket.getAggregations().get("socialTraffic");
					Sum socialTrafficAgg = filter.getAggregations().get("tpvs");
					socialTraffic += (int) socialTrafficAgg.getValue()).longValue();
					Sum totalPvs = bucket.getAggregations().get("tpvs");
					tpvs += Double.valueOf(totalPvs.getValue()).longValue();
					Sum totalUvs = bucket.getAggregations().get("tuvs");
					tuvs += Double.valueOf(totalUvs.getValue()).longValue();
					Cardinality storyCountAggregation = bucket.getAggregations().get("storyCount");
					storyCount += new Long(storyCountAggregation.getValue());
					Terms hostType = bucket.getAggregations().get("host_type");
					for (Terms.Bucket host : hostType.getBuckets()) {
						Sum pvs = host.getAggregations().get("pvs");
						Sum uvs = host.getAggregations().get("uvs");
						if (host.getKey().equals(HostType.MOBILE)) {
							mpvs = (int) pvs.getValue()).longValue();
							muvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.WEB)) {
							wpvs = (int) pvs.getValue()).longValue();
							wuvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.ANDROID)) {
							apvs = (int) pvs.getValue()).longValue();
							auvs = (int) uvs.getValue()).longValue();
						} else if (host.getKey().equals(HostType.IPHONE)) {
							ipvs = (int) pvs.getValue()).longValue();
							iuvs = (int) uvs.getValue()).longValue();
						}
					}
					page.setCurrentDayPvs(tpvs);
					page.setCurrentDayStoryCount(storyCount);
					page.setCurrentDayMPvs(mpvs);
					page.setCurrentDayWPvs(wpvs);
					page.setCurrentDayAPvs(apvs);
					page.setCurrentDayIPvs(ipvs);
					page.setCurrentDaySocialTraffic(socialTraffic);
					page.setCurrentDayUvs(tuvs);
					page.setCurrentDayMUvs(muvs);
					page.setCurrentDayWUvs(wuvs);
					page.setCurrentDayAUvs(auvs);
					page.setCurrentDayIUvs(iuvs);
				}
			}
		}
	} catch (Exception e) {
		e.printStackTrace();
		log.error("Error while retrieving top stories listing tob bar.", e);
	}
	log.info("Retrieving top stories listing top bar; Execution Time:(Seconds) "
			+ (System.currentTimeMillis() - startTime) / 1000.0);
	return page;
}*/


	/*public WisdomPage getEditorDashboard(WisdomQuery query) {
		long start = System.currentTimeMillis();
		WisdomPage page = new WisdomPage();
		Long currentDayPvs = 0L;
		Long currentDayMPvs = 0L;
		Long currentDayWPvs = 0L;
		Long currentDayAPvs = 0L;
		Long currentDayIPvs = 0L;
		Long currentDayUvs = 0L;
		Long currentDayMUvs = 0L;
		Long currentDayWUvs = 0L;
		Long currentDayAUvs = 0L;
		Long currentDayIUvs = 0L;
		Long currentDaySocialTraffic = 0L;
		Long previousDayPvs = 0L;
		Long previousDayMPvs = 0L;
		Long previousDayWPvs = 0L;
		Long previousDayAPvs = 0L;
		Long previousDayIPvs = 0L;
		Long previousDayUvs = 0L;
		Long previousDayMUvs = 0L;
		Long previousDayWUvs = 0L;
		Long previousDayAUvs = 0L;
		Long previousDayIUvs = 0L;
		Long previousDaySocialTraffic = 0L;
		// current day data
		List<StoryDetail> currentDayStories = getStoriesList(query);
		List<Integer> storyIds = new ArrayList<>();

		for (StoryDetail story : currentDayStories) {
			currentDayPvs += story.getTotalpvs();
			currentDayMPvs += story.getMpvs();
			currentDayWPvs += story.getWpvs();
			currentDayAPvs += story.getApvs();
			currentDayIPvs += story.getIpvs();
			currentDayUvs += story.getTotaluvs();
			currentDayMUvs += story.getMuvs();
			currentDayWUvs += story.getWuvs();
			currentDayAUvs += story.getAuvs();
			currentDayIUvs += story.getIuvs();
			storyIds.add(Integer.valueOf(story.getStoryid()));
		}
		BoolQueryBuilder boolQueryBuilder = getQuery(query);
		boolQueryBuilder.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyIds));
		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(boolQueryBuilder)
				.addAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(Constants.TRACKER, query.getSocialTracker()))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS)))
				.setSize(0).execute().actionGet();
		Filter filter = res.getAggregations().get("socialTraffic");
		Sum socialTraffic = filter.getAggregations().get("tpvs");
		currentDaySocialTraffic = (int) socialTraffic.getValue()).longValue();

		// Start: Previous day data calculation
		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();
		if (query.getStartDate() != null && query.getEndDate() != null) {
			startDatetime = query.getStartDate();
			endDatetime = query.getEndDate();
		}
		// Return previous day data only if date range is < 1 day
		String[] prevDayIndex = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getPreviousDate(startDatetime,"yyyy-MM-dd"), DateUtil.getPreviousDate(startDatetime,"yyyy-MM-dd"));
		if (DateUtil.getNumDaysBetweenDates(startDatetime, endDatetime) < 1) {
			if (startDatetime.contains(DateUtil.getCurrentDate().replaceAll("_", "-"))) {
				prevDayIndex[0] = "story_detail_hourly";
				query.setEndDate(DateUtil.addHoursToTime(DateUtil.getCurrentDateTime(), -24));
			} else {
				query.setEndDate(DateUtil.getPreviousDate(endDatetime, "yyyy-MM-dd"));
			}

			query.setStartDate(DateUtil.getPreviousDate(startDatetime, "yyyy-MM-dd"));

			SearchResponse prevDayRes = client.prepareSearch(prevDayIndex).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(getQuery(query))
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
							.size(query.getCount())
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
											AggregationBuilders.sum("uvs").field(Constants.UVS)))
							.subAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(Constants.TRACKER, query.getSocialTracker()))
									.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))))
					.setSize(0).execute().actionGet();
			Terms storyBuckets = prevDayRes.getAggregations().get("stories");
			for (Terms.Bucket story : storyBuckets.getBuckets()) {
				Terms hostType = story.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						previousDayMPvs += (int) pvs.getValue()).longValue();
						previousDayMUvs += (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.WEB)) {
						previousDayWPvs += (int) pvs.getValue()).longValue();
						previousDayWUvs += (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.ANDROID)) {
						previousDayAPvs += (int) pvs.getValue()).longValue();
						previousDayAUvs += (int) uvs.getValue()).longValue();
					} else if (host.getKey().equals(HostType.IPHONE)) {
						previousDayIPvs += (int) pvs.getValue()).longValue();
						previousDayIUvs += (int) uvs.getValue()).longValue();
					}
				}
				Filter prevFilter = story.getAggregations().get("socialTraffic");
				Sum prevSocialTraffic = prevFilter.getAggregations().get("tpvs");
				previousDaySocialTraffic += (int) prevSocialTraffic.getValue()).longValue();
			}
			if (query.getHost_type().contains(HostType.WEB)) {
				previousDayPvs = previousDayMPvs + previousDayWPvs;
				previousDayUvs = previousDayMUvs + previousDayWUvs;
			} else if (query.getHost_type().contains(HostType.ANDROID)) {
				previousDayPvs = previousDayAPvs + previousDayIPvs;
				previousDayUvs = previousDayAUvs + previousDayIUvs;
			}

		}

		// End: Previous day data calculation
		page.setCurrentDayPvs(currentDayPvs);
		page.setCurrentDayMPvs(currentDayMPvs);
		page.setCurrentDayWPvs(currentDayWPvs);
		page.setCurrentDayAPvs(currentDayAPvs);
		page.setCurrentDayIPvs(currentDayIPvs);
		page.setPreviousDayPvs(previousDayPvs);
		page.setPreviousDayMPvs(previousDayMPvs);
		page.setPreviousDayWPvs(previousDayWPvs);
		page.setPreviousDayAPvs(previousDayAPvs);
		page.setPreviousDayIPvs(previousDayIPvs);
		page.setCurrentDayUvs(currentDayUvs);
		page.setCurrentDayMUvs(currentDayMUvs);
		page.setCurrentDayWUvs(currentDayWUvs);
		page.setCurrentDayAUvs(currentDayAUvs);
		page.setCurrentDayIUvs(currentDayIUvs);
		page.setPreviousDayUvs(previousDayUvs);
		page.setPreviousDayMUvs(previousDayMUvs);
		page.setPreviousDayWUvs(previousDayWUvs);
		page.setPreviousDayAUvs(previousDayAUvs);
		page.setPreviousDayIUvs(previousDayIUvs);
		page.setStories(currentDayStories);
		page.setCurrentDaySocialTraffic(currentDaySocialTraffic);
		page.setPreviousDaySocialTraffic(previousDaySocialTraffic);
		page.setShareability((Double)getOverAllSharability(query).get(Constants.SHAREABILITY));
		page.setShares((Long)getOverAllSharability(query).get(Constants.SHARES));
		log.info("Total execution time for editor dashboard (Seconds): " + (System.currentTimeMillis() - start) / 1000);
		return page;
	}*/

	private List<StoryDetail> getBulkStoriesList(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<StoryDetail> records = new ArrayList<>();
		String includes[] = { Constants.STORY_PUBLISH_TIME, Constants.AUTHOR_NAME, Constants.URL, Constants.TITLE };
		String[] indexName = new String[2];
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
		}
		else{
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate());
		}
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(getQuery(query))
				.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
						.size(query.getCount()).order(Order.aggregation("tuvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
								.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS)))
						.subAggregation(AggregationBuilders.terms("version").field(Constants.VERSION)
								.order(Order.term(false)).size(1).subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(includes, new String[] {}).size(1))))
				.setSize(0).execute().actionGet();

		Terms storyBuckets = res.getAggregations().get("stories");
		for (Terms.Bucket story : storyBuckets.getBuckets()) {
			try {
				StoryDetail storyDetail = new StoryDetail();
				// Set default value for isUCB flag
				long tpvs = 0;
				long mpvs = 0;
				long wpvs = 0;
				long tuvs = 0;
				long muvs = 0;
				long wuvs = 0;
				String storyid = story.getKeyAsString();
				storyDetail.setStoryid(storyid);

				Sum totalPvs = story.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				storyDetail.setTotalpvs(tpvs);

				Sum totalUvs = story.getAggregations().get("tuvs");
				tuvs = Double.valueOf(totalUvs.getValue()).longValue();
				storyDetail.setTotaluvs(tuvs);

				Terms version = story.getAggregations().get("version");
				TopHits topHits = version.getBuckets().get(0).getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				if (source.get(Constants.TITLE).toString().contains("????")) {
					log.warn("Ignoring record because of invalid title. Title: " + storyDetail.getTitle());
					continue;
				}
				storyDetail.setAuthor_name(source.get(Constants.AUTHOR_NAME).toString());
				storyDetail.setTitle(source.get(Constants.TITLE).toString());
				storyDetail.setStory_pubtime((String) source.get(Constants.STORY_PUBLISH_TIME));
				storyDetail.setUrl((String) source.get(Constants.URL));

				Terms hostType = story.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						mpvs = (long) pvs.getValue();
						muvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.WEB)) {
						wpvs = (long) pvs.getValue();
						wuvs = (long) uvs.getValue();
					}
				}

				storyDetail.setMpvs(mpvs);
				storyDetail.setWpvs(wpvs);
				storyDetail.setMuvs(muvs);
				storyDetail.setWuvs(wuvs);
				records.add(storyDetail);
			} catch (Exception e) {
				e.printStackTrace();
				log.warn("Ignoring story " + story.getKey() + ", Caused by: " + e.getMessage());
				continue;
			}
		}

		log.info("Retrieving bulk top stories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	private List<StoryDetail> getStoriesList(WisdomQuery query) {

		long startTime = System.currentTimeMillis();
		List<StoryDetail> records = new ArrayList<>();

		/*String includes[] = { Constants.STORY_PUBLISH_TIME, Constants.AUTHOR_NAME, Constants.TITLE, Constants.IMAGE,
				Constants.URL, Constants.UID, Constants.CHANNEL_SLNO, Constants.PGTOTAL, Constants.FLAG_V };*/
		
		String[] indexName = new String[2];
		if(StringUtils.isNotBlank(query.getStoryid())){
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate());
		}		
		else if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
		}
		else {
			indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate());
		}
		
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(getQuery(query))
				.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
						.size(query.getCount()).order(Order.aggregation("tuvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
						.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
										AggregationBuilders.sum("uvs").field(Constants.UVS)))
						.subAggregation(AggregationBuilders.terms("version").field(Constants.VERSION)
								.order(Order.term(false)).size(1)
								.subAggregation(AggregationBuilders.topHits("top").size(1)))
						.subAggregation(AggregationBuilders.terms("sourceTraffic").field(Constants.REF_PLATFORM)
								.size(query.getElementCount()).order(Order.aggregation("pvs", false)).subAggregation(
										AggregationBuilders.sum("pvs").field(Constants.PVS))
								.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS)))
						.subAggregation(AggregationBuilders.terms("pageViewsTracker").field(Constants.TRACKER).size(10)
								.order(Order.aggregation("pvs", false))
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
								.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS))))
				.setSize(0).execute().actionGet();

		Terms storyBuckets = res.getAggregations().get("stories");

		MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
		boolean setFacebookFlag = true;

		ArrayList<String> storyList = new ArrayList<>();
		for (Terms.Bucket story : storyBuckets.getBuckets()) {
			try {
				
				StoryDetail storyDetail = new StoryDetail();
				
				// Set default value for isUCB flag
				storyDetail.setIsUCB(Boolean.FALSE);
				storyDetail.setIsFacebook(Boolean.FALSE);
				Map<String, Map<String, Long>> sourceTraffic = new LinkedHashMap<>();
				Map<String, Map<String, Long>> pageViewsTracker = new LinkedHashMap<>();

				Long tpvs = 0L;
				Long tuvs = 0L;
				long muvs = 0;
				long wuvs = 0;
				long auvs = 0;
				long iuvs = 0;
				long mpvs = 0;
				long wpvs = 0;
				long apvs = 0;
				long ipvs = 0;
				long mSlideDepth = 0;
				long wSlideDepth = 0;
				long aSlideDepth = 0;
				long iSlideDepth = 0;
				long slideDepth = 0;
				int slideCount = 0;
				long sitePvs = 0;
				long pvContribution = 0;
				long siteUvs = 0;
				long uvContribution = 0;

				String storyid = story.getKeyAsString();
				storyDetail.setStoryid(storyid);
				storyList.add(storyid);
				
				Terms version = story.getAggregations().get("version");
				TopHits topHits = version.getBuckets().get(0).getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				
				storyDetail = gson.fromJson(topHits.getHits().getHits()[0].getSourceAsString(), StoryDetail.class);
				
				if (storyDetail.getTitle().contains("????")) {
					log.warn("Ignoring record because of invalid title. Title: " + storyDetail.getTitle());
					continue;
				}

				if (source.get(Constants.UID) instanceof Integer)
					storyDetail.setUid(((Integer) source.get(Constants.UID)).toString());
				else
					storyDetail.setUid((String) source.get(Constants.UID));
				
				if (source.get(Constants.CHANNEL_SLNO) instanceof Integer)
					storyDetail.setChannel_slno(String.valueOf(source.get(Constants.CHANNEL_SLNO)));
				else
					storyDetail.setChannel_slno((String) source.get(Constants.CHANNEL_SLNO));
				
				if (source.get(Constants.PGTOTAL) instanceof Integer)
					slideCount = (Integer) source.get(Constants.PGTOTAL);
				else
					slideCount = Integer.valueOf((String) source.get(Constants.PGTOTAL));
				
				

				Sum totalPvs = story.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				storyDetail.setTotalpvs(tpvs);

				Sum totalUvs = story.getAggregations().get("tuvs");
				tuvs = Double.valueOf(totalUvs.getValue()).longValue();
				storyDetail.setTotaluvs(tuvs);

				

				// To calculate pvContribution for story detail page calculate
				// total pvs of site upto current time
				// from the pub_date of that story

				if (query.isStoryDetail()) {
					SearchResponse totalRes = client.prepareSearch(indexName)
							.setTypes(MappingTypes.MAPPING_REALTIME)
							.setQuery(QueryBuilders.boolQuery()
									.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
									.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList))
									.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
											.gte((String) source.get(Constants.STORY_PUBLISH_TIME)).lte(
													DateUtil.getCurrentDateTime())))
							.addAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.addAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS)).setSize(0).execute()
							.actionGet();

					Sum totalSitePvs = totalRes.getAggregations().get("tpvs");
					sitePvs =(long)totalSitePvs.getValue();
					Sum totalSiteUvs = totalRes.getAggregations().get("tuvs");
					siteUvs = (long)totalSiteUvs.getValue();
				}

				if (sitePvs != 0) {
					pvContribution = (long) ((tpvs.doubleValue() / sitePvs) * 100);
				}

				if (siteUvs != 0) {
					uvContribution = ((long) (tuvs.doubleValue() / siteUvs) * 100);
				}

				storyDetail.setPvContribution(pvContribution);
				storyDetail.setUvContribution(uvContribution);
				storyDetail.setSlideCount(slideCount);
				Terms hostType = story.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum uvs = host.getAggregations().get("uvs");
					if (host.getKey().equals(HostType.MOBILE)) {
						mpvs = (long) pvs.getValue();
						muvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.WEB)) {
						wpvs = (long) pvs.getValue();
						wuvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.ANDROID)) {
						apvs = (long) pvs.getValue();
						auvs = (long) uvs.getValue();
					} else if (host.getKey().equals(HostType.IPHONE)) {
						ipvs = (long) pvs.getValue();
						iuvs = (long) uvs.getValue();
					}
				}

				storyDetail.setMpvs(mpvs);
				storyDetail.setWpvs(wpvs);
				storyDetail.setApvs(apvs);
				storyDetail.setIpvs(ipvs);
				storyDetail.setMuvs(muvs);
				storyDetail.setWuvs(wuvs);
				storyDetail.setAuvs(auvs);
				storyDetail.setIuvs(iuvs);
				if (query.getHost_type().contains(HostType.WEB)) {
					tuvs = wuvs + muvs;
				} else {
					tuvs = auvs + iuvs;
				}

				storyDetail.setTotaluvs(tuvs);

				// formula for slide depth: (pv/(uv*slide))*100

				if (slideCount != 0) {
					if (wuvs != 0)
						wSlideDepth = (long) ((Long.valueOf(wpvs).doubleValue() / (wuvs * slideCount)) * 100);
					if (muvs != 0)
						mSlideDepth = (long) ((Long.valueOf(mpvs).doubleValue() / (muvs * slideCount)) * 100);
					if (auvs != 0)
						aSlideDepth = (long) ((Long.valueOf(apvs).doubleValue() / (auvs * slideCount)) * 100);
					if (iuvs != 0)
						iSlideDepth = (long) ((Long.valueOf(ipvs).doubleValue() / (iuvs * slideCount)) * 100);
					if (tuvs != 0)
						slideDepth = (long) ((Long.valueOf(tpvs).doubleValue() / (tuvs * slideCount)) * 100);
				}

				storyDetail.setwSlideDepth(wSlideDepth);
				storyDetail.setmSlideDepth(mSlideDepth);
				storyDetail.setaSlideDepth(aSlideDepth);
				storyDetail.setiSlideDepth(iSlideDepth);
				storyDetail.setSlideDepth(slideDepth);
				Terms traffic = story.getAggregations().get("sourceTraffic");
				for (Terms.Bucket trafficS : traffic.getBuckets()) {
					Map<String, Long> map = new HashMap<>();
					Sum pvs = trafficS.getAggregations().get("pvs");
					Sum uvs = trafficS.getAggregations().get("uvs");
					map.put("pvs", (long) pvs.getValue());
					map.put("uvs", (long) uvs.getValue());
					sourceTraffic.put(trafficS.getKeyAsString(), map);
				}
				Terms pvTracker = story.getAggregations().get("pageViewsTracker");
				for (Terms.Bucket tracker : pvTracker.getBuckets()) {

					Map<String, Long> map = new HashMap<>();
					if (pageViewsTracker.size() < query.getElementCount()) {
						Sum pvs = tracker.getAggregations().get("pvs");
						Sum uvs = tracker.getAggregations().get("uvs");
						map.put("pvs", (long) pvs.getValue());
						map.put("uvs", (long) uvs.getValue());
						pageViewsTracker.put(tracker.getKeyAsString(), map);
					}
					if (tracker.getKeyAsString().equalsIgnoreCase(Constants.NEWS_UCB)) {
						storyDetail.setIsUCB(Boolean.TRUE);
					}
				}

				multiSearchRequestBuilder
				.add(client.prepareSearch(Indexes.FB_DASHBOARD).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(QueryBuilders.termQuery(Constants.BHASKARSTORYID, storyid)));
				storyDetail.setSourceWiseTraffic(sourceTraffic);
				storyDetail.setPageViewsTracker(pageViewsTracker);
				records.add(storyDetail);
			} catch (Exception e) {
				e.printStackTrace();
				log.warn("Ignoring story " + story.getKey() + ", Caused by: " + e.getMessage());
				continue;
			}
		}

		/*Map<String, WisdomArticleRating> op = getAvgArticleRating(storyList);
		for (StoryDetail story : records) {
			String id = story.getStoryid();
			story.setRating(op.get(id));
		}*/

		// if exclude tracker have any tracker of facebook(i.e socialTracker)
		// then don't set isFacebook to true.
		if (query.getExcludeTracker() != null) {
			List<String> socialTracker = query.getSocialTracker();
			for (String excludeTracker : query.getExcludeTracker()) {
				if (socialTracker.contains(excludeTracker)) {
					setFacebookFlag = false;
					break;
				}
			}

		}

		// Execute query for isFacebook Flag
		if (setFacebookFlag && records.size() > 0) {

			MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
			int storyIndex = 0;
			for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
				SearchResponse response = item.getResponse();
				if (response.getHits().getHits().length > 0) {
					records.get(storyIndex).setIsFacebook(Boolean.TRUE);
				}
				storyIndex++;
			}
		}

		log.info("Retrieving top stories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	private BoolQueryBuilder getQuery(WisdomQuery query) {
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();
		if (!StringUtils.isBlank(query.getStartDate())) {
			startDatetime = query.getStartDate();
		}
		if (!StringUtils.isBlank(query.getEndDate())) {
			endDatetime = query.getEndDate();
		}
		if (query.getHost_type() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
		}

		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			if (DateUtil.getNumDaysBetweenDates(query.getStartPubDate(),
					query.getEndPubDate()) > Constants.WisdomNextConstant.CALENDAR_LIMIT) {
				throw new DBAnalyticsException(
						"Date range more than " + Constants.WisdomNextConstant.CALENDAR_LIMIT + " days");
			}
			boolQuery.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(query.getStartPubDate())
					.lte(query.getEndPubDate()));
		} else if (query.isEnableDate()) {
			if (DateUtil.getNumDaysBetweenDates(startDatetime,
					endDatetime) > Constants.WisdomNextConstant.CALENDAR_LIMIT) {
				throw new DBAnalyticsException(
						"Date range more than " + Constants.WisdomNextConstant.CALENDAR_LIMIT + " days");
			}
			boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(startDatetime).lte(endDatetime));
		}
		if (query.getUid() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
		}
		if (query.getHost() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
		}
		if (query.getChannel_slno() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		}
		if (query.getTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getTracker()) {
				trackersBoolQuery.should(QueryBuilders.matchQuery(Constants.TRACKER_SIMPLE, tracker));
			}
			boolQuery.must(trackersBoolQuery);
			// boolQuery.must(QueryBuilders.termsQuery(Constants.TRACKER,
			// query.getTracker()));
		}
		if (query.getExcludeTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getExcludeTracker()) {
				trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
			}
			boolQuery.mustNot(trackersBoolQuery);
			// boolQuery.mustNot(QueryBuilders.termsQuery(Constants.TRACKER,
			// query.getExcludeTracker()));
		}

		if (!StringUtils.isBlank(query.getStoryid())) {
			boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
		}
		if (query.getCat_id() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, query.getCat_id()));
		}
		if (query.getPcat_id() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.PARENT_CAT_ID_FIELD, query.getPcat_id()));
		}
		if (query.getStory_attribute() != null) {
			boolQuery.must(QueryBuilders.termQuery(Constants.STORY_ATTRIBUTE, query.getStory_attribute()));
		}
		if (query.getSuper_cat_id() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}	
		if (query.getSuper_cat_name() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_NAME, query.getSuper_cat_name()));
		}	

		if (query.getFlag_v() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.FLAG_V, query.getFlag_v()));
		}
		return boolQuery;
	}

	/*
	 * public FacebookInsightsPage getFacebookInsights(WisdomQuery query) { long
	 * startTime = System.currentTimeMillis(); FacebookInsightsPage
	 * facebookInsightsPage = new FacebookInsightsPage(); Integer
	 * totalTotal_reach = 0;
	 * 
	 * Integer totalUnique_reach = 0;
	 * 
	 * Integer totalShares = 0;
	 * 
	 * Integer totalLink_clicks = 0;
	 * 
	 * List<FacebookInsights> records = new ArrayList<>(); try { String
	 * startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-"); String
	 * endDatetime = DateUtil.getCurrentDateTime(); if
	 * (!StringUtils.isBlank(query.getStartDate())) { startDatetime =
	 * query.getStartDate(); } if (!StringUtils.isBlank(query.getEndDate())) {
	 * endDatetime = query.getEndDate(); }
	 * 
	 * BoolQueryBuilder qb = QueryBuilders.boolQuery()
	 * .must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).
	 * lte(endDatetime)) .mustNot(QueryBuilders.termQuery(Constants.TITLE, ""))
	 * .must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO,
	 * query.getChannel_slno()));
	 * 
	 * // Apply category filtering if needed if (query.getCat_id() != null &&
	 * query.getCat_id().size()>0) {
	 * qb.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD,
	 * query.getCat_id())); }
	 * 
	 * else if (query.getGa_cat_name() != null &&
	 * query.getGa_cat_name().size()>0) {
	 * qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.
	 * GA_CAT_NAME, query.getGa_cat_name())); }
	 * 
	 * else if(query.getPp_cat_name() != null &&
	 * query.getPp_cat_name().size()>0) {
	 * qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.
	 * PP_CAT_NAME, query.getPp_cat_name())); }
	 * 
	 * if(query.getStatus_type()!=null){
	 * qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE,
	 * query.getStatus_type())); } FilteredQueryBuilder fqb =
	 * QueryBuilders.filteredQuery(qb,
	 * FilterBuilders.existsFilter(Constants.BHASKARSTORYID));
	 * 
	 * SearchResponse res =
	 * client.prepareSearch(Indexes.FB_DASHBOARD).setTypes(MappingTypes.
	 * MAPPING_REALTIME) .setQuery(qb) .addSort(Constants.TOTAL_REACH,
	 * SortOrder.DESC).setSize(query.getCount()).execute().actionGet();
	 * SearchHit[] searchHits= res.getHits().getHits();
	 * 
	 * for (SearchHit hit : searchHits) { try{ FacebookInsights story =
	 * gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
	 * Map<String, Object> source = hit.getSource(); totalLink_clicks +=
	 * (Integer) source.get(Constants.LINK_CLICKS); totalTotal_reach +=
	 * (Integer) source.get(Constants.TOTAL_REACH); totalUnique_reach +=
	 * (Integer) source.get(Constants.UNIQUE_REACH); totalShares += (Integer)
	 * source.get(Constants.SHARES); if ((Integer)
	 * source.get(Constants.LINK_CLICKS) == 0 || (Integer)
	 * source.get(Constants.TOTAL_REACH) == 0) story.setCtr(0.0); else
	 * story.setCtr((((Integer) source.get(Constants.LINK_CLICKS)).doubleValue()
	 * / (Integer) source.get(Constants.TOTAL_REACH)) * 100.0); //
	 * story.setCreated_datetime(source.get(Constants.CREATED_DATETIME).toString
	 * ()); if(source.get(Constants.BHASKARSTORYID) instanceof Integer){
	 * story.setStoryid(((Integer)source.get(Constants.BHASKARSTORYID)).toString
	 * ()); records.add(story); } else
	 * if(!StringUtils.isBlank((String)source.get(Constants.BHASKARSTORYID))){
	 * story.setStoryid(source.get(Constants.BHASKARSTORYID).toString());
	 * records.add(story); } else { story.setStoryid("0"); records.add(story);
	 * log.warn("bhaskarstoryid missing for fb_storyid: "+hit.getId()); }
	 * if(story.getBhaskarstoryid()==null){
	 * log.warn("bhaskarstoryid missing for fb_storyid: "+hit.getId());
	 * story.setBhaskarstoryid("0"); } records.add(story);
	 * 
	 * } catch (Exception e){
	 * log.warn("Some issue found in FacebookInsights API. Record: "+hit.
	 * getSourceAsString()); continue; }
	 * 
	 * }
	 * 
	 * facebookInsightsPage.setStories(records);
	 * facebookInsightsPage.setTotalLink_clicks(totalLink_clicks);
	 * facebookInsightsPage.setTotalTotal_reach(totalTotal_reach);
	 * facebookInsightsPage.setTotalUnique_reach(totalUnique_reach);
	 * facebookInsightsPage.setTotalShares(totalShares); if (totalLink_clicks ==
	 * 0 || totalTotal_reach == 0) { facebookInsightsPage.setTotalCtr(0.0); }
	 * else { facebookInsightsPage.setTotalCtr((totalLink_clicks.doubleValue() /
	 * totalTotal_reach) * 100); } } catch (Exception e) { e.printStackTrace();
	 * log.error("Error while retrieving facebook insights.", e); }
	 * log.info("Facebook insights: Result Size :"+facebookInsightsPage.
	 * getStories().size()+" Execution Time:(Seconds) " +
	 * (System.currentTimeMillis() - startTime) / 1000.0); return
	 * facebookInsightsPage; }
	 */

	/*public FacebookInsightsPage getFacebookInsights(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		FacebookInsightsPage facebookInsightsPage = new FacebookInsightsPage();
		String parameter = query.getParameter();
		Integer totalTotal_reach = 0;

		Integer totalUnique_reach = 0;

		Integer totalShares = 0;

		Integer totalLink_clicks = 0;

		List<FacebookInsights> records = new ArrayList<>();
		try {
			String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endDatetime = DateUtil.getCurrentDateTime();
			if (!StringUtils.isBlank(query.getStartDate())) {
				startDatetime = query.getStartDate();
			}
			if (!StringUtils.isBlank(query.getEndDate())) {
				endDatetime = query.getEndDate();
			}

			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).lte(endDatetime))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));

			if(!query.getStatus_type().equalsIgnoreCase("photo")){
				qb.mustNot(QueryBuilders.termQuery(Constants.TITLE, ""));
			}

			// Apply category filtering if needed
			if (query.getCat_id() != null && query.getCat_id().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, query.getCat_id()));
			}
			if (query.getSuper_cat_id() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
			} 
			else if (query.getGa_cat_name() != null && query.getGa_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
			}

			else if (query.getPp_cat_name() != null && query.getPp_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
			}

			if (query.getStatus_type() != null) {
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}

			if (query.getProfile_id() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.PROFILE_ID, query.getProfile_id()));
			}

			if (query.getUrl_domain() != null) {
				qb.must(QueryBuilders.termQuery(Constants.URL_DOMAIN, query.getUrl_domain()));
			}

			if (query.getType() != null) {
				if (query.getType() == 1) {
					qb.must(QueryBuilders.termQuery(Constants.IA_FLAG, "1"));
				} else if (query.getType() == 0) {
					qb.mustNot(QueryBuilders.termQuery(Constants.IA_FLAG, "1"));
				}
			}

			// QueryBuilder fqb = qb.filter(QueryBuilders.existsQuery(Constants.BHASKARSTORYID));			
			ArrayList<String> storyids = new ArrayList<>();
			SearchResponse res = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.FB_INSIGHTS_HISTORY, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD,
									SortOrder.DESC))
							.subAggregation(AggregationBuilders.max(parameter).field(parameter)).size(query.getCount())
							.order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();

			//MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
			Terms resTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket resBuckets : resTermAgg.getBuckets()) {

				TopHits topHits = resBuckets.getAggregations().get("top");
				SearchHit[] searchHits = topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					try {
						FacebookInsights story = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
						double unique_reach = story.getUnique_reach();
						int shares = story.getShares();
						double sharability = 0.0;
						if (unique_reach > 0) {
							sharability = (shares / unique_reach) * 100;
						}
						story.setShareability(sharability);
						Map<String, Object> source = hit.getSource();
						totalLink_clicks += (Integer) source.get(Constants.LINK_CLICKS);
						totalTotal_reach += (Integer) source.get(Constants.TOTAL_REACH);
						totalUnique_reach += (Integer) source.get(Constants.UNIQUE_REACH);
						totalShares += (Integer) source.get(Constants.SHARES);
						if ((Integer) source.get(Constants.LINK_CLICKS) == 0
								|| (Integer) source.get(Constants.TOTAL_REACH) == 0)
							story.setCtr(0.0);
						else
							story.setCtr((((Integer) source.get(Constants.LINK_CLICKS)).doubleValue()
									/ (Integer) source.get(Constants.TOTAL_REACH)) * 100.0);

						// preparing multiSearch Request Builder
						BoolQueryBuilder sessQery = new BoolQueryBuilder();
						sessQery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD,
								source.get(Constants.BHASKARSTORYID)));

						storyids.add(story.getBhaskarstoryid());
						// story.setCreated_datetime(source.get(Constants.CREATED_DATETIME).toString());
						
						 * if(source.get(Constants.BHASKARSTORYID) instanceof Integer){
						 * story.setStoryid(((Integer)source.get(Constants.
						 * BHASKARSTORYID)).toString()); records.add(story); } else
						 * if(!StringUtils.isBlank((String)source.get(Constants. BHASKARSTORYID))){
						 * story.setStoryid(source.get(Constants.BHASKARSTORYID) .toString());
						 * records.add(story); } else { story.setStoryid("0"); records.add(story);
						 * log.warn("bhaskarstoryid missing for fb_storyid: " +hit.getId()); }
						 
						if (story.getBhaskarstoryid() == null) {
							log.warn("bhaskarstoryid missing for fb_storyid: " + hit.getId());
							story.setBhaskarstoryid("0");
						}
						records.add(story);

					} catch (Exception e) {
						log.warn("Some issue found in FacebookInsights API. Record: " + hit.getSourceAsString());
						continue;
					}
				}
			}

			BoolQueryBuilder sessionsQb = new BoolQueryBuilder();

			if(storyids!=null) {
				sessionsQb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyids));
			}			

			if(query.getTracker()!=null){
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getTracker()) {
					trackersBoolQuery.should(QueryBuilders.matchQuery(Constants.TRACKER_SIMPLE, tracker));
				}

				sessionsQb.must(trackersBoolQuery);
			}
			sessionsQb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate()).lte(query.getEndDate()));
			//sessionsQb.must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, query.getTracker()));		

			int size = 10;
			if(storyids.size()>0)
			{
				size = storyids.size();
			}
			SearchResponse sr = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(sessionsQb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.sum(Constants.UPVS).field(Constants.UVS))
							.size(size)).setSize(0).execute().actionGet();

			Terms storiesTermAgg = sr.getAggregations().get("storyid");					
			Map<String, Long> sessionsMap = new HashMap<>();


			for (Terms.Bucket resBuckets : storiesTermAgg.getBuckets()) {
				String storyid = resBuckets.getKeyAsString();
				Sum tupvs = resBuckets.getAggregations().get(Constants.UPVS);

				long sessions = (long) tupvs.getValue();					
				sessionsMap.put(storyid, sessions);					
			}		
			
			for (FacebookInsights facebookStory : records) {				

				if(sessionsMap.get(facebookStory.getBhaskarstoryid())!=null) {						
					facebookStory.setSessions(sessionsMap.get(facebookStory.getBhaskarstoryid()));
				}
				else {
					facebookStory.setSessions(0L);
				}

			}					
			records.sort(new Comparator<FacebookInsights>() {
				@Override				
				public int compare(FacebookInsights o1, FacebookInsights o2) {					
					int res=Integer.MIN_VALUE;					
					if((o1.getSessions()!=null) && o2.getSessions()!=null) {
						res = -o1.getSessions().compareTo(o2.getSessions());					
					}
					return res;
				}
			});


			facebookInsightsPage.setStories(records);
			facebookInsightsPage.setTotalLink_clicks(totalLink_clicks);
			facebookInsightsPage.setTotalTotal_reach(totalTotal_reach);
			facebookInsightsPage.setTotalUnique_reach(totalUnique_reach);
			facebookInsightsPage.setTotalShares(totalShares);
			if (totalLink_clicks == 0 || totalTotal_reach == 0) {
				facebookInsightsPage.setTotalCtr(0.0);
			} else {
				facebookInsightsPage.setTotalCtr((totalLink_clicks.doubleValue() / totalTotal_reach) * 100);
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebook insights.", e);
		}
		log.info("Facebook insights: Result Size :" + facebookInsightsPage.getStories().size()
				+ " Execution Time:(Seconds) " + (System.currentTimeMillis() - startTime) / 1000.0);
		return facebookInsightsPage;
	}*/
	
	public List<FacebookInsights> getFacebookInsights(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<FacebookInsights> records = new ArrayList<>();
		try {			
			QueryBuilder qb = getSocialDecodeQuery(query);
			ArrayList<String> storyids = new ArrayList<>();
			SortOrder order;
			if(query.isOrderAsc()){
				order = SortOrder.ASC;
			}
			else{
				order = SortOrder.DESC;
			}
			SearchResponse res = client
					.prepareSearch(Indexes.FB_DASHBOARD)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
					.setSize(query.getCount())
					.addSort(query.getParameter(), order)
					.execute().actionGet();

			SearchHit[] searchHits = res.getHits().getHits();

			for (SearchHit hit : searchHits) {
				FacebookInsights story = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
				double unique_reach = story.getUnique_reach();
				int shares = story.getShares();
				double sharability = 0.0;
				if (unique_reach > 0) {
					sharability = (shares / unique_reach) * 100;
				}
				story.setShareability(sharability);
				if (story.getLink_clicks() == 0||story.getUnique_reach()== 0)
					story.setCtr(0.0);
				else
					story.setCtr(((story.getLink_clicks()).doubleValue()
							/ story.getUnique_reach())* 100.0);

				storyids.add(story.getBhaskarstoryid());
				records.add(story);
			}

			BoolQueryBuilder sessionsQb = new BoolQueryBuilder();

			if(storyids!=null) {
				sessionsQb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyids));
			}			

			if(query.getTracker()!=null){
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getTracker()) {
					trackersBoolQuery.should(QueryBuilders.matchQuery(Constants.TRACKER_SIMPLE, tracker));
				}
				sessionsQb.must(trackersBoolQuery);
			}
			int size = 10;
			if(storyids.size()>0)
			{
				size = storyids.size();
			}
			SearchResponse sr = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate()))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(sessionsQb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.sum(Constants.UPVS).field(Constants.UVS))
							.subAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS))
							.size(size)).setSize(0).execute().actionGet();

			Terms storiesTermAgg = sr.getAggregations().get("storyid");					
			Map<String, Map<String, Object>> storiesData = new HashMap<>();
			for (Terms.Bucket resBuckets : storiesTermAgg.getBuckets()) {
				Map<String, Object> storyMap = new HashMap<>();
				String storyid = resBuckets.getKeyAsString();
				Sum upvs_agg = resBuckets.getAggregations().get(Constants.UPVS);
				Sum pvs_agg = resBuckets.getAggregations().get(Constants.PVS);
				long upvs = (long) upvs_agg.getValue();	
				long pvs = (long) pvs_agg.getValue();
				storyMap.put(Constants.UPVS, upvs);	
				storyMap.put(Constants.PVS, pvs);
				storiesData.put(storyid, storyMap);
			}	

			for (FacebookInsights facebookStory : records) {
				if(storiesData.get(facebookStory.getBhaskarstoryid())!=null) {	
					Map<String, Object> storyMap = storiesData.get(facebookStory.getBhaskarstoryid());
					facebookStory.setSessions((Long)storyMap.get(Constants.UPVS));
					facebookStory.setPvs((Long)storyMap.get(Constants.PVS));					
				}
				else {
					facebookStory.setSessions(0L);
					facebookStory.setPvs(0L);					
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebook insights.", e);
		}
		log.info("Facebook insights Execution Time:(Seconds) " + (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public FacebookInsightsPage getFacebookInsightsForDate(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		FacebookInsightsPage facebookInsightsPage = new FacebookInsightsPage();
		String parameter = query.getParameter();
		Integer totalTotal_reach = 0;

		Integer totalUnique_reach = 0;

		Integer totalShares = 0;

		Integer totalLink_clicks = 0;

		List<FacebookInsights> records = new ArrayList<>();
		try {
			String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endDatetime = DateUtil.getCurrentDateTime();
			if (!StringUtils.isBlank(query.getStartDate())) {
				startDatetime = query.getStartDate();
			}
			if (!StringUtils.isBlank(query.getEndDate())) {
				endDatetime = query.getEndDate();
			}

			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).lte(endDatetime))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));

			if(query.getStatus_type()!=null&&!query.getStatus_type().equalsIgnoreCase("photo")){
				qb.mustNot(QueryBuilders.termQuery(Constants.TITLE, ""));
			}

			// Apply category filtering if needed
			if (query.getCat_id() != null && query.getCat_id().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, query.getCat_id()));
			}
			if (query.getSuper_cat_id() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
			}
			else if (query.getGa_cat_name() != null && query.getGa_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
			}

			else if (query.getPp_cat_name() != null && query.getPp_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
			}

			if (query.getStatus_type() != null) {
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}

			if (query.getProfile_id() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.PROFILE_ID, query.getProfile_id()));
			}

			if (query.getUrl_domain() != null) {
				qb.must(QueryBuilders.termQuery(Constants.URL_DOMAIN, query.getUrl_domain()));
			}
			QueryBuilder fqb = qb.filter(
					QueryBuilders.existsQuery(Constants.BHASKARSTORYID));

			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
					.addAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
					.addAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
					.addAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
					.addAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))	
					//.addAggregation(AggregationBuilders.filter("filter", QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Constants.TITLE, "")))
							.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
									.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
									.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
									.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
									.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
											.field(Constants.REACTION_THANKFUL))
									.subAggregation(
											AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
									.subAggregation(
											AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
									.subAggregation(
											AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
									.subAggregation(
											AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
									.subAggregation(
											AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
									.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
									.subAggregation(
											AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
											.field(Constants.REPORT_SPAM_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
									.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
									.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
									.subAggregation(AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.VIDEO_VIEWS).field(Constants.VIDEO_VIEWS))
									.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_VIDEO_VIEWS)
											.field(Constants.UNIQUE_VIDEO_VIEWS))
									.size(query.getCount()).order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();
			
			Sum total_total_reach_agg = res.getAggregations().get(Constants.TOTAL_REACH);
			totalTotal_reach = (int) total_total_reach_agg.getValue();

			Sum total_unique_reach_agg = res.getAggregations().get(Constants.UNIQUE_REACH);
			totalUnique_reach = (int) total_unique_reach_agg.getValue();

			Sum total_link_clicks_agg = res.getAggregations().get(Constants.LINK_CLICKS);
			totalLink_clicks = (int) total_link_clicks_agg.getValue();

			Sum total_shares_agg = res.getAggregations().get(Constants.SHARES);
			totalShares = (int) total_shares_agg.getValue();
			
			//Filter filter_agg = res.getAggregations().get("filter");

			List<String> storyids = new ArrayList<>();
			Terms storyidsTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket bucket : storyidsTermAgg.getBuckets()) {

				TopHits topHits = bucket.getAggregations().get("top");
				SearchHit hit = topHits.getHits().getHits()[0];
				FacebookInsights insights = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
				storyids.add(insights.getBhaskarstoryid());
				
				Sum total_reach_agg = bucket.getAggregations().get(Constants.TOTAL_REACH);
				insights.setTotal_reach((long) total_reach_agg.getValue());
				//totalTotal_reach += (long) total_reach_agg.getValue();

				Sum unique_reach_agg = bucket.getAggregations().get(Constants.UNIQUE_REACH);
				insights.setUnique_reach((long) unique_reach_agg.getValue());
				//totalUnique_reach += (long) unique_reach_agg.getValue();

				Sum link_clicks_agg = bucket.getAggregations().get(Constants.LINK_CLICKS);
				insights.setLink_clicks((long) link_clicks_agg.getValue());
				//totalLink_clicks += (long) link_clicks_agg.getValue();

				Sum reaction_angry_agg = bucket.getAggregations().get(Constants.REACTION_ANGRY);
				insights.setReaction_angry((int) reaction_angry_agg.getValue());

				Sum reaction_haha_agg = bucket.getAggregations().get(Constants.REACTION_HAHA);
				insights.setReaction_haha((int) reaction_haha_agg.getValue());

				Sum reaction_love_agg = bucket.getAggregations().get(Constants.REACTION_LOVE);
				insights.setReaction_love((int) reaction_love_agg.getValue());

				Sum reaction_sad_agg = bucket.getAggregations().get(Constants.REACTION_SAD);
				insights.setReaction_sad((int) reaction_sad_agg.getValue());

				Sum reaction_thankful_agg = bucket.getAggregations().get(Constants.REACTION_THANKFUL);
				insights.setReaction_thankful((int) reaction_thankful_agg.getValue());

				Sum reaction_wow_agg = bucket.getAggregations().get(Constants.REACTION_WOW);
				insights.setReaction_wow((int) reaction_wow_agg.getValue());

				Sum hide_all_clicks_agg = bucket.getAggregations().get(Constants.HIDE_ALL_CLICKS);
				insights.setHide_all_clicks((int) hide_all_clicks_agg.getValue());

				Sum hide_clicks_agg = bucket.getAggregations().get(Constants.HIDE_CLICKS);
				insights.setHide_clicks((int) hide_clicks_agg.getValue());

				Sum report_spam_clicks_agg = bucket.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
				insights.setReport_spam_clicks((int) report_spam_clicks_agg.getValue());

				Sum likes_agg = bucket.getAggregations().get(Constants.LIKES);
				insights.setLikes((int) likes_agg.getValue());

				Sum comments_agg = bucket.getAggregations().get(Constants.COMMENTS);
				insights.setComments((int) comments_agg.getValue());

				Sum shares_agg = bucket.getAggregations().get(Constants.SHARES);
				insights.setShares((int) shares_agg.getValue());
				//totalShares += (int) shares_agg.getValue();

				Sum ia_clicks_agg = bucket.getAggregations().get(Constants.IA_CLICKS);
				insights.setIa_clicks((int) ia_clicks_agg.getValue());

				Sum video_views_agg = bucket.getAggregations().get(Constants.VIDEO_VIEWS);
				insights.setVideo_views((int) video_views_agg.getValue());

				Sum unique_video_views_agg = bucket.getAggregations().get(Constants.UNIQUE_VIDEO_VIEWS);
				insights.setUnique_video_views((int) unique_video_views_agg.getValue());

				if ((int) total_reach_agg.getValue() == 0
						|| (int) link_clicks_agg.getValue() == 0)
					insights.setCtr(0.0);
				else
					insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);
				if (insights.getBhaskarstoryid() == null) {
					log.warn("bhaskarstoryid missing for fb_storyid: " + hit.getId());
					insights.setBhaskarstoryid("0");
				}
				records.add(insights);
			}
			
			BoolQueryBuilder sessionsQb = new BoolQueryBuilder();

			if(storyids!=null) {
				sessionsQb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyids));
			}			

			if(query.getTracker()!=null){
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getTracker()) {
					trackersBoolQuery.should(QueryBuilders.matchQuery(Constants.TRACKER_SIMPLE, tracker));
				}

				sessionsQb.must(trackersBoolQuery);
			}
			sessionsQb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate()).lte(query.getEndDate()));
			//sessionsQb.must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, query.getTracker()));		

			int size = 10;
			if(storyids.size()>0)
			{
				size = storyids.size();
			}
			SearchResponse sr = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(sessionsQb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.sum(Constants.UPVS).field(Constants.UVS))
							.size(size)).setSize(0).execute().actionGet();

			Terms storiesTermAgg = sr.getAggregations().get("storyid");					
			Map<String, Long> sessionsMap = new HashMap<>();


			for (Terms.Bucket resBuckets : storiesTermAgg.getBuckets()) {
				String storyid = resBuckets.getKeyAsString();
				Sum tupvs = resBuckets.getAggregations().get(Constants.UPVS);

				long sessions = (long) tupvs.getValue();					
				sessionsMap.put(storyid, sessions);					
			}		


			for (FacebookInsights facebookStory : records) {				

				if(sessionsMap.get(facebookStory.getBhaskarstoryid())!=null) {						
					facebookStory.setSessions(sessionsMap.get(facebookStory.getBhaskarstoryid()));
				}
				else {
					facebookStory.setSessions(0L);
				}

			}					
			records.sort(new Comparator<FacebookInsights>() {
				@Override				
				public int compare(FacebookInsights o1, FacebookInsights o2) {					
					int res=Integer.MIN_VALUE;					
					if((o1.getSessions()!=null) && o2.getSessions()!=null) {
						res = -o1.getSessions().compareTo(o2.getSessions());					
					}
					return res;
				}
			});


			facebookInsightsPage.setStories(records);
			facebookInsightsPage.setTotalLink_clicks(totalLink_clicks);
			facebookInsightsPage.setTotalTotal_reach(totalTotal_reach);
			facebookInsightsPage.setTotalUnique_reach(totalUnique_reach);
			facebookInsightsPage.setTotalShares(totalShares);
			if (totalLink_clicks == 0 || totalTotal_reach == 0) {
				facebookInsightsPage.setTotalCtr(0.0);
			} else {
				facebookInsightsPage.setTotalCtr((totalLink_clicks.doubleValue() / totalTotal_reach) * 100);
			}			
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFacebookInsightsForDate.", e);
		}
		log.info("getFacebookInsightsForDate: Result Size :" + facebookInsightsPage.getStories().size()
				+ " Execution Time:(Seconds) " + (System.currentTimeMillis() - startTime) / 1000.0);
		return facebookInsightsPage;
	}

	public List<TrendingEntities> getTrendingEntities(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<TrendingEntities> records = new ArrayList<>();
		try {
			String field = query.getField();
			String indexName = "realtime_" + DateUtil.getCurrentDate();
			BoolQueryBuilder boolQuery = getQuery(query);
			SearchResponse res = client.prepareSearch(indexName)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("trends").field(field).size(query.getCount())).setSize(0)
					.execute().actionGet();
			Terms trendingBuckets = res.getAggregations().get("trends");
			for (Terms.Bucket trend : trendingBuckets.getBuckets()) {
				TrendingEntities entity = new TrendingEntities();
				if (!StringUtils.isBlank(trend.getKeyAsString()) && !trend.getKeyAsString().equalsIgnoreCase("0")) {
					entity.setName(trend.getKeyAsString());
					entity.setClicks(trend.getDocCount());
					records.add(entity);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving trending entities.", e);
		}
		log.info("Retrieving trending entities; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	private BoolQueryBuilder getSocialDecodeQuery(WisdomQuery query) {
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery();

		if (query.getGa_cat_name() != null && !query.getGa_cat_name().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
		}

		else if (query.getPp_cat_name() != null && !query.getPp_cat_name().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
		}

		if (query.getStoryid() != null && !query.getStoryid().isEmpty()) {
			mainQuery.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
		} 
		else {
			if(query.getChannel_slno()!=null) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		}
			BoolQueryBuilder dateQuery = QueryBuilders.boolQuery();
			if(query.getStartDate()!=null&&query.getEndDate()!=null){
				dateQuery.should(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			}
			else
			{
				dateQuery.should(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartPubDate()).lte(query.getEndPubDate()));

			}
			if (!query.isPrevDayRequired() && query.getCompareEndDate() != null
					&& query.getCompareStartDate() != null) {
				dateQuery.should(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getCompareStartDate())
						.lte(query.getCompareEndDate()));
			}
			mainQuery.must(dateQuery);
		}

		if (query.getUrl_domain() != null) {
			mainQuery.must(QueryBuilders.termQuery(Constants.URL_DOMAIN, query.getUrl_domain()));
		}

		if (query.getDateField().equals(Constants.CREATED_DATETIME) && query.getProfile_id() != null) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.PROFILE_ID, query.getProfile_id()));
		}
		
		if (query.getSuper_cat_id() != null && !query.getSuper_cat_id().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}
		
		if (query.getSuper_cat_name() != null && !query.getSuper_cat_name().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_NAME, query.getSuper_cat_name()));
		}
		
		if (query.getUid() != null && !query.getUid().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
		}
		
		if (query.getStatus_type() != null) {
			mainQuery.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
		}
		return mainQuery;
	}

	public Map<String, FacebookInsights> getFacebookInsightsByInterval(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, FacebookInsights> records = new LinkedHashMap<>();
		try {

			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);
			Histogram.Order order = null;

			if (query.isOrderAsc())
				order = Histogram.Order.KEY_ASC;
			else
				order = Histogram.Order.KEY_DESC;

			Map<String, Long> popularity = getFacebookPopularity(query);
			// System.out.println(popularity);
			DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("interval").field(query.getDateField())
					.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order)
					.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
					.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
					.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL).field(Constants.REACTION_THANKFUL))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
					.subAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
					.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS).field(Constants.REPORT_SPAM_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
					.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
					.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
					.subAggregation(AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.VIDEO_VIEWS).field(Constants.VIDEO_VIEWS))
					.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_VIDEO_VIEWS).field(Constants.UNIQUE_VIDEO_VIEWS));

			if (query.getStoryid() != null) {
				aggBuilder
				.subAggregation(AggregationBuilders.sum(Constants.POPULAR_COUNT).field(Constants.POPULAR_COUNT))
				.subAggregation(
						AggregationBuilders.sum(Constants.NOT_POPULAR_COUNT).field(Constants.NOT_POPULAR_COUNT))
				.subAggregation(
						AggregationBuilders.sum(Constants.POPULARITY_SCORE).field(Constants.POPULARITY_SCORE));

			}

			else {
				if (query.getInterval().equals("1d")
						&& query.getEndDate().equals(DateUtil.getCurrentDate().replaceAll("_", "-"))
						&& query.isPrevDayRequired()) {
					records.put("prevDayData", getPrevDayFacebookInsights(query));
				}
				aggBuilder
				.subAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD))
				.subAggregation(AggregationBuilders.cardinality("ia_story_count").field(Constants.IA_STORYID))
				.subAggregation(AggregationBuilders.filter("links",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"))
						.subAggregation(
								AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
								.field(Constants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY)
								.field(Constants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS)
								.field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(
								AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
				.subAggregation(AggregationBuilders.filter("photos",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "photo"))
						.subAggregation(
								AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
								.field(Constants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY)
								.field(Constants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS)
								.field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(
								AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
				.subAggregation(AggregationBuilders.filter("videos",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "video"))
						.subAggregation(
								AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
								.field(Constants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY)
								.field(Constants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS)
								.field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count")
								.field(Constants.STORY_ID_FIELD)));

			}

			/*
			 * SearchResponse res =
			 * client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY).setTypes(
			 * MappingTypes.MAPPING_REALTIME)
			 * .setQuery(getSocialDecodeQuery(query)) .addAggregation(
			 * AggregationBuilders.dateHistogram("interval").field(query.
			 * getDateField()).interval(histInterval).order(order)
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * Constants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * Constants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * Constants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(Constants.STORY_ID_FIELD))
			 * .subAggregation(AggregationBuilders.filter("links").filter(
			 * FilterBuilders.termsFilter(Constants.STATUS_TYPE, "link"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * Constants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * Constants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * Constants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(Constants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.filter("photos").filter(
			 * FilterBuilders.termsFilter(Constants.STATUS_TYPE, "photo"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * Constants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * Constants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * Constants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(Constants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.filter("videos").filter(
			 * FilterBuilders.termsFilter(Constants.STATUS_TYPE, "video"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * Constants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * Constants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * Constants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(Constants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.topHits("top").fetchSource
			 * (new String[]{Constants.IS_POPULAR}, new String[] {})
			 * .setSize(1).addSort(query.getDateField(), SortOrder.DESC)) )
			 * .setSize(0).execute().actionGet();
			 */

			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(aggBuilder).setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					FacebookInsights insights = new FacebookInsights();

					Sum total_reach_agg = bucket.getAggregations().get(Constants.TOTAL_REACH);
					insights.setTotal_reach((long) total_reach_agg.getValue());

					Sum unique_reach_agg = bucket.getAggregations().get(Constants.UNIQUE_REACH);
					insights.setUnique_reach((long) unique_reach_agg.getValue());

					Sum link_clicks_agg = bucket.getAggregations().get(Constants.LINK_CLICKS);
					insights.setLink_clicks((long) link_clicks_agg.getValue());

					Sum reaction_angry_agg = bucket.getAggregations().get(Constants.REACTION_ANGRY);
					insights.setReaction_angry((int) reaction_angry_agg.getValue());

					Sum reaction_haha_agg = bucket.getAggregations().get(Constants.REACTION_HAHA);
					insights.setReaction_haha((int) reaction_haha_agg.getValue());

					Sum reaction_love_agg = bucket.getAggregations().get(Constants.REACTION_LOVE);
					insights.setReaction_love((int) reaction_love_agg.getValue());

					Sum reaction_sad_agg = bucket.getAggregations().get(Constants.REACTION_SAD);
					insights.setReaction_sad((int) reaction_sad_agg.getValue());

					Sum reaction_thankful_agg = bucket.getAggregations().get(Constants.REACTION_THANKFUL);
					insights.setReaction_thankful((int) reaction_thankful_agg.getValue());

					Sum reaction_wow_agg = bucket.getAggregations().get(Constants.REACTION_WOW);
					insights.setReaction_wow((int) reaction_wow_agg.getValue());

					Sum hide_all_clicks_agg = bucket.getAggregations().get(Constants.HIDE_ALL_CLICKS);
					insights.setHide_all_clicks((int) hide_all_clicks_agg.getValue());

					Sum hide_clicks_agg = bucket.getAggregations().get(Constants.HIDE_CLICKS);
					insights.setHide_clicks((int) hide_clicks_agg.getValue());

					Sum report_spam_clicks_agg = bucket.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
					insights.setReport_spam_clicks((int) report_spam_clicks_agg.getValue());

					Sum likes_agg = bucket.getAggregations().get(Constants.LIKES);
					insights.setLikes((int) likes_agg.getValue());

					Sum comments_agg = bucket.getAggregations().get(Constants.COMMENTS);
					insights.setComments((int) comments_agg.getValue());

					Sum shares_agg = bucket.getAggregations().get(Constants.SHARES);
					insights.setShares((int) shares_agg.getValue());

					Sum ia_clicks_agg = bucket.getAggregations().get(Constants.IA_CLICKS);
					insights.setIa_clicks((int) ia_clicks_agg.getValue());

					Sum video_views_agg = bucket.getAggregations().get(Constants.VIDEO_VIEWS);
					insights.setVideo_views((int) video_views_agg.getValue());

					Sum unique_video_views_agg = bucket.getAggregations().get(Constants.UNIQUE_VIDEO_VIEWS);
					insights.setUnique_video_views((int) unique_video_views_agg.getValue());

					if (unique_reach_agg.getValue() > 0) {
						insights.setShareability((shares_agg.getValue() / unique_reach_agg.getValue()) * 100);
					}

					if ((int) total_reach_agg.getValue() == 0
							|| (int) link_clicks_agg.getValue() == 0)
						insights.setCtr(0.0);
					else
						insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);

					if (query.getStoryid() != null) {
						Sum popular_agg = bucket.getAggregations().get(Constants.POPULAR_COUNT);
						insights.setPopular_count((int) popular_agg.getValue());

						Sum not_popular_agg = bucket.getAggregations().get(Constants.NOT_POPULAR_COUNT);
						insights.setNot_popular_count((int) not_popular_agg.getValue());

						Sum popularity_score_agg = bucket.getAggregations().get(Constants.POPULARITY_SCORE);
						insights.setPopularity_score((int) popularity_score_agg.getValue());
					}

					else {

						Cardinality story_count = bucket.getAggregations().get("story_count");
						insights.setStory_count((int)story_count.getValue());

						Cardinality ia_story_count = bucket.getAggregations().get("ia_story_count");
						insights.setIa_story_count((int)ia_story_count.getValue());

						Filter link_filter = bucket.getAggregations().get("links");

						Sum link_total_reach = link_filter.getAggregations().get(Constants.TOTAL_REACH);
						insights.setLinks_total_reach((long) link_total_reach.getValue());

						Sum link_unique_reach = link_filter.getAggregations().get(Constants.UNIQUE_REACH);
						insights.setLinks_unique_reach((long) link_unique_reach.getValue());

						Sum link_clicked = link_filter.getAggregations().get(Constants.LINK_CLICKS);
						insights.setLinks_clicked((long) link_clicked.getValue());

						Sum link_reaction_angry = link_filter.getAggregations().get(Constants.REACTION_ANGRY);
						insights.setLinks_reaction_angry((int) link_reaction_angry.getValue());

						Sum link_reaction_haha = link_filter.getAggregations().get(Constants.REACTION_HAHA);
						insights.setLinks_reaction_haha((int) link_reaction_haha.getValue());

						Sum link_reaction_love = link_filter.getAggregations().get(Constants.REACTION_LOVE);
						insights.setLinks_reaction_love((int) link_reaction_love.getValue());

						Sum link_reaction_sad = link_filter.getAggregations().get(Constants.REACTION_SAD);
						insights.setLinks_reaction_sad((int) link_reaction_sad.getValue());

						Sum link_reaction_thankful = link_filter.getAggregations().get(Constants.REACTION_THANKFUL);
						insights.setLinks_reaction_thankful((int) link_reaction_thankful.getValue());

						Sum link_reaction_wow = link_filter.getAggregations().get(Constants.REACTION_WOW);
						insights.setLinks_reaction_wow((int) link_reaction_wow.getValue());

						Sum link_hide_all_clicks = link_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
						insights.setLinks_hide_all_clicks((int) link_hide_all_clicks.getValue());

						Sum link_hide_clicks = link_filter.getAggregations().get(Constants.HIDE_CLICKS);
						insights.setLinks_hide_clicks((int) link_hide_clicks.getValue());

						Sum link_report_spam_clicks = link_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
						insights.setLinks_report_spam_clicks((int) link_report_spam_clicks.getValue());

						Sum link_likes = link_filter.getAggregations().get(Constants.LIKES);
						insights.setLinks_likes((int) link_likes.getValue());

						Sum link_comments = link_filter.getAggregations().get(Constants.COMMENTS);
						insights.setLinks_comments((int) link_comments.getValue());

						Sum link_shares = link_filter.getAggregations().get(Constants.SHARES);
						insights.setLinks_shares((int) link_shares.getValue());

						Cardinality link_count = link_filter.getAggregations().get("story_count");
						insights.setLinks_count((int)link_count.getValue());

						if (link_unique_reach.getValue() > 0) {
							insights.setLinks_shareability((link_shares.getValue() / link_unique_reach.getValue()) * 100);
						}

						if ((int) link_total_reach.getValue() == 0
								|| (int) link_clicked.getValue() == 0)
							insights.setLinks_ctr(0.0);
						else
							insights.setLinks_ctr((link_clicked.getValue() / link_total_reach.getValue()) * 100.0);

						Filter photo_filter = bucket.getAggregations().get("photos");

						Sum photo_total_reach = photo_filter.getAggregations().get(Constants.TOTAL_REACH);
						insights.setPhotos_total_reach((long) photo_total_reach.getValue());

						Sum photo_unique_reach = photo_filter.getAggregations().get(Constants.UNIQUE_REACH);
						insights.setPhotos_unique_reach((long) photo_unique_reach.getValue());

						Sum photo_clicked = photo_filter.getAggregations().get(Constants.LINK_CLICKS);
						insights.setPhotos_clicked((long) photo_clicked.getValue());

						Sum photo_reaction_angry = photo_filter.getAggregations().get(Constants.REACTION_ANGRY);
						insights.setPhotos_reaction_angry((int) photo_reaction_angry.getValue());

						Sum photo_reaction_haha = photo_filter.getAggregations().get(Constants.REACTION_HAHA);
						insights.setPhotos_reaction_haha((int) photo_reaction_haha.getValue());

						Sum photo_reaction_love = photo_filter.getAggregations().get(Constants.REACTION_LOVE);
						insights.setPhotos_reaction_love((int) photo_reaction_love.getValue());

						Sum photo_reaction_sad = photo_filter.getAggregations().get(Constants.REACTION_SAD);
						insights.setPhotos_reaction_sad((int) photo_reaction_sad.getValue());

						Sum photo_reaction_thankful = photo_filter.getAggregations().get(Constants.REACTION_THANKFUL);
						insights.setPhotos_reaction_thankful((int) photo_reaction_thankful.getValue());

						Sum photo_reaction_wow = photo_filter.getAggregations().get(Constants.REACTION_WOW);
						insights.setPhotos_reaction_wow((int) photo_reaction_wow.getValue());

						Sum photo_hide_all_clicks = photo_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
						insights.setPhotos_hide_all_clicks((int) photo_hide_all_clicks.getValue());

						Sum photo_hide_clicks = photo_filter.getAggregations().get(Constants.HIDE_CLICKS);
						insights.setPhotos_hide_clicks((int) photo_hide_clicks.getValue());

						Sum photo_report_spam_clicks = photo_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
						insights.setPhotos_report_spam_clicks((int) photo_report_spam_clicks.getValue());

						Sum photo_likes = photo_filter.getAggregations().get(Constants.LIKES);
						insights.setPhotos_likes((int) photo_likes.getValue());

						Sum photo_comments = photo_filter.getAggregations().get(Constants.COMMENTS);
						insights.setPhotos_comments((int) photo_comments.getValue());

						Sum photo_shares = photo_filter.getAggregations().get(Constants.SHARES);
						insights.setPhotos_shares((int) photo_shares.getValue());

						if (photo_unique_reach.getValue() > 0) {
							insights.setPhotos_shareability(
									(photo_shares.getValue() / photo_unique_reach.getValue()) * 100);
						}

						Cardinality photo_count = photo_filter.getAggregations().get("story_count");
						insights.setPhotos_count((int)photo_count.getValue());

						if ((int) photo_total_reach.getValue() == 0
								|| (int) photo_clicked.getValue() == 0)
							insights.setPhotos_ctr(0.0);
						else
							insights.setPhotos_ctr((photo_clicked.getValue() / photo_total_reach.getValue()) * 100.0);

						Filter video_filter = bucket.getAggregations().get("videos");

						Sum video_total_reach = video_filter.getAggregations().get(Constants.TOTAL_REACH);
						insights.setVideos_total_reach((int) video_total_reach.getValue());

						Sum video_unique_reach = video_filter.getAggregations().get(Constants.UNIQUE_REACH);
						insights.setVideos_unique_reach((int) video_unique_reach.getValue());

						Sum video_clicked = video_filter.getAggregations().get(Constants.LINK_CLICKS);
						insights.setVideos_clicked((int) video_clicked.getValue());

						Sum video_reaction_angry = video_filter.getAggregations().get(Constants.REACTION_ANGRY);
						insights.setVideos_reaction_angry((int) video_reaction_angry.getValue());

						Sum video_reaction_haha = video_filter.getAggregations().get(Constants.REACTION_HAHA);
						insights.setVideos_reaction_haha((int) video_reaction_haha.getValue());

						Sum video_reaction_love = video_filter.getAggregations().get(Constants.REACTION_LOVE);
						insights.setVideos_reaction_love((int) video_reaction_love.getValue());

						Sum video_reaction_sad = video_filter.getAggregations().get(Constants.REACTION_SAD);
						insights.setVideos_reaction_sad((int) video_reaction_sad.getValue());

						Sum video_reaction_thankful = video_filter.getAggregations().get(Constants.REACTION_THANKFUL);
						insights.setVideos_reaction_thankful((int) video_reaction_thankful.getValue());

						Sum video_reaction_wow = video_filter.getAggregations().get(Constants.REACTION_WOW);
						insights.setVideos_reaction_wow((int) video_reaction_wow.getValue());

						Sum video_hide_all_clicks = video_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
						insights.setVideos_hide_all_clicks((int) video_hide_all_clicks.getValue());

						Sum video_hide_clicks = video_filter.getAggregations().get(Constants.HIDE_CLICKS);
						insights.setVideos_hide_clicks((int) video_hide_clicks.getValue());

						Sum video_report_spam_clicks = video_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
						insights.setVideos_report_spam_clicks((int) video_report_spam_clicks.getValue());

						Sum video_likes = video_filter.getAggregations().get(Constants.LIKES);
						insights.setVideos_likes((int) video_likes.getValue());

						Sum video_comments = video_filter.getAggregations().get(Constants.COMMENTS);
						insights.setVideos_comments((int) video_comments.getValue());

						Sum video_shares = video_filter.getAggregations().get(Constants.SHARES);
						insights.setVideos_shares((int) video_shares.getValue());

						if (video_unique_reach.getValue() > 0) {
							insights.setVideos_shareability(
									(video_shares.getValue() / video_unique_reach.getValue()) * 100);
						}

						Cardinality video_count = video_filter.getAggregations().get("story_count");
						insights.setVideos_count((int)video_count.getValue());

						if ((int) video_total_reach.getValue() == 0
								|| (int) video_clicked.getValue() == 0)
							insights.setVideos_ctr(0.0);
						else
							insights.setVideos_ctr((video_clicked.getValue() / video_total_reach.getValue()) * 100.0);
						if (query.getInterval().equals("1d")) {
							insights.setWeekDay(DateUtil.getWeekDay(bucket.getKeyAsString()));
						}
						if (popularity.keySet().contains(bucket.getKeyAsString())) {
							insights.setPopular_count(new Long(popularity.get(bucket.getKeyAsString())).intValue());
						}
					}

					records.put(bucket.getKeyAsString(), insights);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebookInsightsByInterval.", e);
		}
		log.info("Retrieving facebookInsightsByInterval; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	private FacebookInsights getPrevDayFacebookInsights(WisdomQuery query) {
		FacebookInsights insights = new FacebookInsights();
		BoolQueryBuilder qb = QueryBuilders.boolQuery()
				.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
				.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getCompareStartDate())
						.lte(query.getCompareEndDate()));
		if (query.getGa_cat_name() != null && !query.getGa_cat_name().isEmpty()) {
			qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
		}
		if (query.getSuper_cat_id() != null) {
			qb.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}
		else if (query.getPp_cat_name() != null && !query.getPp_cat_name().isEmpty()) {
			qb.must(QueryBuilders.termsQuery(Constants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
		}

		if (query.getDateField().equals(Constants.CREATED_DATETIME) && query.getProfile_id() != null) {
			qb.must(QueryBuilders.termsQuery(Constants.PROFILE_ID, query.getProfile_id()));
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getCompareStartDate())
					.lte(query.getCompareEndDate()));
		}

		SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(qb)
				.addAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
				.addAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL).field(Constants.REACTION_THANKFUL))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
				.addAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
				.addAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
				.addAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
				.addAggregation(
						AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS).field(Constants.REPORT_SPAM_CLICKS))
				.addAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
				.addAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
				.addAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
				.addAggregation(AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))
				.addAggregation(AggregationBuilders.sum(Constants.VIDEO_VIEWS).field(Constants.VIDEO_VIEWS))
				.addAggregation(
						AggregationBuilders.sum(Constants.UNIQUE_VIDEO_VIEWS).field(Constants.UNIQUE_VIDEO_VIEWS))
				.addAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD))
				.addAggregation(AggregationBuilders.cardinality("ia_story_count").field(Constants.IA_STORYID))
				.addAggregation(AggregationBuilders.filter("links",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"))
						.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_THANKFUL).field(Constants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
				.addAggregation(AggregationBuilders.filter("photos",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "photo"))
						.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_THANKFUL).field(Constants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
				.addAggregation(AggregationBuilders.filter("videos",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "video"))
						.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_THANKFUL).field(Constants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
								.field(Constants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
						.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
				.setSize(0).execute().actionGet();
		Sum total_reach_agg = res.getAggregations().get(Constants.TOTAL_REACH);
		insights.setTotal_reach((long) total_reach_agg.getValue());

		Sum unique_reach_agg = res.getAggregations().get(Constants.UNIQUE_REACH);
		insights.setUnique_reach((long) unique_reach_agg.getValue());

		Sum link_clicks_agg = res.getAggregations().get(Constants.LINK_CLICKS);
		insights.setLink_clicks((long) link_clicks_agg.getValue());

		Sum reaction_angry_agg = res.getAggregations().get(Constants.REACTION_ANGRY);
		insights.setReaction_angry((int) reaction_angry_agg.getValue());

		Sum reaction_haha_agg = res.getAggregations().get(Constants.REACTION_HAHA);
		insights.setReaction_haha((int) reaction_haha_agg.getValue());

		Sum reaction_love_agg = res.getAggregations().get(Constants.REACTION_LOVE);
		insights.setReaction_love((int) reaction_love_agg.getValue());

		Sum reaction_sad_agg = res.getAggregations().get(Constants.REACTION_SAD);
		insights.setReaction_sad((int) reaction_sad_agg.getValue());

		Sum reaction_thankful_agg = res.getAggregations().get(Constants.REACTION_THANKFUL);
		insights.setReaction_thankful((int) reaction_thankful_agg.getValue());

		Sum reaction_wow_agg = res.getAggregations().get(Constants.REACTION_WOW);
		insights.setReaction_wow((int) reaction_wow_agg.getValue());

		Sum hide_all_clicks_agg = res.getAggregations().get(Constants.HIDE_ALL_CLICKS);
		insights.setHide_all_clicks((int) hide_all_clicks_agg.getValue());

		Sum hide_clicks_agg = res.getAggregations().get(Constants.HIDE_CLICKS);
		insights.setHide_clicks((int) hide_clicks_agg.getValue());

		Sum report_spam_clicks_agg = res.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
		insights.setReport_spam_clicks((int) report_spam_clicks_agg.getValue());

		Sum likes_agg = res.getAggregations().get(Constants.LIKES);
		insights.setLikes((int) likes_agg.getValue());

		Sum comments_agg = res.getAggregations().get(Constants.COMMENTS);
		insights.setComments((int) comments_agg.getValue());

		Sum shares_agg = res.getAggregations().get(Constants.SHARES);
		insights.setShares((int) shares_agg.getValue());

		Sum ia_clicks_agg = res.getAggregations().get(Constants.IA_CLICKS);
		insights.setIa_clicks((int) ia_clicks_agg.getValue());

		Cardinality story_count = res.getAggregations().get("story_count");
		insights.setStory_count((int)story_count.getValue());

		Cardinality ia_story_count = res.getAggregations().get("ia_story_count");
		insights.setIa_story_count((int)ia_story_count.getValue());

		Sum video_views_agg = res.getAggregations().get(Constants.VIDEO_VIEWS);
		insights.setVideo_views((int) video_views_agg.getValue());

		Sum unique_video_views_agg = res.getAggregations().get(Constants.UNIQUE_VIDEO_VIEWS);
		insights.setUnique_video_views((int) unique_video_views_agg.getValue());

		if (unique_reach_agg.getValue() > 0) {
			insights.setShareability((shares_agg.getValue() / unique_reach_agg.getValue()) * 100);
		}

		if ((int) total_reach_agg.getValue() == 0
				|| (int) link_clicks_agg.getValue() == 0)
			insights.setCtr(0.0);
		else
			insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);

		Filter link_filter = res.getAggregations().get("links");

		Sum link_total_reach = link_filter.getAggregations().get(Constants.TOTAL_REACH);
		insights.setLinks_total_reach((long) link_total_reach.getValue());

		Sum link_unique_reach = link_filter.getAggregations().get(Constants.UNIQUE_REACH);
		insights.setLinks_unique_reach((long) link_unique_reach.getValue());

		Sum link_clicked = link_filter.getAggregations().get(Constants.LINK_CLICKS);
		insights.setLinks_clicked((long) link_clicked.getValue());

		Sum link_reaction_angry = link_filter.getAggregations().get(Constants.REACTION_ANGRY);
		insights.setLinks_reaction_angry((int) link_reaction_angry.getValue());

		Sum link_reaction_haha = link_filter.getAggregations().get(Constants.REACTION_HAHA);
		insights.setLinks_reaction_haha((int) link_reaction_haha.getValue());

		Sum link_reaction_love = link_filter.getAggregations().get(Constants.REACTION_LOVE);
		insights.setLinks_reaction_love((int) link_reaction_love.getValue());

		Sum link_reaction_sad = link_filter.getAggregations().get(Constants.REACTION_SAD);
		insights.setLinks_reaction_sad((int) link_reaction_sad.getValue());

		Sum link_reaction_thankful = link_filter.getAggregations().get(Constants.REACTION_THANKFUL);
		insights.setLinks_reaction_thankful((int) link_reaction_thankful.getValue());

		Sum link_reaction_wow = link_filter.getAggregations().get(Constants.REACTION_WOW);
		insights.setLinks_reaction_wow((int) link_reaction_wow.getValue());

		Sum link_hide_all_clicks = link_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
		insights.setLinks_hide_all_clicks((int) link_hide_all_clicks.getValue());

		Sum link_hide_clicks = link_filter.getAggregations().get(Constants.HIDE_CLICKS);
		insights.setLinks_hide_clicks((int) link_hide_clicks.getValue());

		Sum link_report_spam_clicks = link_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
		insights.setLinks_report_spam_clicks((int) link_report_spam_clicks.getValue());

		Sum link_likes = link_filter.getAggregations().get(Constants.LIKES);
		insights.setLinks_likes((int) link_likes.getValue());

		Sum link_comments = link_filter.getAggregations().get(Constants.COMMENTS);
		insights.setLinks_comments((int) link_comments.getValue());

		Sum link_shares = link_filter.getAggregations().get(Constants.SHARES);
		insights.setLinks_shares((int) link_shares.getValue());

		if (link_unique_reach.getValue() > 0) {
			insights.setLinks_shareability((link_shares.getValue() / link_unique_reach.getValue()) * 100);
		}

		Cardinality link_count = link_filter.getAggregations().get("story_count");
		insights.setLinks_count((int)link_count.getValue());

		if ((int) link_total_reach.getValue() == 0
				|| (int) link_clicked.getValue() == 0)
			insights.setLinks_ctr(0.0);
		else
			insights.setLinks_ctr((link_clicked.getValue() / link_total_reach.getValue()) * 100.0);

		Filter photo_filter = res.getAggregations().get("photos");

		Sum photo_total_reach = photo_filter.getAggregations().get(Constants.TOTAL_REACH);
		insights.setPhotos_total_reach((long) photo_total_reach.getValue());

		Sum photo_unique_reach = photo_filter.getAggregations().get(Constants.UNIQUE_REACH);
		insights.setPhotos_unique_reach((long) photo_unique_reach.getValue());

		Sum photo_clicked = photo_filter.getAggregations().get(Constants.LINK_CLICKS);
		insights.setPhotos_clicked((long) photo_clicked.getValue());

		Sum photo_reaction_angry = photo_filter.getAggregations().get(Constants.REACTION_ANGRY);
		insights.setPhotos_reaction_angry((int) photo_reaction_angry.getValue());

		Sum photo_reaction_haha = photo_filter.getAggregations().get(Constants.REACTION_HAHA);
		insights.setPhotos_reaction_haha((int) photo_reaction_haha.getValue());

		Sum photo_reaction_love = photo_filter.getAggregations().get(Constants.REACTION_LOVE);
		insights.setPhotos_reaction_love((int) photo_reaction_love.getValue());

		Sum photo_reaction_sad = photo_filter.getAggregations().get(Constants.REACTION_SAD);
		insights.setPhotos_reaction_sad((int) photo_reaction_sad.getValue());

		Sum photo_reaction_thankful = photo_filter.getAggregations().get(Constants.REACTION_THANKFUL);
		insights.setPhotos_reaction_thankful((int) photo_reaction_thankful.getValue());

		Sum photo_reaction_wow = photo_filter.getAggregations().get(Constants.REACTION_WOW);
		insights.setPhotos_reaction_wow((int) photo_reaction_wow.getValue());

		Sum photo_hide_all_clicks = photo_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
		insights.setPhotos_hide_all_clicks((int) photo_hide_all_clicks.getValue());

		Sum photo_hide_clicks = photo_filter.getAggregations().get(Constants.HIDE_CLICKS);
		insights.setPhotos_hide_clicks((int) photo_hide_clicks.getValue());

		Sum photo_report_spam_clicks = photo_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
		insights.setPhotos_report_spam_clicks((int) photo_report_spam_clicks.getValue());

		Sum photo_likes = photo_filter.getAggregations().get(Constants.LIKES);
		insights.setPhotos_likes((int) photo_likes.getValue());

		Sum photo_comments = photo_filter.getAggregations().get(Constants.COMMENTS);
		insights.setPhotos_comments((int) photo_comments.getValue());

		Sum photo_shares = photo_filter.getAggregations().get(Constants.SHARES);
		insights.setPhotos_shares((int) photo_shares.getValue());

		if (photo_unique_reach.getValue() > 0) {
			insights.setPhotos_shareability((photo_shares.getValue() / photo_unique_reach.getValue()) * 100);
		}

		Cardinality photo_count = photo_filter.getAggregations().get("story_count");
		insights.setPhotos_count((int)photo_count.getValue());

		if ((int) photo_total_reach.getValue() == 0
				|| (int) photo_clicked.getValue() == 0)
			insights.setPhotos_ctr(0.0);
		else
			insights.setPhotos_ctr((photo_clicked.getValue() / photo_total_reach.getValue()) * 100.0);

		Filter video_filter = res.getAggregations().get("videos");

		Sum video_total_reach = video_filter.getAggregations().get(Constants.TOTAL_REACH);
		insights.setVideos_total_reach((int) video_total_reach.getValue());

		Sum video_unique_reach = video_filter.getAggregations().get(Constants.UNIQUE_REACH);
		insights.setVideos_unique_reach((int) video_unique_reach.getValue());

		Sum video_clicked = video_filter.getAggregations().get(Constants.LINK_CLICKS);
		insights.setVideos_clicked((int) video_clicked.getValue());

		Sum video_reaction_angry = video_filter.getAggregations().get(Constants.REACTION_ANGRY);
		insights.setVideos_reaction_angry((int) video_reaction_angry.getValue());

		Sum video_reaction_haha = video_filter.getAggregations().get(Constants.REACTION_HAHA);
		insights.setVideos_reaction_haha((int) video_reaction_haha.getValue());

		Sum video_reaction_love = video_filter.getAggregations().get(Constants.REACTION_LOVE);
		insights.setVideos_reaction_love((int) video_reaction_love.getValue());

		Sum video_reaction_sad = video_filter.getAggregations().get(Constants.REACTION_SAD);
		insights.setVideos_reaction_sad((int) video_reaction_sad.getValue());

		Sum video_reaction_thankful = video_filter.getAggregations().get(Constants.REACTION_THANKFUL);
		insights.setVideos_reaction_thankful((int) video_reaction_thankful.getValue());

		Sum video_reaction_wow = video_filter.getAggregations().get(Constants.REACTION_WOW);
		insights.setVideos_reaction_wow((int) video_reaction_wow.getValue());

		Sum video_hide_all_clicks = video_filter.getAggregations().get(Constants.HIDE_ALL_CLICKS);
		insights.setVideos_hide_all_clicks((int) video_hide_all_clicks.getValue());

		Sum video_hide_clicks = video_filter.getAggregations().get(Constants.HIDE_CLICKS);
		insights.setVideos_hide_clicks((int) video_hide_clicks.getValue());

		Sum video_report_spam_clicks = video_filter.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
		insights.setVideos_report_spam_clicks((int) video_report_spam_clicks.getValue());

		Sum video_likes = video_filter.getAggregations().get(Constants.LIKES);
		insights.setVideos_likes((int) video_likes.getValue());

		Sum video_comments = video_filter.getAggregations().get(Constants.COMMENTS);
		insights.setVideos_comments((int) video_comments.getValue());

		Sum video_shares = video_filter.getAggregations().get(Constants.SHARES);
		insights.setVideos_shares((int) video_shares.getValue());

		if (video_unique_reach.getValue() > 0) {
			insights.setVideos_shareability((video_shares.getValue() / video_unique_reach.getValue()) * 100);
		}

		Cardinality video_count = video_filter.getAggregations().get("story_count");
		insights.setVideos_count((int)video_count.getValue());

		if ((int) video_total_reach.getValue() == 0
				|| (int) video_clicked.getValue() == 0)
			insights.setVideos_ctr(0.0);
		else
			insights.setVideos_ctr((video_clicked.getValue() / video_total_reach.getValue()) * 100.0);

		qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.DAY))
		.must(QueryBuilders.rangeQuery(Constants.POPULARITY_SCORE).gte(0));

		SearchResponse popRes = client.prepareSearch(Indexes.FB_INSIGHTS_POPULAR)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
				.addAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
				.setSize(0).execute().actionGet();

		Cardinality popStories = popRes.getAggregations().get("storyCount");
		insights.setPopular_count((int) popStories.getValue());
		return insights;
	}

	public Map<String, FacebookInsights> getFacebookInsightsByIntervalAndCategory(WisdomQuery query) {
		Map<String, FacebookInsights> records = new LinkedHashMap<>();
		long startTime = System.currentTimeMillis();

		try {
			String interval = query.getInterval();
			String parameter = query.getParameter();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);

			TermsAggregationBuilder catAgg = null; // new TermsBuilder("category");

			if(query.getCategory_type().equalsIgnoreCase(Constants.SUPER_CAT_NAME))
			{
				catAgg = AggregationBuilders.terms("category").field(Constants.SUPER_CAT_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(parameter, false));
			}
			else{
			if (query.getChannel_slno().contains(1463) || query.getChannel_slno().contains(9069)
					|| query.getChannel_slno().contains(9254)) {
				catAgg = AggregationBuilders.terms("category").field(Constants.WisdomNextConstant.PP_CAT_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(parameter, false));

			} 
			else {
				catAgg = AggregationBuilders.terms("category").field(Constants.WisdomNextConstant.GA_CAT_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(parameter, false));
			}
			}

			Histogram.Order order = null;

			if (query.isOrderAsc())
				order = Histogram.Order.KEY_ASC;
			else
				order = Histogram.Order.KEY_DESC;
			System.out.println("query :"+getSocialDecodeQuery(query));
			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
							.format("yyyy-MM-dd'T'HH:mm:ss'Z'").dateHistogramInterval(histInterval).order(order)
							.subAggregation(
									AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD))
							.subAggregation(
									AggregationBuilders.cardinality("ia_story_count").field(Constants.IA_STORYID))
							.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
							.subAggregation(
									AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
									.field(Constants.REACTION_THANKFUL))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
							.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
							.subAggregation(
									AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
									.field(Constants.REPORT_SPAM_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
							.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
							.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
							.subAggregation(AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))
							.subAggregation(catAgg
									.subAggregation(AggregationBuilders.cardinality("story_count").field(
											Constants.STORY_ID_FIELD))
									.subAggregation(AggregationBuilders.cardinality("ia_story_count")
											.field(Constants.IA_STORYID))
									.subAggregation(
											AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
									.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH)
											.field(Constants.UNIQUE_REACH))
									.subAggregation(
											AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
											.field(Constants.REACTION_THANKFUL))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_SAD)
											.field(Constants.REACTION_SAD))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_ANGRY)
											.field(Constants.REACTION_ANGRY))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_WOW)
											.field(Constants.REACTION_WOW))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_HAHA)
											.field(Constants.REACTION_HAHA))
									.subAggregation(AggregationBuilders.sum(Constants.REACTION_LOVE)
											.field(Constants.REACTION_LOVE))
									.subAggregation(
											AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS)
											.field(Constants.HIDE_ALL_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
											.field(Constants.REPORT_SPAM_CLICKS))
									.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
									.subAggregation(
											AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
									.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
									.subAggregation(
											AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))))
					.setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					FacebookInsights insights = new FacebookInsights();
					List<Object> categoryInsights = new ArrayList<>();

					Cardinality story_count = bucket.getAggregations().get("story_count");
					insights.setStory_count((int)story_count.getValue());

					Cardinality ia_story_count = bucket.getAggregations().get("ia_story_count");
					insights.setIa_story_count((int)ia_story_count.getValue());

					Sum total_reach_agg = bucket.getAggregations().get(Constants.TOTAL_REACH);
					insights.setTotal_reach((long) total_reach_agg.getValue());

					Sum unique_reach_agg = bucket.getAggregations().get(Constants.UNIQUE_REACH);
					insights.setUnique_reach((long) unique_reach_agg.getValue());

					Sum link_clicks_agg = bucket.getAggregations().get(Constants.LINK_CLICKS);
					insights.setLink_clicks((long) link_clicks_agg.getValue());

					Sum reaction_angry_agg = bucket.getAggregations().get(Constants.REACTION_ANGRY);
					insights.setReaction_angry((int) reaction_angry_agg.getValue());

					Sum reaction_haha_agg = bucket.getAggregations().get(Constants.REACTION_HAHA);
					insights.setReaction_haha((int) reaction_haha_agg.getValue());

					Sum reaction_love_agg = bucket.getAggregations().get(Constants.REACTION_LOVE);
					insights.setReaction_love((int) reaction_love_agg.getValue());

					Sum reaction_sad_agg = bucket.getAggregations().get(Constants.REACTION_SAD);
					insights.setReaction_sad((int) reaction_sad_agg.getValue());

					Sum reaction_thankful_agg = bucket.getAggregations().get(Constants.REACTION_THANKFUL);
					insights.setReaction_thankful((int) reaction_thankful_agg.getValue());

					Sum reaction_wow_agg = bucket.getAggregations().get(Constants.REACTION_WOW);
					insights.setReaction_wow((int) reaction_wow_agg.getValue());

					Sum hide_all_clicks_agg = bucket.getAggregations().get(Constants.HIDE_ALL_CLICKS);
					insights.setHide_all_clicks((int) hide_all_clicks_agg.getValue());

					Sum hide_clicks_agg = bucket.getAggregations().get(Constants.HIDE_CLICKS);
					insights.setHide_clicks((int) hide_clicks_agg.getValue());

					Sum report_spam_clicks_agg = bucket.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
					insights.setReport_spam_clicks((int) report_spam_clicks_agg.getValue());

					Sum likes_agg = bucket.getAggregations().get(Constants.LIKES);
					insights.setLikes((int) likes_agg.getValue());

					Sum comments_agg = bucket.getAggregations().get(Constants.COMMENTS);
					insights.setComments((int) comments_agg.getValue());

					Sum shares_agg = bucket.getAggregations().get(Constants.SHARES);
					insights.setShares((int) shares_agg.getValue());

					Sum ia_clicks_agg = bucket.getAggregations().get(Constants.IA_CLICKS);
					insights.setIa_clicks((int) ia_clicks_agg.getValue());
					Map<String, Object> catOutMap = new LinkedHashMap<>();							

					if(insights.getUnique_reach()>0){
						insights.setShareability(((double) insights.getShares())/insights.getUnique_reach());
					}

					if(insights.getLink_clicks()>0&&insights.getTotal_reach()>0)
					{
						insights.setCtr(((double) insights.getLink_clicks())/insights.getTotal_reach());
					}


					Terms categories = bucket.getAggregations().get("category");
					for (Terms.Bucket catBucket : categories.getBuckets()) {
						//if (!StringUtils.isBlank(catBucket.getKeyAsString())) {
						Map<String, Object> catMap = new HashMap<>();
						catMap.put(Constants.WisdomNextConstant.GA_CAT_NAME, catBucket.getKey());

						Sum total_reach = catBucket.getAggregations().get(Constants.TOTAL_REACH);
						Sum unique_reach = catBucket.getAggregations().get(Constants.UNIQUE_REACH);
						Sum link_clicks = catBucket.getAggregations().get(Constants.LINK_CLICKS);
						Sum reaction_angry = catBucket.getAggregations().get(Constants.REACTION_ANGRY);
						Sum reaction_haha = catBucket.getAggregations().get(Constants.REACTION_HAHA);
						Sum reaction_love = catBucket.getAggregations().get(Constants.REACTION_LOVE);
						Sum reaction_sad = catBucket.getAggregations().get(Constants.REACTION_SAD);
						Sum reaction_thankful = catBucket.getAggregations().get(Constants.REACTION_THANKFUL);
						Sum reaction_wow = catBucket.getAggregations().get(Constants.REACTION_WOW);
						Sum hide_all_clicks = catBucket.getAggregations().get(Constants.HIDE_ALL_CLICKS);
						Sum hide_clicks = catBucket.getAggregations().get(Constants.HIDE_CLICKS);
						Sum report_spam_clicks = catBucket.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
						Sum likes = catBucket.getAggregations().get(Constants.LIKES);
						Sum comments = catBucket.getAggregations().get(Constants.COMMENTS);
						Sum shares = catBucket.getAggregations().get(Constants.SHARES);
						Sum ia_clicks = catBucket.getAggregations().get(Constants.IA_CLICKS);
						Cardinality cat_story_count = catBucket.getAggregations().get("story_count");
						Cardinality cat_ia_story_count = catBucket.getAggregations().get("ia_story_count");
						catMap.put(Constants.STORY_COUNT, cat_story_count.getValue());
						catMap.put(Constants.IA_STORY_COUNT, cat_ia_story_count.getValue());
						catMap.put(Constants.TOTAL_REACH, total_reach.getValue());
						catMap.put(Constants.UNIQUE_REACH, unique_reach.getValue());
						catMap.put(Constants.LINK_CLICKS, link_clicks.getValue());
						catMap.put(Constants.REACTION_ANGRY, reaction_angry.getValue());
						catMap.put(Constants.REACTION_HAHA, reaction_haha.getValue());
						catMap.put(Constants.REACTION_LOVE, reaction_love.getValue());
						catMap.put(Constants.REACTION_SAD, reaction_sad.getValue());
						catMap.put(Constants.REACTION_THANKFUL, reaction_thankful.getValue());
						catMap.put(Constants.REACTION_WOW, reaction_wow.getValue());
						catMap.put(Constants.HIDE_ALL_CLICKS, hide_all_clicks.getValue());
						catMap.put(Constants.HIDE_CLICKS, hide_clicks.getValue());
						catMap.put(Constants.REPORT_SPAM_CLICKS, report_spam_clicks.getValue());
						catMap.put(Constants.LIKES, likes.getValue());
						catMap.put(Constants.COMMENTS, comments.getValue());
						catMap.put(Constants.SHARES, shares.getValue());
						catMap.put(Constants.IA_CLICKS, ia_clicks.getValue());
						if(unique_reach.getValue()>0)
						{
							catMap.put(Constants.SHAREABILITY, ((double) shares.getValue())/unique_reach.getValue());
						}
						if(link_clicks.getValue()>0&&total_reach.getValue()>0)
						{
							catMap.put(Constants.CTR, ((double) link_clicks.getValue())/total_reach.getValue());
						}
						else
						{
							catMap.put(Constants.CTR,0);
						}
						catOutMap.put((String) catBucket.getKey(), catMap);

						//categoryInsights.add(catOutMap);
						//}
					}

					/*if (categoryInsights.size() > 0) {
						insights.setCategoryInsights(categoryInsights);
						if (query.getInterval().equals("1d")) {
							insights.setWeekDay(DateUtil.getWeekDay(bucket.getKeyAsString()));
						}
						records.put(bucket.getKeyAsString(), insights);
					}*/

					if (catOutMap.size() > 0) {
						insights.setCategoryInsights(catOutMap);
						if (query.getInterval().equals("1d")) {
							insights.setWeekDay(DateUtil.getWeekDay(bucket.getKeyAsString()));
						}	
						records.put(bucket.getKeyAsString(), insights);
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebookInsightsByIntervalAndCategory.", e);
		}
		log.info("Retrieving facebookInsightsByIntervalAndCategory; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<FacebookInsights> getFacebookInsightsByCategory(WisdomQuery query) {
		List<FacebookInsights> records = new ArrayList<>();
		long startTime = System.currentTimeMillis();

		try {
			TermsAggregationBuilder catAgg =null;// new TermsBuilder("category");
			if (query.getChannel_slno().contains(1463) || query.getChannel_slno().contains(9069)
					|| query.getChannel_slno().contains(9254)) {
				catAgg = AggregationBuilders.terms("category").field(Constants.WisdomNextConstant.PP_CAT_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(Constants.TOTAL_REACH, false));

			}

			else {
				catAgg = AggregationBuilders.terms("category").field(Constants.WisdomNextConstant.GA_CAT_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(Constants.TOTAL_REACH, false));

			}

			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(catAgg
							.subAggregation(
									AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD))
							.subAggregation(
									AggregationBuilders.cardinality("ia_story_count").field(Constants.IA_STORYID))
							.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
							.subAggregation(
									AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.REACTION_THANKFUL)
									.field(Constants.REACTION_THANKFUL))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_SAD).field(Constants.REACTION_SAD))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_ANGRY).field(Constants.REACTION_ANGRY))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_WOW).field(Constants.REACTION_WOW))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_HAHA).field(Constants.REACTION_HAHA))
							.subAggregation(
									AggregationBuilders.sum(Constants.REACTION_LOVE).field(Constants.REACTION_LOVE))
							.subAggregation(AggregationBuilders.sum(Constants.HIDE_CLICKS).field(Constants.HIDE_CLICKS))
							.subAggregation(
									AggregationBuilders.sum(Constants.HIDE_ALL_CLICKS).field(Constants.HIDE_ALL_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.REPORT_SPAM_CLICKS)
									.field(Constants.REPORT_SPAM_CLICKS))
							.subAggregation(AggregationBuilders.sum(Constants.LIKES).field(Constants.LIKES))
							.subAggregation(AggregationBuilders.sum(Constants.COMMENTS).field(Constants.COMMENTS))
							.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
							.subAggregation(AggregationBuilders.sum(Constants.IA_CLICKS).field(Constants.IA_CLICKS))
							.order(Order.aggregation("total_reach", query.isOrderAsc())))
					.setSize(0).execute().actionGet();

			Terms categories = res.getAggregations().get("category");
			for (Terms.Bucket catBucket : categories.getBuckets()) {
				//if (!StringUtils.isBlank(catBucket.getKeyAsString())) {
				FacebookInsights insights = new FacebookInsights();
				Cardinality cat_story_count = catBucket.getAggregations().get("story_count");
				Cardinality cat_ia_story_count = catBucket.getAggregations().get("ia_story_count");
				insights.setGa_cat_name(catBucket.getKeyAsString());
				insights.setStory_count((int) cat_story_count.getValue());
				insights.setIa_story_count((int) cat_ia_story_count.getValue());

				Sum total_reach_agg = catBucket.getAggregations().get(Constants.TOTAL_REACH);
				insights.setTotal_reach((long) total_reach_agg.getValue());

				Sum unique_reach_agg = catBucket.getAggregations().get(Constants.UNIQUE_REACH);
				insights.setUnique_reach((long) unique_reach_agg.getValue());

				Sum link_clicks_agg = catBucket.getAggregations().get(Constants.LINK_CLICKS);
				insights.setLink_clicks((long) link_clicks_agg.getValue());

				Sum reaction_angry_agg = catBucket.getAggregations().get(Constants.REACTION_ANGRY);
				insights.setReaction_angry((int) reaction_angry_agg.getValue());

				Sum reaction_haha_agg = catBucket.getAggregations().get(Constants.REACTION_HAHA);
				insights.setReaction_haha((int) reaction_haha_agg.getValue());

				Sum reaction_love_agg = catBucket.getAggregations().get(Constants.REACTION_LOVE);
				insights.setReaction_love((int) reaction_love_agg.getValue());

				Sum reaction_sad_agg = catBucket.getAggregations().get(Constants.REACTION_SAD);
				insights.setReaction_sad((int) reaction_sad_agg.getValue());

				Sum reaction_thankful_agg = catBucket.getAggregations().get(Constants.REACTION_THANKFUL);
				insights.setReaction_thankful((int) reaction_thankful_agg.getValue());

				Sum reaction_wow_agg = catBucket.getAggregations().get(Constants.REACTION_WOW);
				insights.setReaction_wow((int) reaction_wow_agg.getValue());

				Sum hide_all_clicks_agg = catBucket.getAggregations().get(Constants.HIDE_ALL_CLICKS);
				insights.setHide_all_clicks((int) hide_all_clicks_agg.getValue());

				Sum hide_clicks_agg = catBucket.getAggregations().get(Constants.HIDE_CLICKS);
				insights.setHide_clicks((int) hide_clicks_agg.getValue());

				Sum report_spam_clicks_agg = catBucket.getAggregations().get(Constants.REPORT_SPAM_CLICKS);
				insights.setReport_spam_clicks((int) report_spam_clicks_agg.getValue());

				Sum likes_agg = catBucket.getAggregations().get(Constants.LIKES);
				insights.setLikes((int) likes_agg.getValue());

				Sum comments_agg = catBucket.getAggregations().get(Constants.COMMENTS);
				insights.setComments((int) comments_agg.getValue());

				Sum shares_agg = catBucket.getAggregations().get(Constants.SHARES);
				insights.setShares((int) shares_agg.getValue());

				Sum ia_clicks_agg = catBucket.getAggregations().get(Constants.IA_CLICKS);
				insights.setIa_clicks((int) ia_clicks_agg.getValue());

				records.add(insights);
				//}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebookInsightsByCategory.", e);
		}
		log.info("Retrieving facebookInsightsByCategory; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public LinkedHashMap<String, Object> getFacebookInsightsByCategoryAndDay(WisdomQuery query) {
		LinkedHashMap<String, Object> records = new LinkedHashMap<>();
		long startTime = System.currentTimeMillis();

		try {
			String parameter = query.getParameter();
			String weekDay = query.getWeekDay();
			String dateField = query.getDateField();

			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(getSocialDecodeQuery(query).mustNot(QueryBuilders.termQuery(Constants.CAT_NAME, "")))
					.addAggregation(AggregationBuilders.terms("days")
							.script(new Script("Date date = new Date(doc['" + dateField	+ "'].value-19800000) ;new SimpleDateFormat('EEE').format(date)"))
							.subAggregation(AggregationBuilders.terms("categories").field(Constants.CAT_NAME)
									.size(query.getCategorySize()).order(Order.aggregation(parameter, false))
									.subAggregation(AggregationBuilders.topHits("top").size(1)
											.fetchSource(new String[] { Constants.CAT_ID_FIELD }, new String[] {}))
									.subAggregation(AggregationBuilders.sum(parameter).field(parameter))
									.subAggregation(AggregationBuilders
											.terms("hours").script(new Script("Date date = new Date(doc['" + dateField
													+ "'].value-19800000) ;new SimpleDateFormat('HH').format(date)"))
											.order(Order.aggregation(parameter, false)).size(query.getCategorySize())
											.subAggregation(AggregationBuilders.sum(parameter).field(parameter))))
							.subAggregation(AggregationBuilders.sum(parameter).field(parameter)))
					.execute().actionGet();

			Terms days = res.getAggregations().get("days");
			for (Terms.Bucket day : days.getBuckets()) {
				if (day.getKey().equals(weekDay)) {
					Sum total_reach_day = day.getAggregations().get(parameter);
					records.put(parameter, ((Double) total_reach_day.getValue()).longValue());
					Terms categories = day.getAggregations().get("categories");
					for (Terms.Bucket category : categories.getBuckets()) {
						LinkedHashMap<String, Object> catMap = new LinkedHashMap<>();
						TopHits hit = category.getAggregations().get("top");
						catMap.put(Constants.CAT_ID_FIELD,
								hit.getHits().getHits()[0].getSource().get(Constants.CAT_ID_FIELD).toString());
						Sum total_reach_cat = category.getAggregations().get(parameter);
						catMap.put(parameter, ((Double) total_reach_cat.getValue()).longValue());
						Terms hours = category.getAggregations().get("hours");
						for (Terms.Bucket hour : hours.getBuckets()) {
							Sum total_reach_hour = hour.getAggregations().get(parameter);
							catMap.put("_" + hour.getKey(), ((Double) total_reach_hour.getValue()).longValue());
						}
						records.put(category.getKeyAsString(), catMap);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFacebookInsightsByCategoryAndDay.", e);
		}
		log.info("Retrieving getFacebookInsightsByCategoryAndDay; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public TreeMap<String, ArrayList<Object>> getFacebookInsightsForAutomation(WisdomQuery query) {
		TreeMap<String, ArrayList<Object>> records = new TreeMap<>();
		long startTime = System.currentTimeMillis();

		try {
			String parameter = query.getParameter();
			String weekDay = query.getWeekDay();
			String dateField = query.getDateField();

			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(getSocialDecodeQuery(query).mustNot(QueryBuilders.termQuery(Constants.CAT_NAME, "")))
					.addAggregation(AggregationBuilders.terms("days")
							.script(new Script("Date date = new Date(doc['" + dateField + "'].value-19800000) ;new SimpleDateFormat('EEE').format(date)"))
							.subAggregation(AggregationBuilders.terms("categories").field(Constants.CAT_NAME)
									.size(query.getCategorySize()).order(Order.aggregation(parameter, false))
									.subAggregation(AggregationBuilders.topHits("top").size(1)
											.fetchSource(new String[] { Constants.CAT_ID_FIELD }, new String[] {}))
									.subAggregation(AggregationBuilders.sum(parameter).field(parameter))
									.subAggregation(AggregationBuilders
											.terms("hours").script(new Script("Date date = new Date(doc['" + dateField
													+ "'].value-19800000) ;new SimpleDateFormat('HH').format(date)"))
											.order(Order.aggregation(parameter, false)).size(query.getCategorySize())
											.subAggregation(AggregationBuilders.sum(parameter).field(parameter))))
							.subAggregation(AggregationBuilders.sum(parameter).field(parameter)))
					.execute().actionGet();

			Terms days = res.getAggregations().get("days");
			for (Terms.Bucket day : days.getBuckets()) {
				if (day.getKey().equals(weekDay)) {
					Terms categories = day.getAggregations().get("categories");
					for (Terms.Bucket category : categories.getBuckets()) {
						HashMap<String, Object> catMap = new HashMap<>();
						TopHits hit = category.getAggregations().get("top");
						catMap.put(Constants.CAT_ID_FIELD,
								hit.getHits().getHits()[0].getSource().get(Constants.CAT_ID_FIELD).toString());
						catMap.put(Constants.CAT_NAME, category.getKey());
						Terms hours = category.getAggregations().get("hours");
						for (Terms.Bucket hour : hours.getBuckets()) {
							if (records.containsKey("_" + hour.getKey())
									&& records.get("_" + hour.getKey()).size() < 4) {
								records.get("_" + hour.getKey()).add(catMap);
							} else {
								ArrayList<Object> hourList = new ArrayList<>();
								hourList.add(catMap);
								records.put("_" + hour.getKey(), hourList);
							}

						}

					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFacebookInsightsForAutomation.", e);
		}
		log.info("Retrieving getFacebookInsightsForAutomation; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public HashMap<String, Object> getTrendingEntitiesForSocialDecode(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		HashMap<String, Object> records = new HashMap<>();
		try {
			String parameter = query.getParameter();
			// String indexName = "realtime_" + DateUtil.getCurrentDate();
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(query.getDateField()).from(query.getStartDate())
							.to(query.getEndDate()))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()))
					.must(QueryBuilders.termsQuery(Constants.CAT_NAME, query.getCat_name()));
			SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("categories").field(Constants.CAT_NAME)
							.size(query.getCat_name().size())
							.subAggregation(AggregationBuilders.terms(Constants.PEOPLE).field(Constants.PEOPLE)
									.order(Order.aggregation(parameter, false)).size(query.getCount()).subAggregation(
											AggregationBuilders.sum(parameter).field(parameter)))
							.subAggregation(
									AggregationBuilders.terms(Constants.ORGANIZATION).field(Constants.ORGANIZATION)
									.order(Order.aggregation(parameter, false)).size(query.getCount())
									.subAggregation(AggregationBuilders.sum(parameter).field(parameter)))
							.subAggregation(AggregationBuilders.terms(Constants.EVENT).field(Constants.EVENT)
									.order(Order.aggregation(parameter, false)).size(query.getCount()).subAggregation(
											AggregationBuilders.sum(parameter).field(parameter))))
					.setSize(0).execute().actionGet();
			Terms categoryBuckets = res.getAggregations().get("categories");
			for (Terms.Bucket category : categoryBuckets.getBuckets()) {
				HashMap<String, Object> catMap = new HashMap<>();
				LinkedHashMap<String, Object> peopleMap = new LinkedHashMap<>();
				LinkedHashMap<String, Object> organizationMap = new LinkedHashMap<>();
				LinkedHashMap<String, Object> eventMap = new LinkedHashMap<>();
				Terms peopleBuckets = category.getAggregations().get(Constants.PEOPLE);
				for (Terms.Bucket people : peopleBuckets.getBuckets()) {
					if (!StringUtils.isBlank(people.getKeyAsString())) {
						Sum sumAgg = people.getAggregations().get(parameter);
						peopleMap.put(people.getKeyAsString(), ((Double) sumAgg.getValue()).longValue());
					}
				}
				Terms organizationBuckets = category.getAggregations().get(Constants.ORGANIZATION);
				for (Terms.Bucket organization : organizationBuckets.getBuckets()) {
					if (!StringUtils.isBlank(organization.getKeyAsString())) {
						Sum sumAgg = organization.getAggregations().get(parameter);
						organizationMap.put(organization.getKeyAsString(), ((Double) sumAgg.getValue()).longValue());
					}
				}
				Terms eventBuckets = category.getAggregations().get(Constants.EVENT);
				for (Terms.Bucket event : eventBuckets.getBuckets()) {
					if (!StringUtils.isBlank(event.getKeyAsString())) {
						Sum sumAgg = event.getAggregations().get(parameter);
						eventMap.put(event.getKeyAsString(), ((Double) sumAgg.getValue()).longValue());
					}
				}
				catMap.put(Constants.PEOPLE, peopleMap);
				catMap.put(Constants.ORGANIZATION, organizationMap);
				catMap.put(Constants.EVENT, eventMap);
				records.put(category.getKeyAsString(), catMap);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving TrendingEntitiesForSocialDecode.", e);
		}
		log.info("Retrieving TrendingEntitiesForSocialDecode; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String, EODFlickerDetail> getEODFlickerData(WisdomQuery query) {
		Map<String, EODFlickerDetail> records = new LinkedHashMap<>();
		long startTime = System.currentTimeMillis();
		int size = 1000;
		try {
			String[] include1 = { Constants.POSITION };
			String[] include2 = { Constants.TITLE, Constants.URL, Constants.FLAG_V };

			BoolQueryBuilder qb1 = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate())
							.lte(query.getEndDate()))
					.must(QueryBuilders.termsQuery(Constants.CAT_ID_FIELD, query.getCat_id()))
					.must(QueryBuilders.termQuery(Constants.WisdomNextConstant.TYPE, query.getType()));
			if (query.getSuper_cat_id() != null) {
				qb1.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
			}
			SearchResponse res1;
			List<Integer> stories = new ArrayList<>();
			HashSet<Integer> uniqueStories = new HashSet<>();
			HashSet<Integer> latestStories = new HashSet<>();

			if (query.getMinutes().equalsIgnoreCase("all")) {
				res1 = client.prepareSearch(Indexes.REALTIME_EOD_FLICKER_ALL).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb1).setSize(size).addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute()
						.actionGet();

				for (SearchHit hit : res1.getHits().getHits()) {
					stories.addAll((ArrayList<Integer>) (hit.getSource().get("storyids")));
				}
				latestStories.addAll((ArrayList<Integer>) res1.getHits().getHits()[0].getSource().get("storyids"));
			} else {
				res1 = client.prepareSearch(Indexes.REALTIME_EOD_FLICKER_ALL).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb1).setSize(1).addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute()
						.actionGet();

				if (res1.getHits().getHits().length > 0) {
					stories = (ArrayList<Integer>) (res1.getHits().getHits()[0].getSource().get("storyids"));
					uniqueStories.addAll(stories);

					int minutes = Integer.parseInt(query.getMinutes());
					String indexName = "realtime_" + DateUtil.getCurrentDate();

					BoolQueryBuilder qb2 = QueryBuilders.boolQuery()
							.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD)
									.gte(DateUtil.addMinutesToCurrentTime((-1) * minutes))
									.lte(DateUtil.getCurrentDateTime()))
							.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, uniqueStories))
							.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));

					if (query.getExcludeTracker() != null) {
						BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
						for (String tracker : query.getExcludeTracker()) {
							trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
						}
						qb2.mustNot(trackersBoolQuery);
						// boolQuery.mustNot(QueryBuilders.termsQuery(Constants.TRACKER,
						// query.getExcludeTracker()));
					}

					SearchResponse res2 = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
							.setQuery(qb2)
							.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
									.size(100)
									.subAggregation(AggregationBuilders.cardinality("uvs").field(Constants.SESSION_ID_FIELD)))
							.setSize(0).execute().actionGet();

					Terms storybuckets = res2.getAggregations().get("stories");
					int recPos = 1;
					for (Terms.Bucket story : storybuckets.getBuckets()) {
						if (!StringUtils.isBlank(story.getKeyAsString())) {
							EODFlickerDetail detail = new EODFlickerDetail();
							Cardinality min_uvs_agg = story.getAggregations().get("uvs");
							detail.setMinutesuvs(min_uvs_agg.getValue());
							detail.setMinutespvs(story.getDocCount());
							detail.setRecPosition(recPos++);
							detail.setCat_id(query.getCat_id().get(0));
							detail.setPosition(stories.indexOf(Integer.parseInt(story.getKeyAsString()))+1);
							records.put(story.getKeyAsString(), detail);
						}
					}
				}
			}
			uniqueStories.addAll(stories);

			BoolQueryBuilder qb3 = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate())
							.lte(query.getEndDate()))
					.must(QueryBuilders.termQuery(Constants.WisdomNextConstant.WIDGET_ID, query.getType()))
					.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, uniqueStories));

			BoolQueryBuilder qb4 = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate())
							.lte(query.getEndDate()))
					.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList))
					.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, uniqueStories));
			if (query.getExcludeTracker() != null) {
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getExcludeTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
				}
				qb4.mustNot(trackersBoolQuery);
				// boolQuery.mustNot(QueryBuilders.termsQuery(Constants.TRACKER,
				// query.getExcludeTracker()));
			}
			if (query.getTracker() != null) {
				qb4.must(QueryBuilders.termsQuery(Constants.TRACKER,query.getTracker()));
			}

			SearchResponse res3 = client.prepareSearch(Indexes.REALTIME_EOD_FLICKER_DETAILS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb3)
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(size)
							.subAggregation(AggregationBuilders.max("endDate").field(Constants.DATE_TIME_FIELD))
							.subAggregation(AggregationBuilders.min("startDate").field(Constants.DATE_TIME_FIELD))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(include1, new String[] {})
									.size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.subAggregation(AggregationBuilders.terms("position").field(Constants.POSITION)
									.order(Order.aggregation("datetime", true)).subAggregation(
											AggregationBuilders.max("datetime").field(Constants.DATE_TIME_FIELD))))
					.setSize(0).execute().actionGet();

			Terms storybuckets1 = res3.getAggregations().get("stories");
			ArrayList<Integer> storyPositionList = new ArrayList<>();
			for (Terms.Bucket story : storybuckets1.getBuckets()) {
				String storyid = story.getKeyAsString();
				if (!StringUtils.isBlank(storyid)) {

					EODFlickerDetail detail = new EODFlickerDetail();
					if (latestStories.contains(Integer.parseInt(storyid))) {
						detail.setIsAvailable(true);
					}
					List<Integer> prevPos = new ArrayList<>();
					if (records.keySet().contains(storyid)) {
						detail = records.get(storyid);
					}
					Max endDate = story.getAggregations().get("endDate");
					Min startDate = story.getAggregations().get("startDate");
					TopHits topHits = story.getAggregations().get("top");
					Map<String, Object> source = topHits.getHits().getHits()[0].getSource();

					Terms position = story.getAggregations().get("position");
					for (Terms.Bucket pos : position.getBuckets()) {
						prevPos.add(Integer.parseInt(pos.getKeyAsString()));
					}
					detail.setPrevPositions(prevPos);
					detail.setEndDate(endDate.getValueAsString());
					detail.setStartDate(startDate.getValueAsString());
					if (query.getMinutes().equalsIgnoreCase("all")
							){
						//||!storyPositionList.contains(Integer.parseInt(source.get(Constants.POSITION).toString()))) {
						detail.setPosition(Integer.parseInt(source.get(Constants.POSITION).toString()));
						storyPositionList.add(Integer.parseInt(source.get(Constants.POSITION).toString()));
						records.put(storyid, detail);
					} else
						continue;
				}

			}
			String[] indexName = IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate());
			SearchResponse res4 = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb4)
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(size)
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
											AggregationBuilders.sum("uvs").field(Constants.UVS)))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(include2, new String[] {})
									.size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC)))
					.setSize(0).execute().actionGet();

			Terms storybuckets2 = res4.getAggregations().get("stories");
			for (Terms.Bucket story : storybuckets2.getBuckets()) {
				String storyid = story.getKeyAsString();

				if (!StringUtils.isBlank(storyid)) {
					EODFlickerDetail detail = new EODFlickerDetail();
					if (latestStories.contains(Integer.parseInt(storyid))) {
						detail.setIsAvailable(true);
					}
					long tpvs = 0;
					long mpvs = 0;
					long wpvs = 0;
					long tuvs = 0;
					long muvs = 0;
					long wuvs = 0;

					if (records.keySet().contains(storyid)) {
						detail = records.get(storyid);

						Sum totalPvs = story.getAggregations().get("tpvs");
						tpvs = Double.valueOf(totalPvs.getValue()).longValue();
						Sum totalUvs = story.getAggregations().get("tuvs");
						tuvs = Double.valueOf(totalUvs.getValue()).longValue();
						Terms hostType = story.getAggregations().get("host_type");

						for (Terms.Bucket host : hostType.getBuckets()) {
							Sum pvs = host.getAggregations().get("pvs");
							Sum uvs = host.getAggregations().get("uvs");
							if (host.getKey().equals(HostType.MOBILE)) {
								mpvs = (long) pvs.getValue();
								muvs = (long) uvs.getValue();
							} else if (host.getKey().equals(HostType.WEB)) {
								wpvs = (long) pvs.getValue();
								wuvs = (long) uvs.getValue();
							}
						}
						TopHits topHits = story.getAggregations().get("top");
						Map<String, Object> source = topHits.getHits().getHits()[0].getSource();

						detail.setTitle(source.get(Constants.TITLE).toString());
						detail.setUrl(source.get(Constants.URL).toString());
						detail.setFlag_v(source.get(Constants.FLAG_V).toString());
						detail.setStoryid(storyid);
						detail.setTotalpvs(tpvs);
						detail.setMpvs(mpvs);
						detail.setWpvs(wpvs);
						detail.setTotaluvs(tuvs);
						detail.setMuvs(muvs);
						detail.setWuvs(wuvs);
						detail.setCat_id(query.getCat_id().get(0));						
						records.put(storyid, detail);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving EODFlickerData.", e);
		}
		log.info("Retrieving EODFlickerData; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);

		Map<String, Object> tempMap = new LinkedHashMap<>(records);
		for(String key:tempMap.keySet()){
			records.put("_"+key, records.get(key));
			records.remove(key);
		}
		return records;
	}

	public Map<String, AdMetrics> getAdMetricsData(WisdomQuery query) {

		Map<String, AdMetrics> records = new LinkedHashMap<>();
		long startTime = System.currentTimeMillis();

		try {

			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);
			BoolQueryBuilder qb = QueryBuilders.boolQuery();

			BoolQueryBuilder dateQuery = QueryBuilders.boolQuery();
			dateQuery
			.should(QueryBuilders.rangeQuery(Constants.DATE).gte(query.getStartDate()).lte(query.getEndDate()));
			if (query.getCompareEndDate() != null && query.getCompareStartDate() != null) {
				dateQuery.should(QueryBuilders.rangeQuery(Constants.DATE).gte(query.getCompareStartDate())
						.lte(query.getCompareEndDate()));
			}

			qb.must(dateQuery);

			if (query.getInterval().contains("d")) {
				qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.DAY));
			}

			else if (query.getInterval().contains("M")) {
				qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.MONTH));
			}

			if (query.getCat_name() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.CATEGORY, query.getCat_name()));
			}

			if (query.getAd_unit_id() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.AD_UNIT_ID, query.getAd_unit_id()));
			}

			if (query.getDomain() != null) {
				qb.must(QueryBuilders.termQuery(Constants.DOMAIN, query.getDomain()));
			}

			if (query.getPlatform() != null) {
				qb.must(QueryBuilders.termQuery(Constants.PLATFORM, query.getPlatform()));
			}

			org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order order = org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_DESC;

			if (query.isOrderAsc()) {

				order = org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_ASC;
			}

			DateHistogramAggregationBuilder aggregation = AggregationBuilders.dateHistogram("interval").field(Constants.DATE)
					.format("yyyy-MM-dd").dateHistogramInterval(histInterval).order(order)
					.subAggregation(AggregationBuilders.sum(Constants.AD_EX_CLICKS).field(Constants.AD_EX_CLICKS))
					.subAggregation(AggregationBuilders.sum(Constants.AD_EX_REVENUE).field(Constants.AD_EX_REVENUE))
					.subAggregation(AggregationBuilders.terms(Constants.UV).field(Constants.UV).size(100))
					.subAggregation(AggregationBuilders.terms(Constants.PV).field(Constants.PV).size(100))
					.subAggregation(AggregationBuilders.terms(Constants.SESSION).field(Constants.SESSION).size(100))
					.subAggregation(AggregationBuilders.terms(Constants.DOMAIN_SESSION).field(Constants.DOMAIN_SESSION)
							.size(100))
					.subAggregation(
							AggregationBuilders.terms(Constants.SESS_DURATION).field(Constants.SESS_DURATION).size(100))
					.subAggregation(AggregationBuilders.terms(Constants.BOUNCES).field(Constants.BOUNCES).size(100))
					.subAggregation(AggregationBuilders.terms(Constants.PAGE_LOAD_TIME).field(Constants.PAGE_LOAD_TIME)
							.size(100))
					.subAggregation(AggregationBuilders.terms(Constants.PAGE_LOAD_SAMPLE)
							.field(Constants.PAGE_LOAD_SAMPLE).size(100));

			/*
			 * if(query.getDomain().equals(Constants.DB_APP)||query.getDomain().
			 * equals(Constants.DVB_APP)){ FilterAggregationBuilder filter_agg =
			 * AggregationBuilders.filter("ad_units").filter(FilterBuilders.
			 * boolFilter().mustNot(FilterBuilders.termsFilter(Constants.
			 * AD_UNIT_ID, Constants.INTERSTITIAL_IDS)))
			 * .subAggregation(AggregationBuilders.sum(Constants.
			 * TOTAL_IMPRESSIONS).field(Constants.TOTAL_IMPRESSIONS))
			 * .subAggregation(
			 * AggregationBuilders.sum(Constants.UNFILLED_IMPRESSIONS).field(
			 * Constants.UNFILLED_IMPRESSIONS)) .subAggregation(
			 * AggregationBuilders.sum(Constants.AD_EX_IMPRESSIONS).field(
			 * Constants.AD_EX_IMPRESSIONS));
			 * aggregation.subAggregation(filter_agg); }
			 * 
			 * else { aggregation
			 * .subAggregation(AggregationBuilders.sum(Constants.
			 * TOTAL_IMPRESSIONS).field(Constants.TOTAL_IMPRESSIONS))
			 * .subAggregation(AggregationBuilders.sum(Constants.
			 * UNFILLED_IMPRESSIONS).field(Constants.UNFILLED_IMPRESSIONS))
			 * .subAggregation(AggregationBuilders.sum(Constants.
			 * AD_EX_IMPRESSIONS).field(Constants.AD_EX_IMPRESSIONS)); }
			 */

			FilterAggregationBuilder filter_agg = AggregationBuilders.filter("ad_units",QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(Constants.IMP_STATUS, "0")))
					.subAggregation(
							AggregationBuilders.sum(Constants.TOTAL_IMPRESSIONS).field(Constants.TOTAL_IMPRESSIONS))
					.subAggregation(AggregationBuilders.sum(Constants.UNFILLED_IMPRESSIONS)
							.field(Constants.UNFILLED_IMPRESSIONS))
					.subAggregation(
							AggregationBuilders.sum(Constants.AD_EX_IMPRESSIONS).field(Constants.AD_EX_IMPRESSIONS));

			aggregation.subAggregation(filter_agg);
			SearchResponse res = client.prepareSearch(Indexes.AD_METRICS).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).addAggregation(aggregation).setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					long uv = 0;
					long pv = 0;
					long session = 0;
					long domain_session = 0;
					double pvPerUv = 0.0;
					double page_depth = 0.0;
					double avg_ses_duration = 0.0;
					double sess_duration = 0.0;
					double bounce_rate = 0.0;
					double bounces = 0.0;
					long total_impressions = 0;
					long unfilled_impressions = 0;
					double ad_ex_impressions = 0.0;
					double ad_ex_clicks = 0.0;
					double ad_ex_ctr = 0.0;
					double ad_ex_revenue = 0.0;
					double e_cpm = 0.0;
					double adx_cov = 0.0;
					double impPerPage = 0.0;
					double impPerSession = 0.0;
					double revPerUser = 0.0;
					double revPerSession = 0.0;
					double revPerPage = 0.0;
					long page_load_time = 0;
					long page_load_sample = 0;
					double avg_page_load_time = 0.0;

					AdMetrics adMetrics = new AdMetrics();

					/*
					 * if(query.getDomain().equals(Constants.DB_APP)||query.
					 * getDomain().equals(Constants.DVB_APP)){ Filter filter =
					 * bucket.getAggregations().get("ad_units"); Sum
					 * total_impressions_agg =
					 * filter.getAggregations().get(Constants.TOTAL_IMPRESSIONS);
					 * total_impressions =
					 * ((Double)total_impressions_agg.getValue();
					 * 
					 * Sum unfilled_impressions_agg =
					 * filter.getAggregations().get(Constants.UNFILLED_IMPRESSIONS);
					 * unfilled_impressions =
					 * ((Double)unfilled_impressions_agg.getValue();
					 * 
					 * Sum ad_ex_impressions_agg =
					 * filter.getAggregations().get(Constants.AD_EX_IMPRESSIONS);
					 * ad_ex_impressions = ad_ex_impressions_agg.getValue(); }
					 * 
					 * else{ Sum total_impressions_agg =
					 * bucket.getAggregations().get(Constants.TOTAL_IMPRESSIONS);
					 * total_impressions =
					 * ((Double)total_impressions_agg.getValue();
					 * 
					 * Sum unfilled_impressions_agg =
					 * bucket.getAggregations().get(Constants.UNFILLED_IMPRESSIONS);
					 * unfilled_impressions =
					 * ((Double)unfilled_impressions_agg.getValue();
					 * 
					 * Sum ad_ex_impressions_agg =
					 * bucket.getAggregations().get(Constants.AD_EX_IMPRESSIONS);
					 * ad_ex_impressions = ad_ex_impressions_agg.getValue(); }
					 */

					Filter filter = bucket.getAggregations().get("ad_units");
					Sum total_impressions_agg = filter.getAggregations().get(Constants.TOTAL_IMPRESSIONS);
					total_impressions = ((long) total_impressions_agg.getValue());

					Sum unfilled_impressions_agg = filter.getAggregations().get(Constants.UNFILLED_IMPRESSIONS);
					unfilled_impressions = ((long) unfilled_impressions_agg.getValue());

					Sum ad_ex_impressions_agg = filter.getAggregations().get(Constants.AD_EX_IMPRESSIONS);
					ad_ex_impressions = ad_ex_impressions_agg.getValue();

					Sum ad_ex_clicks_agg = bucket.getAggregations().get(Constants.AD_EX_CLICKS);
					ad_ex_clicks = ad_ex_clicks_agg.getValue();

					Sum ad_ex_revenue_agg = bucket.getAggregations().get(Constants.AD_EX_REVENUE);
					ad_ex_revenue = ad_ex_revenue_agg.getValue();

					if (ad_ex_impressions != 0) {
						ad_ex_ctr = (ad_ex_clicks / ad_ex_impressions) * 100;
					}

					Terms uv_terms = bucket.getAggregations().get(Constants.UV);
					for (Terms.Bucket uv_bucket : uv_terms.getBuckets()) {
						uv += Integer.parseInt(uv_bucket.getKeyAsString());
					}

					Terms pv_terms = bucket.getAggregations().get(Constants.PV);
					for (Terms.Bucket pv_bucket : pv_terms.getBuckets()) {
						pv += Integer.parseInt(pv_bucket.getKeyAsString());
					}

					Terms session_terms = bucket.getAggregations().get(Constants.SESSION);
					for (Terms.Bucket session_bucket : session_terms.getBuckets()) {
						session += Integer.parseInt(session_bucket.getKeyAsString());
					}

					Terms domain_session_terms = bucket.getAggregations().get(Constants.DOMAIN_SESSION);
					for (Terms.Bucket domain_session_bucket : domain_session_terms.getBuckets()) {
						domain_session += Integer.parseInt(domain_session_bucket.getKeyAsString());
					}

					Terms session_duration_terms = bucket.getAggregations().get(Constants.SESS_DURATION);
					for (Terms.Bucket session_duration_bucket : session_duration_terms.getBuckets()) {
						sess_duration += Double.parseDouble(session_duration_bucket.getKeyAsString());
					}

					Terms bounce_terms = bucket.getAggregations().get(Constants.BOUNCES);
					for (Terms.Bucket bounce_bucket : bounce_terms.getBuckets()) {
						bounces += Double.parseDouble(bounce_bucket.getKeyAsString());
					}

					Terms page_load_time_terms = bucket.getAggregations().get(Constants.PAGE_LOAD_TIME);
					for (Terms.Bucket page_load_time_bucket : page_load_time_terms.getBuckets()) {
						page_load_time += Double.parseDouble(page_load_time_bucket.getKeyAsString());
					}

					Terms page_load_sample_terms = bucket.getAggregations().get(Constants.PAGE_LOAD_SAMPLE);
					for (Terms.Bucket page_load_sample_bucket : page_load_sample_terms.getBuckets()) {
						page_load_sample += Double.parseDouble(page_load_sample_bucket.getKeyAsString());
					}

					if (uv != 0) {
						pvPerUv = ((Long) pv).doubleValue() / uv;
						revPerUser = ad_ex_revenue / uv;
					}

					if (domain_session != 0) {
						avg_ses_duration = sess_duration / domain_session;
						bounce_rate = (bounces * 100) / domain_session;
						page_depth = ((Long) pv).doubleValue() / domain_session;
						impPerSession = ((Long) total_impressions).doubleValue() / domain_session;
						revPerSession = ad_ex_revenue / domain_session;
					}

					if (pv != 0) {
						impPerPage = total_impressions / ((Long) pv).doubleValue();
						revPerPage = ad_ex_revenue / pv;
					}

					if (page_load_sample != 0) {
						avg_page_load_time = ((Long) page_load_time).doubleValue() / (page_load_sample * 1000);
					}

					if (ad_ex_impressions != 0) {
						e_cpm = ad_ex_revenue / (ad_ex_impressions / 1000);
					}

					if (total_impressions + unfilled_impressions != 0) {
						adx_cov = (ad_ex_impressions * 100) / (total_impressions + unfilled_impressions);
					}

					adMetrics.setUv(uv);
					adMetrics.setPv(pv);
					adMetrics.setSession(session);
					adMetrics.setPage_depth(page_depth);
					adMetrics.setTotal_impressions(total_impressions + unfilled_impressions);
					adMetrics.setAd_ex_ctr(ad_ex_ctr);
					adMetrics.setE_cpm(e_cpm);
					adMetrics.setAdx_cov(adx_cov);
					adMetrics.setAd_ex_revenue(ad_ex_revenue);
					adMetrics.setTotal_revenue(ad_ex_revenue);
					adMetrics.setAvg_ses_duration(avg_ses_duration);
					adMetrics.setBounce_rate(bounce_rate);
					adMetrics.setPvPerUv(pvPerUv);
					adMetrics.setImpPerPage(impPerPage);
					adMetrics.setRevPerPage(revPerPage);
					adMetrics.setRevPerSession(revPerSession);
					adMetrics.setRevPerUser(revPerUser);
					adMetrics.setImpPerSession(impPerSession);
					adMetrics.setAvg_page_load_time(avg_page_load_time);
					adMetrics.setAd_ex_impressions(ad_ex_impressions);
					adMetrics.setAd_ex_clicks(ad_ex_clicks);
					adMetrics.setSess_duration(sess_duration);
					adMetrics.setBounces(bounces);
					adMetrics.setPage_load_time(page_load_time);
					adMetrics.setPage_load_sample(page_load_sample);
					adMetrics.setUnfilled_impressions(unfilled_impressions);
					adMetrics.setTotal_impressions_raw(total_impressions);
					records.put(bucket.getKeyAsString(), adMetrics);
				}

			} 
		}
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getAdMetricsData.", e);
		}
		log.info("Retrieving getAdMetricsData; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	private Map<String, Long> getFacebookPopularity(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Long> records = new LinkedHashMap<>();
		try {
			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);
			Histogram.Order order = null;

			if (query.isOrderAsc())
				order = Histogram.Order.KEY_ASC;
			else
				order = Histogram.Order.KEY_DESC;

			BoolQueryBuilder qb = getSocialDecodeQuery(query);
			DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("interval").field(query.getDateField())
					.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order);

			if (query.getStoryid() == null) {
				qb.must(QueryBuilders.rangeQuery(Constants.POPULARITY_SCORE).gte(0));
				if (query.getInterval().contains("M")) {
					qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.MONTH));
				}

				else if (query.getInterval().contains("d")) {
					qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.DAY));
				}

				else if (query.getInterval().contains("h")) {
					qb.must(QueryBuilders.termQuery(Constants.INTERVAL, Constants.HOUR));
				}
				aggBuilder
				.subAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD));
				SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_POPULAR)
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb).addAggregation(aggBuilder).setSize(0)
						.execute().actionGet();

				Histogram histogram = res.getAggregations().get("interval");
				for (Histogram.Bucket bucket : histogram.getBuckets()) {
					Cardinality stories = bucket.getAggregations().get("storyCount");
					records.put(bucket.getKeyAsString(), stories.getValue());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebook popularity.", e);
		}
		log.info("Retrieving facebook popularity; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String, List<UserSession>> getUserSessionDetails(GenericQuery genericQuery) {
		long start = System.currentTimeMillis();
		Map<String, List<UserSession>> userSessionMap = new LinkedHashMap<>();
		// Add start date and end date match conditions to query
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD);
		if (StringUtils.isNotBlank(genericQuery.getStartDate())) {
			rangeQueryBuilder.from(genericQuery.getStartDate());
		}
		if (StringUtils.isNotBlank(genericQuery.getEndDate())) {
			rangeQueryBuilder.to(genericQuery.getEndDate());
		}
		if (StringUtils.isNotBlank(genericQuery.getStartDate()) || StringUtils.isNotBlank(genericQuery.getEndDate())) {
			boolQuery.must(rangeQueryBuilder);
		} 
		else {
			rangeQueryBuilder.from(DateUtil.getPreviousDate().replaceAll("_", "-"));
			boolQuery.must(rangeQueryBuilder);
		}
		List<String> userIdList = new ArrayList<>();
		userIdList = Arrays.asList(genericQuery.getUserId().split(","));
		boolQuery.must(QueryBuilders.termsQuery(Constants.SESSION_ID_FIELD, userIdList));
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.USER_PROFILE_DAYWISE)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(500)
				.addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC);
		SearchResponse sr = searchRequestBuilder.execute().actionGet();
		SearchHit[] searchHits = sr.getHits().getHits();
		for (SearchHit searchHit : searchHits) {
			String userSessionDate = (String) searchHit.getSource().get(Constants.DATE_TIME_FIELD);

			UserSession userSession = new UserSession();
			userSession.setUser_id((String) searchHit.getSource().get(Constants.SESSION_ID_FIELD));
			userSession.setSession_count(String.valueOf((Integer) searchHit.getSource().get(Constants.SESSION_COUNT)));
			if (!userSessionMap.containsKey(userSessionDate)) {
				List<UserSession> userSessionList = new ArrayList<>();
				userSessionList.add(userSession);
				userSessionMap.put(userSessionDate, userSessionList);
			} else {
				userSessionMap.get(userSessionDate).add(userSession);
			}
		}
		log.info("Session Detail for users [" + genericQuery.getUserId() + "] " + userSessionMap
				+ "; Execution Time:(Seconds) " + (System.currentTimeMillis() - start) / 1000.0);
		return userSessionMap;
	}

	public Map<String, List<UserSession>> getUserSessionBuckets(GenericQuery genericQuery) {
		long start = System.currentTimeMillis();
		Map<String, List<UserSession>> userSessionMap = new LinkedHashMap<>();
		try {
			// Add start date and end date match conditions to query
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD);
			if (StringUtils.isNotBlank(genericQuery.getStartDate())) {
				rangeQueryBuilder.from(genericQuery.getStartDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getEndDate())) {
				rangeQueryBuilder.to(genericQuery.getEndDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getStartDate())
					|| StringUtils.isNotBlank(genericQuery.getEndDate())) {
				boolQuery.must(rangeQueryBuilder);
			} else {
				rangeQueryBuilder.from(DateUtil.getPreviousDate().replaceAll("_", "-"));
				boolQuery.must(rangeQueryBuilder);
			}
			List<String> hostList = new ArrayList<>();
			hostList = Arrays.asList(genericQuery.getHosts().split(","));
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hostList));

			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.USER_PROFILE_DAYWISE)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000)).setQuery(boolQuery)
					.setSize(0)
					.addAggregation(AggregationBuilders.dateHistogram("DATE").field("datetime")
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							// AggregationBuilders.terms("DATE").field(Constants.DATE_TIME_FIELD)
							.subAggregation(AggregationBuilders.terms("GROUP_BY_SESSION_COUNT")
									.field(Constants.SESSION_COUNT).order(Terms.Order.term(true))));
			SearchResponse sr = searchRequestBuilder.execute().actionGet();

			// Terms groupByDate = sr.getAggregations().get("DATE");
			InternalDateHistogram groupByDate = sr.getAggregations().get("DATE");
			List<Integer> sessionCountKeysList = Arrays.asList(1, 2, 3, 4, 5);
			for (Bucket entry : groupByDate.getBuckets()) {

				Terms groupBySessionCount = entry.getAggregations().get("GROUP_BY_SESSION_COUNT");
				String userSessionDate = entry.getKeyAsString();// AsText().toString().split("T")[0];
				long totalUsers = entry.getDocCount();
				long usersWithLessThan3Sessions = 0;
				for (Terms.Bucket sessionCountBucket : groupBySessionCount.getBuckets()) {
					if (sessionCountKeysList.contains(Integer.valueOf(sessionCountBucket.getKeyAsString()))) {
						UserSession userSession = new UserSession();
						userSession.setSession_count(sessionCountBucket.getKeyAsString());
						userSession.setUser_count(String.valueOf(sessionCountBucket.getDocCount()));
						if (!userSessionMap.containsKey(userSessionDate)) {
							List<UserSession> userSessionList = new ArrayList<>();
							userSessionList.add(userSession);
							userSessionMap.put(userSessionDate, userSessionList);
						} else {
							userSessionMap.get(userSessionDate).add(userSession);
						}
						usersWithLessThan3Sessions = usersWithLessThan3Sessions + sessionCountBucket.getDocCount();
					} else {
						break;
					}
				}

				UserSession userSession = new UserSession();
				userSession.setSession_count(">5");
				userSession.setUser_count(String.valueOf(totalUsers - usersWithLessThan3Sessions));
				userSessionMap.get(userSessionDate).add(userSession);
			}
			log.info("User Session Buckets. Execution Time:(Seconds) " + (System.currentTimeMillis() - start) / 1000.0);
			;

		} catch (Exception e) {
			log.error(e);
		}
		return userSessionMap;
	}

	public Map<String, List<FrequencyDetailResponse>> getUserSessionBucketsWithDetails(GenericQuery genericQuery) {
		long start = System.currentTimeMillis();
		Map<String, List<FrequencyDetailResponse>> userSessionMap = new LinkedHashMap<>();
		try {
			// Add start date and end date match conditions to query
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD);
			if (StringUtils.isNotBlank(genericQuery.getStartDate())) {
				rangeQueryBuilder.from(genericQuery.getStartDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getEndDate())) {
				rangeQueryBuilder.to(genericQuery.getEndDate());
			}
			if (StringUtils.isNotBlank(genericQuery.getStartDate())
					|| StringUtils.isNotBlank(genericQuery.getEndDate())) {
				boolQuery.must(rangeQueryBuilder);
			} else {
				rangeQueryBuilder.from(DateUtil.getPreviousDate().replaceAll("_", "-"));
				boolQuery.must(rangeQueryBuilder);
			}
			List<String> hostList = new ArrayList<>();
			hostList = Arrays.asList(genericQuery.getHosts().split(","));
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, hostList));

			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Indexes.USER_PROFILE_DAYWISE)
					.setTypes(MappingTypes.MAPPING_REALTIME).setTimeout(new TimeValue(2000)).setQuery(boolQuery)
					.setSize(0)
					.addAggregation(AggregationBuilders.dateHistogram("DATE").field("datetime")
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							// AggregationBuilders.terms("DATE").field(Constants.DATE_TIME_FIELD)
							.subAggregation(AggregationBuilders.terms("GROUP_BY_SESSION_COUNT")
									.field(Constants.SESSION_COUNT).order(Terms.Order.term(true)).subAggregation(
											AggregationBuilders.terms("GROUP_BY_SOURCE").field(Constants.REF_HOST)
											.size(5).subAggregation(
													AggregationBuilders.terms("GROUP_BY_CATEGORY")
													.field(Constants.SECTION).size(5))))

							);
			SearchResponse sr = searchRequestBuilder.execute().actionGet();

			// Terms groupByDate = sr.getAggregations().get("DATE");
			InternalDateHistogram groupByDate = sr.getAggregations().get("DATE");
			List<Integer> sessionCountKeysList = Arrays.asList(1, 2, 3, 4, 5);
			for (Bucket entry : groupByDate.getBuckets()) {

				Terms groupBySessionCount = entry.getAggregations().get("GROUP_BY_SESSION_COUNT");
				String userSessionDate = entry.getKeyAsString();// AsText().toString().split("T")[0];
				long totalUsers = entry.getDocCount();
				long usersWithLessThan5Sessions = 0;
				for (Terms.Bucket sessionCountBucket : groupBySessionCount.getBuckets()) {
					FrequencyDetailResponse frequencyDetailResponse = new FrequencyDetailResponse();
					if (sessionCountKeysList.contains(Integer.valueOf(sessionCountBucket.getKeyAsString()))) {
						// UserSession userSession = new UserSession();
						frequencyDetailResponse.setSession_count(sessionCountBucket.getKeyAsString());
						frequencyDetailResponse.setUser_count(sessionCountBucket.getDocCount());
						frequencyDetailResponse.setDate(userSessionDate);

						// Get Source information

						Terms groupBySource = sessionCountBucket.getAggregations().get("GROUP_BY_SOURCE");
						List<Source> sources = new ArrayList<>();
						long totalSourceUsers = 0;
						for (Terms.Bucket sourceBucket : groupBySource.getBuckets()) {
							Source source = frequencyDetailResponse.new Source();
							source.setSource(sourceBucket.getKeyAsString());
							source.setUser_count(sourceBucket.getDocCount());
							totalSourceUsers = totalSourceUsers + source.getUser_count();
							List<Category> categories = new ArrayList<>();
							Terms groupByCategory = sourceBucket.getAggregations().get("GROUP_BY_CATEGORY");
							long totalCatUsers = 0;
							for (Terms.Bucket categoryBucket : groupByCategory.getBuckets()) {
								Category category = frequencyDetailResponse.new Category();
								category.setCategory(categoryBucket.getKeyAsString());
								category.setUser_count(categoryBucket.getDocCount());
								totalCatUsers = totalCatUsers + category.getUser_count();
								categories.add(category);
							}
							// Add OTHER category to match count of user
							Category category = frequencyDetailResponse.new Category();
							category.setCategory("OTHER");
							category.setUser_count(source.getUser_count() - totalCatUsers);
							if (category.getUser_count() > 0) {
								categories.add(category);
							}

							source.setCategories(categories);
							sources.add(source);
						}
						// Add OTHER source to match count of user
						Source source = frequencyDetailResponse.new Source();
						source.setSource("OTHER");
						source.setUser_count(frequencyDetailResponse.getUser_count() - totalSourceUsers);
						if (source.getUser_count() > 0) {
							sources.add(source);
						}

						frequencyDetailResponse.setSources(sources);

						if (!userSessionMap.containsKey(userSessionDate)) {
							List<FrequencyDetailResponse> userSessionList = new ArrayList<>();
							userSessionList.add(frequencyDetailResponse);
							userSessionMap.put(userSessionDate, userSessionList);
						} else {
							userSessionMap.get(userSessionDate).add(frequencyDetailResponse);
						}
						usersWithLessThan5Sessions = usersWithLessThan5Sessions + sessionCountBucket.getDocCount();
					} else {
						break;
					}
				}

				FrequencyDetailResponse frequencyDetailResponse = new FrequencyDetailResponse();
				frequencyDetailResponse.setSession_count(">5");
				frequencyDetailResponse.setUser_count((totalUsers - usersWithLessThan5Sessions));
				userSessionMap.get(userSessionDate).add(frequencyDetailResponse);
			}
			log.info("User Session Buckets. Execution Time:(Seconds) " + (System.currentTimeMillis() - start) / 1000.0);
			;

		} catch (Exception e) {
			log.error("Error Occured in getUserSessionBucketsWithDetails.", e);
		}
		return userSessionMap;
	}

	public Integer getSessionCountForUser(String sessionId) {
		long startTime = System.currentTimeMillis();
		Integer result = null;

		GetResponse response = client.prepareGet().setIndex(Indexes.USER_PERSONALIZATION_STATS)
				.setType(MappingTypes.MAPPING_REALTIME).setId(sessionId).execute().actionGet();

		if (!response.isExists() || !response.getSourceAsMap().containsKey(Constants.SESSION_COUNT)) {
			log.info("No session information found for user " + sessionId);
			result = 0;
		} else {
			result = (Integer) response.getSourceAsMap().get(Constants.SESSION_COUNT);
		}

		log.info("Session Count for user [" + sessionId + "] is " + result + "; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}

	public List<WisdomArticleDiscovery> getArticleDiscovery(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<WisdomArticleDiscovery> records = new ArrayList<WisdomArticleDiscovery>();

		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();

		if (!StringUtils.isBlank(query.getStartDate())) {
			startDatetime = query.getStartDate();
		}

		if (!StringUtils.isBlank(query.getEndDate())) {
			endDatetime = query.getEndDate();
		}

		try {
			String indexName = Indexes.IDENTIFICATION_STORY_DETAIL;
			if (query.getInterval() != null) {
				indexName = "identification_" + DateUtil.getCurrentDate();
				int interval = (-1) * Integer.parseInt(query.getInterval());
				startDatetime = DateUtil.addHoursToCurrentTime(interval);
				endDatetime = DateUtil.getCurrentDateTime();
			}

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).lte(endDatetime))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			if (query.getWidget_name() != null) {
				boolQuery.must(QueryBuilders.termQuery(Constants.WIDGET_NAME, query.getWidget_name()));
			}

			String include[] = { Constants.STORY_ID_FIELD, Constants.URL, Constants.STORY_PUBLISH_TIME, Constants.TITLE,
					Constants.WIDGET_NAME, Constants.CHANNEL_SLNO };

			if (indexName.equalsIgnoreCase(Indexes.IDENTIFICATION_STORY_DETAIL)) {
				SearchResponse sr = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery)
						.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
								.size(query.getCount()).order(Order.aggregation(query.getParameter(), false))
								.subAggregation(
										AggregationBuilders.sum(Constants.IMPRESSIONS).field(Constants.IMPRESSIONS))
								.subAggregation(AggregationBuilders.sum(Constants.VIEWS).field(Constants.VIEWS))
								.subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(include, new String[] {}).size(1)))
						.setSize(0).execute().actionGet();

				Terms storyTermAgg = sr.getAggregations().get("storyid");

				for (Terms.Bucket buckets : storyTermAgg.getBuckets()) {
					WisdomArticleDiscovery wisArDis = new WisdomArticleDiscovery();

					Sum impression_sum = buckets.getAggregations().get(Constants.IMPRESSIONS);
					wisArDis.setImpression((long) impression_sum.getValue());

					Sum views_sum = buckets.getAggregations().get(Constants.VIEWS);
					wisArDis.setclicks((long) views_sum.getValue());

					wisArDis.setCtr(wisArDis.getClicks().doubleValue() / wisArDis.getImpression() * 100);

					TopHits topHits = buckets.getAggregations().get("top");
					wisArDis.setStoryid(
							topHits.getHits().getHits()[0].getSource().get(Constants.STORY_ID_FIELD).toString());
					wisArDis.setUrl(topHits.getHits().getHits()[0].getSource().get(Constants.URL).toString());
					wisArDis.setPublished_date(
							topHits.getHits().getHits()[0].getSource().get(Constants.STORY_PUBLISH_TIME).toString());
					wisArDis.setChannel_slno(
							topHits.getHits().getHits()[0].getSource().get(Constants.CHANNEL_SLNO).toString());
					records.add(wisArDis);
				}
			}

			else if (indexName.equals("identification_" + DateUtil.getCurrentDate())) {
				SearchResponse sr = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery)
						.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
								.size(query.getCount())
								.subAggregation(AggregationBuilders.terms("event_type").field(Constants.EVENT_TYPE))
								.subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(include, new String[] {}).size(1)))
						.setSize(0).execute().actionGet();

				Terms storyTermAgg = sr.getAggregations().get("storyid");

				for (Terms.Bucket buckets : storyTermAgg.getBuckets()) {
					WisdomArticleDiscovery wisArDis = new WisdomArticleDiscovery();

					Terms event_term_data = buckets.getAggregations().get("event_type");
					for (Terms.Bucket event_termbucket : event_term_data.getBuckets()) {
						if (event_termbucket.getKeyAsString().equals(String.valueOf(Constants.EVENT_TYPE_IMPRESSION))) {
							wisArDis.setImpression((long) event_termbucket.getDocCount());
						} else if (event_termbucket.getKeyAsString().equals(String.valueOf(Constants.EVENT_TYPE_CLICK))) {
							wisArDis.setclicks((long) event_termbucket.getDocCount());
						} else {
							log.warn("Invalid event type "+event_termbucket.getKeyAsString()+".  Ignoring it..");
							continue;
						}						
					}
					wisArDis.setCtr(wisArDis.getClicks().doubleValue() / wisArDis.getImpression() * 100);
					TopHits topHits = buckets.getAggregations().get("top");
					wisArDis.setStoryid(topHits.getHits().getHits()[0].getSource().get(Constants.STORY_ID_FIELD).toString());
					wisArDis.setUrl(topHits.getHits().getHits()[0].getSource().get(Constants.URL).toString());
					wisArDis.setPublished_date(topHits.getHits().getHits()[0].getSource().get(Constants.STORY_PUBLISH_TIME).toString());
					wisArDis.setChannel_slno(topHits.getHits().getHits()[0].getSource().get(Constants.CHANNEL_SLNO).toString());
					records.add(wisArDis);
				}
			}

		} catch (Exception e) {
			log.error("Error Occured in getArticleDiscovery.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of getArticleDiscovery: " + records.size() + "; Execution Time:(Seconds) "
				+ (endTime - startTime) / 1000.0 + "Result of getArticleDiscovery" + records);

		return records;

	}

	public Map<String, WisdomArticleDiscovery> getWidgetArticleDiscovery(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, WisdomArticleDiscovery> records = new LinkedHashMap<>();
		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();

		try {
			if (!StringUtils.isBlank(query.getStartDate())) {
				startDatetime = query.getStartDate();
			}

			if (!StringUtils.isBlank(query.getEndDate())) {
				endDatetime = query.getEndDate();
			}
			String include[] = { Constants.WIDGET_NAME };
			String indexName = Indexes.IDENTIFICATION_STORY_DETAIL;

			if (query.getInterval() != null) {
				indexName = "identification_" + DateUtil.getCurrentDate();
				int interval = (-1) * Integer.parseInt(query.getInterval());
				startDatetime = DateUtil.addHoursToCurrentTime(interval);
				endDatetime = DateUtil.getCurrentDateTime();
			}

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).lte(endDatetime))
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()))
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));

			if (query.getWidget_name() != null) {
				boolQuery.must(QueryBuilders.termQuery(Constants.WIDGET_NAME, query.getWidget_name()));
			}

			// putting conditions on index name

			if (indexName.equalsIgnoreCase("identification_" + DateUtil.getCurrentDate())) {
				SearchResponse sr = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery)
						.addAggregation(AggregationBuilders.terms("referer_path").field(Constants.REFERER_PATH)
								.size(query.getCount())
								.subAggregation(AggregationBuilders.terms("event_type").field(Constants.EVENT_TYPE))
								.subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(include, new String[] {}).size(1)))
						.setSize(0).execute().actionGet();
				// //System.out.println("realtime ..... " + sr);
				Terms storyTermAgg = sr.getAggregations().get("referer_path");
				for (Terms.Bucket buckets : storyTermAgg.getBuckets()) {
					WisdomArticleDiscovery wisArDis = new WisdomArticleDiscovery();

					Terms event_term_data = buckets.getAggregations().get("event_type");
					for (Terms.Bucket event_termbucket : event_term_data.getBuckets()) {
						if (event_termbucket.getKey().equals("1")) {
							wisArDis.setImpression((long) event_termbucket.getDocCount());
						} else if (event_termbucket.getKey().equals("2")) {
							wisArDis.setclicks((long) event_termbucket.getDocCount());
						}
					}
					wisArDis.setCtr(wisArDis.getClicks().doubleValue() / wisArDis.getImpression() * 100);

					TopHits topHits = buckets.getAggregations().get("top");
					wisArDis.setWidget_name(
							topHits.getHits().getHits()[0].getSource().get(Constants.WIDGET_NAME).toString());

					records.put(buckets.getKeyAsString(), wisArDis);
				}
			}

			else if (indexName.equalsIgnoreCase(Indexes.IDENTIFICATION_STORY_DETAIL)) {
				SearchResponse sr = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery)
						.addAggregation(AggregationBuilders.terms("referer_path").field(Constants.REFERER_PATH)
								.size(query.getCount())
								.subAggregation(
										AggregationBuilders.sum(Constants.IMPRESSIONS).field(Constants.IMPRESSIONS))
								.subAggregation(AggregationBuilders.sum(Constants.VIEWS).field(Constants.VIEWS))
								.subAggregation(AggregationBuilders.topHits("top")
										.fetchSource(include, new String[] {}).size(1)))
						.setSize(0).execute().actionGet();
				// //System.out.println("identification story detail ...."+sr);
				Terms referrer_path_term_agg = sr.getAggregations().get("referer_path");
				for (Terms.Bucket referrer_path_buckets : referrer_path_term_agg.getBuckets()) {
					WisdomArticleDiscovery wad_rp = new WisdomArticleDiscovery();
					Sum rp_impression_sum = referrer_path_buckets.getAggregations().get(Constants.IMPRESSIONS);

					wad_rp.setImpression((long) rp_impression_sum.getValue());

					Sum rp_views_sum = referrer_path_buckets.getAggregations().get(Constants.VIEWS);
					wad_rp.setclicks((long) rp_views_sum.getValue());

					if (wad_rp.getImpression() == 0)
						continue;
					wad_rp.setCtr(wad_rp.getClicks().doubleValue() / wad_rp.getImpression() * 100);

					TopHits tophits = referrer_path_buckets.getAggregations().get("top");
					wad_rp.setWidget_name(
							tophits.getHits().getHits()[0].getSource().get(Constants.WIDGET_NAME).toString());

					records.put(referrer_path_buckets.getKeyAsString(), wad_rp);
				}

			}
		} catch (Exception e) {
			log.error("Error Occured in getWidgetArticleDiscovery.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of getWidgetArticleDiscovery: " + records.size() + "; Execution Time:(Seconds) "
				+ (endTime - startTime) / 1000.0 + "Result of getWidgetArticleDiscovery" + records);
		return records;
	}

	private Map<String, WisdomArticleRating> getAvgArticleRating(ArrayList<String> storyList) {
		//// System.out.println(storyList);
		Map<String, WisdomArticleRating> finalmap = new HashMap<>();
		QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, storyList));
		SearchResponse ser = client.prepareSearch(Indexes.ARTICLE_RATING_DETAIL).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(qb)
				.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
						.subAggregation(AggregationBuilders.terms("terms_rating").field(Constants.RATING))
						.subAggregation(AggregationBuilders.avg("average_rating").field(Constants.RATING)))
				.setSize(0).execute().actionGet();

		Terms storyTermsAgg = ser.getAggregations().get("storyid");
		for (Terms.Bucket storybucket : storyTermsAgg.getBuckets()) {
			WisdomArticleRating war = new WisdomArticleRating();

			war.setTotal_users(storybucket.getDocCount());

			Terms rating_terms_agg = storybucket.getAggregations().get("terms_rating");
			Map<String, Object> breakupMap = new HashMap<>();

			for (Terms.Bucket rating_terms_bucket : rating_terms_agg.getBuckets()) {

				breakupMap.put(rating_terms_bucket.getKeyAsString(), rating_terms_bucket.getDocCount());
				Avg avgRatingAgg = storybucket.getAggregations().get("average_rating");

				Double average_rating = 0.0;
				if (ser.getHits().getTotalHits() != 0) {
					average_rating = avgRatingAgg.getValue();
					war.setAverage_rating(average_rating);
				}

			}
			war.setBreakup(breakupMap);

			finalmap.put(storybucket.getKeyAsString(), war);
		}
		return finalmap;
	}

	public List<String> getArticleFeedback(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<String> feedbackList = new ArrayList<>();
		;
		try {
			String storyid = query.getStoryid();
			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, storyid));
			QueryBuilder fqb = qb.filter(QueryBuilders.existsQuery(Constants.FEEDBACK));
			SearchResponse ser = client.prepareSearch(Indexes.ARTICLE_RATING_DETAIL)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(fqb).setSize(10).execute().actionGet();
			for (SearchHit hits : ser.getHits().getHits()) {
				Map<String, Object> record = hits.getSource();
				if (!StringUtils.isBlank(record.get(Constants.FEEDBACK).toString())) {
					feedbackList.add(record.get(Constants.FEEDBACK).toString());
				}
			}
		} catch (Exception e) {
			log.error("Error Occured in getArticleFeedback.", e);

		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of getArticleFeedback: " + feedbackList.size() + "; Execution Time:(Seconds) "
				+ (endTime - startTime) / 1000.0 + "Result of getArticleFeedback" + feedbackList);
		return feedbackList;
	}

	public Map<String, Object> getUserFrequencybyStory(WisdomQuery query) {

		long startTime = System.currentTimeMillis();
		Map<String, Object> finalMap = new HashMap<>();
		try {
			BoolQueryBuilder bqb = new BoolQueryBuilder();
			bqb.must(QueryBuilders.termQuery(Constants.STORIES, query.getStoryid()));
			bqb.must(QueryBuilders.rangeQuery(Constants.SESSION_COUNT).lte(100));

			SearchResponse sr = client.prepareSearch(Indexes.USER_PERSONALIZATION_STATS)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb).addAggregation(AggregationBuilders
							.terms("session_count").field(Constants.SESSION_COUNT).order(Order.term(true)))
					.execute().actionGet();
			long loyal_total_count = 0;
			long average_total_count = 0;
			long low_total_count = 0;
			long total_doc_count = sr.getHits().getTotalHits();

			Map<Object, Object> sessionMap = new LinkedHashMap<>();
			Map<Object, Object> loyal = new LinkedHashMap<>();
			Map<Object, Object> moderate = new LinkedHashMap<>();
			Map<Object, Object> low = new LinkedHashMap<>();

			Terms storiesTermAgg = sr.getAggregations().get("session_count");
			for (Terms.Bucket buckets : storiesTermAgg.getBuckets()) {
				Long session_key = Long.parseLong(buckets.getKeyAsString());
				Long document_count = buckets.getDocCount();
				if (session_key != 0) {

					if (session_key <= 1) {
						low_total_count += document_count;
						low.put("total_count", low_total_count);
						// low.put("session_count: " + session_key, "user_count
						// : " + document_count);
						sessionMap.put("low", low);
					}

					if (session_key >= 2 && session_key <= 4) {
						average_total_count += document_count;
						moderate.put("total_count", average_total_count);
						// moderate.put("session_count: " + session_key,
						// "user_count : " + document_count);
						sessionMap.put("average", moderate);
					}

					if (session_key >= 5) {
						loyal_total_count = total_doc_count - (low_total_count + average_total_count);
						loyal.put("total_count", loyal_total_count);
						// loyal.put("session_count: " + session_key,
						// "user_count : " + document_count);
						sessionMap.put("frequent_user", loyal);
					}
				}
			}
			finalMap.put("total documents", total_doc_count);
			finalMap.put(query.getStoryid(), sessionMap);
		} catch (Exception e) {
			log.error("Error Occured in getUserFrequencybyStory.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of getUserFrequencybyStory: " + finalMap.size() + "; Execution Time:(Seconds) "
				+ (endTime - startTime) / 1000.0 + "Result of getUserFrequencybyStory" + finalMap);
		return finalMap;
	}

	public Map<String, Object> getFacebookUcbFlag(WisdomQuery query) {

		long startTime = System.currentTimeMillis();
		Map<String, Object> finalMap = new HashMap<>();
		try {

			List<String> stories = Arrays.asList(query.getStoryid().split(","));
			BoolQueryBuilder bqb = new BoolQueryBuilder();
			bqb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, stories));

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(bqb)
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.terms("pageViewsTracker")
									.field(Constants.TRACKER).size(10).order(Order.aggregation("pvs", false))
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)))
							.size(stories.size()))
					.setSize(0).execute().actionGet();

			MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
			Terms storyBuckets = res.getAggregations().get("stories");

			for (Terms.Bucket story : storyBuckets.getBuckets()) {

				HashMap<String, Boolean> storyMap = new HashMap<>();
				storyMap.put("isUCB", false);
				storyMap.put("isFacebook", false);
				Terms pvTracker = story.getAggregations().get("pageViewsTracker");
				for (Terms.Bucket tracker : pvTracker.getBuckets()) {
					if (tracker.getKeyAsString().equalsIgnoreCase(Constants.NEWS_UCB)) {
						storyMap.put("isUCB", true);
					}
				}
				multiSearchRequestBuilder
				.add(client.prepareSearch(Indexes.FB_DASHBOARD).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(QueryBuilders.termQuery(Constants.BHASKARSTORYID, story.getKey())));
				finalMap.put(story.getKeyAsString(), storyMap);
			}

			MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
			for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
				SearchResponse response = item.getResponse();
				if (response.getHits().getHits().length > 0) {
					Map<String, Object> source = response.getHits().getHits()[0].getSource();
					((HashMap<String, Boolean>) finalMap.get(source.get(Constants.BHASKARSTORYID).toString()))
					.put("isFacebook", true);
				}
			}

		} catch (Exception e) {
			log.error("Error Occured in getFacebookUcbFlag.", e);
		}
		long endTime = System.currentTimeMillis();
		log.info("Result size of getFacebookUcbFlag: " + finalMap.size() + "; Execution Time:(Seconds) "
				+ (endTime - startTime) / 1000.0 + "Result of getFacebookUcbFlag" + finalMap);
		return finalMap;
	}

	public Map<String, FacebookPageInsights> getFacebookPageInsightsByInterval(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, FacebookPageInsights> records = new LinkedHashMap<>();
		try {

			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);
			Histogram.Order order = null;

			if (query.isOrderAsc())
				order = Histogram.Order.KEY_ASC;
			else
				order = Histogram.Order.KEY_DESC;

			DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("interval")
					.field(Constants.DATE_TIME_FIELD).dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'")
					.order(order)
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_LIKES).field(Constants.PAGE_LIKES))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_UNLIKES).field(Constants.PAGE_UNLIKES))
					.subAggregation(
							AggregationBuilders.sum(Constants.PAGE_UNIQUE_VIEWS).field(Constants.PAGE_UNIQUE_VIEWS))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_VIEWS).field(Constants.PAGE_VIEWS))
					.subAggregation(AggregationBuilders.sum(Constants.IA_ALL_VIEWS).field(Constants.IA_ALL_VIEWS))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE)
							.field(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE)
							.field(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_NEGATIVE_FEEDBACK)
							.field(Constants.PAGE_NEGATIVE_FEEDBACK))
					.subAggregation(
							AggregationBuilders.sum(Constants.PAGE_CONSUMPTIONS).field(Constants.PAGE_CONSUMPTIONS))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE)
							.field(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE))
					.subAggregation(AggregationBuilders.sum(Constants.PAGE_POST_ENGAGEMENTS)
							.field(Constants.PAGE_POST_ENGAGEMENTS));

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate())
					.lte(query.getEndDate()));
			if (query.getChannel_slno() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			SearchResponse res = client.prepareSearch(Indexes.FB_PAGE_INSIGHTS).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).addAggregation(aggBuilder).setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					FacebookPageInsights insights = new FacebookPageInsights();

					Sum page_likes_agg = bucket.getAggregations().get(Constants.PAGE_LIKES);
					insights.setPage_likes((long) page_likes_agg.getValue());

					Sum page_unlikes_agg = bucket.getAggregations().get(Constants.PAGE_UNLIKES);
					insights.setPage_unlikes((long) page_unlikes_agg.getValue());

					Sum page_views_agg = bucket.getAggregations().get(Constants.PAGE_VIEWS);
					insights.setPage_views((long) page_views_agg.getValue());

					Sum page_unique_views_agg = bucket.getAggregations().get(Constants.PAGE_UNIQUE_VIEWS);
					insights.setPage_unique_views((long) page_unique_views_agg.getValue());

					Sum ia_all_views_agg = bucket.getAggregations().get(Constants.IA_ALL_VIEWS);
					insights.setIa_all_views((long) ia_all_views_agg.getValue());

					Sum page_negative_feedback_agg = bucket.getAggregations().get(Constants.PAGE_NEGATIVE_FEEDBACK);
					insights.setPage_negative_feedback((long) page_negative_feedback_agg.getValue());

					Sum page_negative_feedback_by_type_agg = bucket.getAggregations()
							.get(Constants.PAGE_NEGATIVE_FEEDBACK_BY_TYPE);
					insights.setPage_negative_feedback_by_type(
							(long) page_negative_feedback_by_type_agg.getValue());

					Sum page_positive_feedback_agg = bucket.getAggregations().get(Constants.PAGE_POSITIVE_FEEDBACK_BY_TYPE);
					insights.setPage_positive_feedback_by_type(
							(long) page_positive_feedback_agg.getValue());

					Sum page_consumptions_agg = bucket.getAggregations().get(Constants.PAGE_CONSUMPTIONS);
					insights.setPage_consumptions((long) page_consumptions_agg.getValue());

					Sum page_consumptions_by_type_agg = bucket.getAggregations()
							.get(Constants.PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE);
					insights.setPage_consumptions_by_consumption_type(
							(long) page_consumptions_by_type_agg.getValue());

					Sum page_post_engagements_agg = bucket.getAggregations().get(Constants.PAGE_POST_ENGAGEMENTS);
					insights.setPage_post_engagements((long) page_post_engagements_agg.getValue());

					records.put(bucket.getKeyAsString(), insights);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebookPageInsightsByInterval.", e);
		}
		log.info("Retrieving facebookPageInsightsByInterval; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String, Map<String, Object>> getTrackerwiseStoryDetail(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		SourcePerformance sourcePerformance = new SourcePerformance();
		Map<String, Map<String, Object>> records = new HashMap<>();
		try {			
			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termsQuery(Constants.TRACKER, query.getTracker()))
					.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, hostTypeList));
			if (query.getStartDate() != null && query.getEndDate() != null) {
				qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate())
						.lte(query.getEndDate()));
			}
			if (query.getStoryid() != null) {
				qb.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, query.getStoryid().split(",")));
			}
			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.terms("tracker").field(Constants.TRACKER).size(10)
									.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
									.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
									.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE)
											.size(5).subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
											.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS)))))
					.setSize(0).execute().actionGet();

			Terms storyBuckets = res.getAggregations().get("stories");
			for (Terms.Bucket story : storyBuckets.getBuckets()) {
				Map<String, Object> storyMap = new HashMap<>();
				long totalPvs = 0;
				long totalMPvs = 0;
				long totalWPvs = 0;
				long totalUvs = 0;
				long totalMUvs = 0;
				long totalWUvs = 0;

				List<StoryPerformance> trackerList = new ArrayList<>();
				Terms trackerBuckets = story.getAggregations().get("tracker");
				for (Terms.Bucket tracker : trackerBuckets.getBuckets()) {
					StoryPerformance storyPerf = new StoryPerformance();
					long tpvs = 0;
					long mpvs = 0;
					long wpvs = 0;
					long tuvs = 0;
					long muvs = 0;
					long wuvs = 0;

					Sum totalPvsAgg = tracker.getAggregations().get("tpvs");
					tpvs = Double.valueOf(totalPvsAgg.getValue()).longValue();
					totalPvs += tpvs;

					Sum totalUvsAgg = tracker.getAggregations().get("tuvs");
					tuvs = Double.valueOf(totalUvsAgg.getValue()).longValue();
					totalUvs += tuvs;

					Terms hostType = tracker.getAggregations().get("host_type");
					for (Terms.Bucket host : hostType.getBuckets()) {
						Sum pvs = host.getAggregations().get("pvs");
						Sum uvs = host.getAggregations().get("uvs");
						if (host.getKey().equals(HostType.MOBILE)) {
							mpvs = (long) pvs.getValue();
							totalMPvs += mpvs;
							muvs = (long) uvs.getValue();
							totalMUvs += muvs;
						} else if (host.getKey().equals(HostType.WEB)) {
							wpvs = (long) pvs.getValue();
							totalWPvs += wpvs;
							wuvs = (long) uvs.getValue();
							totalWUvs += wuvs;
						}
					}

					storyPerf.setTpvs(tpvs);
					storyPerf.setWpvs(wpvs);
					storyPerf.setMpvs(mpvs);
					storyPerf.setTuvs(tuvs);
					storyPerf.setWuvs(wuvs);
					storyPerf.setMuvs(muvs);
					storyPerf.setTracker(tracker.getKeyAsString());
					trackerList.add(storyPerf);
				}
				storyMap.put("pvs", totalPvs);
				storyMap.put("mpvs", totalMPvs);
				storyMap.put("wpvs", totalWPvs);
				storyMap.put("uvs", totalUvs);
				storyMap.put("muvs", totalMUvs);
				storyMap.put("wuvs", totalWUvs);
				storyMap.put("trackers", trackerList);
				records.put(story.getKeyAsString(), storyMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getTrackerwiseStoryDetail.", e);
		}
		log.info("Retrieving getTrackerwiseStoryDetail; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<Competitor> getFbCompetitors(WisdomQuery query){

		long startTime = System.currentTimeMillis();
		List<Competitor> records = new ArrayList<>();
		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getStatus_type()!=null){
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}
			if(query.getCompetitor()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}

			SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms(Constants.COMPETITOR).field(Constants.COMPETITOR).size(1000)
							.subAggregation(AggregationBuilders.sum(Constants.TOTAL_ENGAGEMENT).field(Constants.TOTAL_ENGAGEMENT))
							.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
							.subAggregation(AggregationBuilders.sum(Constants.VIDEO_VIEWS).field(Constants.VIDEO_VIEWS))
							.subAggregation(AggregationBuilders.max(Constants.PAGE_FANS).field(Constants.PAGE_FANS))
							.subAggregation(AggregationBuilders.cardinality("posts").field(Constants.STORY_ID_FIELD))
							.subAggregation(AggregationBuilders.filter("videoFilter", QueryBuilders.termQuery(Constants.STATUS_TYPE, "video"))
									.subAggregation(AggregationBuilders.cardinality("videos").field(Constants.STORY_ID_FIELD))))					
					.setSize(0).execute().actionGet();

			Terms competitorBuckets = res.getAggregations().get(Constants.COMPETITOR);
			for (Terms.Bucket competitor : competitorBuckets.getBuckets()) {
				Competitor comp = new Competitor();
				Sum engagement_agg = competitor.getAggregations().get(Constants.TOTAL_ENGAGEMENT);
				Sum shares_agg = competitor.getAggregations().get(Constants.SHARES);
				Sum video_views_agg = competitor.getAggregations().get(Constants.VIDEO_VIEWS);
				Max page_fans_agg = competitor.getAggregations().get(Constants.PAGE_FANS);
				Cardinality posts_agg = competitor.getAggregations().get("posts");
				Filter videoFilter = competitor.getAggregations().get("videoFilter");
				Cardinality videos_agg = videoFilter.getAggregations().get("videos");

				comp.setName(competitor.getKeyAsString());
				comp.setEngagement(((Double)engagement_agg.getValue()).longValue());
				comp.setShares(((Double)shares_agg.getValue()).longValue());
				comp.setVideoViews(((Double)video_views_agg.getValue()).longValue());
				comp.setFans(((Double)page_fans_agg.getValue()).longValue());
				comp.setPosts(posts_agg.getValue());
				comp.setAvgEngagement((((Double)engagement_agg.getValue()).longValue())/(posts_agg.getValue()));
				comp.setAvgShares((((Double)shares_agg.getValue()).longValue())/(posts_agg.getValue()));
				if(videos_agg.getValue()!=0){
					comp.setAvgVideoViews((((Double)video_views_agg.getValue()).longValue())/(videos_agg.getValue()));
				}
				else {
					comp.setAvgVideoViews(0L);
				}

				records.add(comp);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFbCompetitors.", e);
		}
		log.info("Retrieving getFbCompetitors; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	public Map<String,List<Competitor>> getFbCompetitorsWithInterval(WisdomQuery query){

		long startTime = System.currentTimeMillis();
		Map<String,List<Competitor>> records = new LinkedHashMap<>();
		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getStatus_type()!=null){
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}
			if(query.getCompetitor()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}		

			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);
			Histogram.Order order = null;

			if (query.isOrderAsc())
				order = Histogram.Order.KEY_ASC;
			else
				order = Histogram.Order.KEY_DESC;

			SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
							.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order)							
							.subAggregation(AggregationBuilders.terms(Constants.COMPETITOR).field(Constants.COMPETITOR).size(1000)
									.subAggregation(AggregationBuilders.sum(Constants.TOTAL_ENGAGEMENT).field(Constants.TOTAL_ENGAGEMENT))
									.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
									.subAggregation(AggregationBuilders.sum(Constants.VIDEO_VIEWS).field(Constants.VIDEO_VIEWS))									
									.subAggregation(AggregationBuilders.max(Constants.PAGE_FANS).field(Constants.PAGE_FANS))
									.subAggregation(AggregationBuilders.cardinality("posts").field(Constants.STORY_ID_FIELD))
									.subAggregation(AggregationBuilders.filter("videoFilter", QueryBuilders.termQuery(Constants.STATUS_TYPE, "video"))
											.subAggregation(AggregationBuilders.cardinality("videos").field(Constants.STORY_ID_FIELD)))))
					.setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					Terms competitorBuckets = bucket.getAggregations().get(Constants.COMPETITOR);
					List<Competitor> competitorList = new ArrayList<>();

					for (Terms.Bucket competitor : competitorBuckets.getBuckets()) {
						Competitor comp = new Competitor();
						Sum engagement_agg = competitor.getAggregations().get(Constants.TOTAL_ENGAGEMENT);
						Sum shares_agg = competitor.getAggregations().get(Constants.SHARES);
						Sum video_views_agg = competitor.getAggregations().get(Constants.VIDEO_VIEWS);
						Max page_fans_agg = competitor.getAggregations().get(Constants.PAGE_FANS);
						Cardinality posts_agg = competitor.getAggregations().get("posts");
						Filter videoFilter = competitor.getAggregations().get("videoFilter");
						Cardinality videos_agg = videoFilter.getAggregations().get("videos");

						comp.setName(competitor.getKeyAsString());
						comp.setEngagement(((Double)engagement_agg.getValue()).longValue());
						comp.setShares(((Double)shares_agg.getValue()).longValue());
						comp.setVideoViews(((Double)video_views_agg.getValue()).longValue());						
						comp.setFans(((Double)page_fans_agg.getValue()).longValue());
						comp.setPosts(posts_agg.getValue());
						comp.setAvgEngagement((((Double)engagement_agg.getValue()).longValue())/(posts_agg.getValue()));
						comp.setAvgShares((((Double)shares_agg.getValue()).longValue())/(posts_agg.getValue()));
						if(videos_agg.getValue()!=0){
							comp.setAvgVideoViews((((Double)video_views_agg.getValue()).longValue())/(videos_agg.getValue()));
						}
						else {
							comp.setAvgVideoViews(0L);
						}

						competitorList.add(comp);
					}
					records.put(bucket.getKeyAsString(), competitorList);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFbCompetitorsWithInterval.", e);
		}
		log.info("Retrieving getFbCompetitorsWithInterval; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	public List<CompetitorStory> getFbCompetitorsStories(WisdomQuery query){

		long startTime = System.currentTimeMillis();
		List<CompetitorStory> records = new ArrayList<>();
		String parameter = query.getParameter();

		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getStatus_type()!=null){
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}
			if(query.getCompetitor()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}			

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.FB_COMPETITOR_HISTORY, query.getStartDate(), query.getEndDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.size(query.getCount())
							.subAggregation(AggregationBuilders.max(parameter).field(parameter))
							.order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();

			List<String> stories = new ArrayList<>();

			Terms resTermAgg = res.getAggregations().get("storyid");
			MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();			
			for (Terms.Bucket resBuckets : resTermAgg.getBuckets()) {				
				CompetitorStory story = new CompetitorStory();
				TopHits topHits = resBuckets.getAggregations().get("top");	
				SearchHit[] searchHits= topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					story = gson.fromJson(hit.getSourceAsString(), CompetitorStory.class);
					Map<String, Object> source = hit.getSource();						
					stories.add((String)source.get(Constants.STORY_ID_FIELD));													

					// Prepare query Builder to find similar story count
					BoolQueryBuilder qbFilter = new BoolQueryBuilder();

					String startDate = DateUtil.getPreviousDate(query.getEndDate(), "yyyy-MM-dd", -query.getSim_date_count());					
					qbFilter.must(QueryBuilders.rangeQuery(Constants.CREATED_DATETIME).gte(startDate).lte(query.getEndDate()));
					if(query.getChannel_slno()!=null){
						qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
					}
					qbFilter.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, (String)source.get(Constants.STORY_ID_FIELD)));
					BoolQueryBuilder qbuilder = new BoolQueryBuilder();
					qbuilder.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, (String)source.get(Constants.TITLE )));							

					multiSearchRequestBuilder.add(client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
							.setSize(2000).setQuery(qbuilder).setFetchSource(false));
				}				
				records.add(story);				
			}			

			MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();

			ArrayList<Integer> storyCounter = new ArrayList<>();			
			for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
				int similarStoryCounter = 0;
				SearchResponse response = item.getResponse();
				if (response.getHits().getTotalHits() > 0) {
					for (SearchHit hit : response.getHits().getHits()) {
						if (hit.getScore() > query.getScore()) {
							similarStoryCounter++;
						}
					}
				}
				storyCounter.add(similarStoryCounter);
			}						
			for (int i = 0; i < records.size(); i++) {
				records.get(i).setStories_count(storyCounter.get(i));
			}

			// to calculate enagagement and share performance	
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(DateUtil.addHoursToCurrentTime(-3)).lte(DateUtil.getCurrentDateTime()));
			SearchResponse res1 = client.prepareSearch(Indexes.FB_COMPETITOR_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms(Constants.COMPETITOR).field(Constants.COMPETITOR).size(1000)
							.subAggregation(AggregationBuilders.sum(Constants.TOTAL_ENGAGEMENT).field(Constants.TOTAL_ENGAGEMENT))
							.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
							.subAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
							.subAggregation(AggregationBuilders.filter("stories", QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, stories))
									.subAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
											.subAggregation(AggregationBuilders.sum(Constants.TOTAL_ENGAGEMENT).field(Constants.TOTAL_ENGAGEMENT))
											.subAggregation(AggregationBuilders.sum(Constants.SHARES).field(Constants.SHARES))
											.size(query.getCount()))))
					.setSize(0).execute().actionGet();			

			Map<String, Map<String, Double>> storyPerformanceMap = new HashMap<>();
			Terms competitorBuckets = res1.getAggregations().get(Constants.COMPETITOR);
			for (Terms.Bucket competitor : competitorBuckets.getBuckets()) {
				Sum engagement_agg = competitor.getAggregations().get(Constants.TOTAL_ENGAGEMENT);
				long competitorEngagement = ((Double)engagement_agg.getValue()).longValue();
				Sum shares_agg = competitor.getAggregations().get(Constants.SHARES);
				long competitorShares = ((Double)shares_agg.getValue()).longValue();
				Cardinality story_agg = competitor.getAggregations().get("storyCount");
				long story_count = story_agg.getValue();
				long avgCompEng = 0;
				long avgCompShares = 0;
				if(story_count!=0){
					avgCompEng = competitorEngagement/story_count;
					avgCompShares = competitorShares/story_count;
				}
				Filter filter = competitor.getAggregations().get("stories");
				Terms storyBuckets = filter.getAggregations().get("storyid");
				for (Terms.Bucket story : storyBuckets.getBuckets()) {
					Map<String, Double> storyMap = new HashMap<>();
					Sum story_engagement_agg = story.getAggregations().get(Constants.TOTAL_ENGAGEMENT);
					double storyEngagement = story_engagement_agg.getValue();
					Sum story_shares_agg = story.getAggregations().get(Constants.SHARES);
					double storyShares = story_shares_agg.getValue();
					if(avgCompEng!=0)
					{
						storyMap.put("engPerf", storyEngagement/avgCompEng);
					}
					else
					{
						storyMap.put("engPerf",0.0);
					}
					if(avgCompShares!=0){
						storyMap.put("sharesPerf", storyShares/avgCompShares);
					}
					else
					{
						storyMap.put("sharesPerf",0.0);
					}
					storyPerformanceMap.put(story.getKeyAsString(), storyMap);
				}
			}

			for(CompetitorStory story:records){
				Map<String, Double> storyMap = storyPerformanceMap.get(story.getStoryid());
				if(storyMap!=null){
					story.setEngagementPerformance(storyMap.get("engPerf"));
					story.setSharesPerformance(storyMap.get("sharesPerf"));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFbCompetitorsStories.", e);
		}
		log.info("Retrieving getFbCompetitorsStories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	public List<FacebookInsights> getFbVelocity(WisdomQuery query){
		long startTime = System.currentTimeMillis();
		List<FacebookInsights> records = new ArrayList<>();
		Map<String,FacebookInsights> storyMap = new HashMap<>();
		String parameter = query.getParameter();		
		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();
			int interval = (-1)*(Integer.parseInt(query.getInterval()));
			qb.must(QueryBuilders.rangeQuery(query.getDateField()).gte(DateUtil.addHoursToCurrentTime(interval)).lte(DateUtil.getCurrentDateTime()));
			if(query.getChannel_slno()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getStatus_type()!=null){
				qb.must(QueryBuilders.termQuery(Constants.STATUS_TYPE, query.getStatus_type()));
			}

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.FB_INSIGHTS_HISTORY, DateUtil.addHoursToCurrentTime(interval), DateUtil.getCurrentDateTime())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.size(query.getCount()*3)
							.subAggregation(AggregationBuilders.max(parameter).field(parameter))
							.order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();			

			List<String> stories = new ArrayList<>();
			Terms resTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket resBuckets : resTermAgg.getBuckets()) {				
				FacebookInsights story = new FacebookInsights();
				TopHits topHits = resBuckets.getAggregations().get("top");	
				SearchHit[] searchHits= topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					story = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
					Map<String, Object> source = hit.getSource();
					String storyid = (String)source.get(Constants.STORY_ID_FIELD);
					stories.add(storyid);
					storyMap.put(storyid, story);
				}
			}
			BoolQueryBuilder qbForPrevData = new BoolQueryBuilder();

			qbForPrevData.must(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, stories));
			qbForPrevData.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lt(DateUtil.addHoursToCurrentTime(-25)));

			SearchResponse prevRes = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.FB_INSIGHTS_HISTORY, DateUtil.addHoursToCurrentTime(interval), DateUtil.getCurrentDateTime())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qbForPrevData)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("prevTop").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))							
							.size(query.getCount()*3))							

					.setSize(0).execute().actionGet();			

			Terms prevResTermAgg = prevRes.getAggregations().get("storyid"); 			
			for(Terms.Bucket prevResBuckets:prevResTermAgg.getBuckets()){

				TopHits topHit = prevResBuckets.getAggregations().get("prevTop");
				SearchHit[] prevSearchHits = topHit.getHits().getHits();				
				for (SearchHit prevHit : prevSearchHits) {	

					Map<String, Object> prevSourceMap = prevHit.getSource();
					String storyid = (String)prevSourceMap.get(Constants.STORY_ID_FIELD);

					FacebookInsights story = storyMap.get(storyid);
					Integer prevTotalReach =(Integer) prevSourceMap.get(Constants.TOTAL_REACH);
					Long prevTotalReachL = new Long(prevTotalReach);					
					story.setPrev_total_reach(prevTotalReachL);					
					Long totalReach = story.getTotal_reach();					
					if(totalReach !=null && prevTotalReachL !=null) {	
						story.setTotal_reach_difference(totalReach - prevTotalReachL);					 					
					}
					Integer current_shares = story.getShares();
					Integer prev_shares =  (Integer) prevSourceMap.get(Constants.SHARES);

					if(current_shares !=null && prev_shares !=null) {	
						story.setTotal_shares_difference(current_shares - prev_shares);					 					
					}

					// current engagement 					
					Integer current_reaction_wow = story.getReaction_wow();
					Integer current_reaction_thankful= story.getReaction_thankful();
					Integer current_reaction_haha= story.getReaction_haha();
					Integer current_reaction_love= story.getReaction_love();
					Integer current_reaction_sad= story.getReaction_sad();
					Integer current_reaction_angry= story.getReaction_angry();
					Integer current_likes= story.getLikes();
					Integer current_comments= story.getComments();					
					Integer total_curr_engagement = (current_reaction_wow + current_reaction_thankful + current_reaction_haha 
							+current_reaction_love + current_reaction_sad + current_reaction_angry
							+ current_likes + current_comments + current_shares);

					// previous engagement 	
					Integer prv_shares = (Integer) prevSourceMap.get(Constants.SHARES);
					Integer prev_reaction_wow =(Integer) prevSourceMap.get(Constants.REACTION_WOW);
					Integer prev_reaction_thankful =(Integer) prevSourceMap.get(Constants.REACTION_THANKFUL);
					Integer prev_reaction_haha =(Integer) prevSourceMap.get(Constants.REACTION_HAHA);
					Integer prev_reaction_love =(Integer) prevSourceMap.get(Constants.REACTION_LOVE);
					Integer prev_reaction_sad =(Integer) prevSourceMap.get(Constants.REACTION_SAD);
					Integer prev_reaction_angry =(Integer) prevSourceMap.get(Constants.REACTION_ANGRY);
					Integer prev_likes =(Integer) prevSourceMap.get(Constants.LIKES);
					Integer prev_comments =(Integer) prevSourceMap.get(Constants.COMMENTS);
					Integer total_prev_engagement = (prev_reaction_wow + prev_reaction_thankful + prev_reaction_haha + prev_reaction_love + prev_reaction_sad 
							+prev_reaction_angry + prev_likes + prev_comments + prev_shares);

					if(total_curr_engagement !=null && total_prev_engagement !=null) {	
						story.setTotal_engagement_difference(total_curr_engagement - total_prev_engagement);					 					
					}

				}	
			}	

			// to calculate probabilities of future impressions	
			String indexname = null;
			if(StringUtils.contains(Constants.SHARES, query.getPrediction())) {
				indexname = Indexes.SHARE_PREDICTION;
			}
			else if(StringUtils.contains(Constants.ENGAGEMENT, query.getPrediction())) {
				indexname = Indexes.ENGAGEMENT_PREDICTION;
			}
			else{
				indexname = Indexes.PROB_PREDICTION;
			}

			SearchResponse res1 = client.prepareSearch(indexname).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, stories))
					.setSize(0).addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC)
					.addAggregation(AggregationBuilders.terms("topStories").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("topHits").size(1)
									.sort(Constants.DATE_TIME_FIELD,SortOrder.DESC))
							.subAggregation(AggregationBuilders.filter("prevTopRange", QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(DateUtil.addHoursToCurrentTime(-24)))
									.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC)))
							.size(query.getCount()))
					.execute().actionGet();


			Terms hit=res1.getAggregations().get("topStories");		
			for(Terms.Bucket storiesBucket:hit.getBuckets()){
				String storyid = storiesBucket.getKeyAsString();
				FacebookInsights story = storyMap.get(storyid);
				TopHits topHits = storiesBucket.getAggregations().get("topHits");
				for(SearchHit hits:topHits.getHits().getHits()){	
					Map<String, Object> source =hits.getSource();					
					source.remove(Constants.STORY_ID_FIELD);
					story.setTop_prob((Double)source.remove(Constants.TOP_PROB));
					story.setTop_range((String)source.remove(Constants.TOP_RANGE));
					story.setProbMap(source);
				}
				Filter prevTopRangeFilter = storiesBucket.getAggregations().get("prevTopRange");						
				TopHits topRangeHits = prevTopRangeFilter.getAggregations().get("top");				
				for (SearchHit prevHit : topRangeHits.getHits().getHits()) {						
					Map<String, Object> prevTopRangeSourceMap = prevHit.getSource();						
					String prevTopRange = (String) prevTopRangeSourceMap.get(Constants.TOP_RANGE);			
					story.setPrev_top_range(prevTopRange);
				}
				records.add(story);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getFbVelocity.", e);
		}
		log.info("Retrieving getFbVelocity; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	public Map<String,List<Map<String,Object>>> get0HourFbVelocityGraph(WisdomQuery query){
		long startTime = System.currentTimeMillis();
		Map<String,List<Map<String,Object>>> records = new HashMap<>();
		List<String> storyids = Arrays.asList(query.getStoryid().split(","));
		try {	

			BoolQueryBuilder bqb= QueryBuilders.boolQuery()
					.must(QueryBuilders.termsQuery(Constants.BHASKARSTORYID, storyids))
					.mustNot(QueryBuilders.rangeQuery(Constants.HOUR).gte(0).lte(6));

			if(query.getInterval()!=null){
				int day = DateUtil.getWeekDay(DateUtil.getCurrentDateTime());
				String days = "";
				if(day==1){
					days = "Sun,Mon";
				}
				else if(day==2){
					days = "Mon,Tue";
				}
				else if(day==3){
					days = "Tue,Wed";
				}
				else if(day==4){
					days = "Wed,Thu";
				}
				else if(day==5){
					days = "Thu,Fri";
				}
				else if(day==6){
					days = "Fri,Sat";
				}
				else if(day==7){
					days = "Sat,Sun";
				}
				bqb.must(QueryBuilders.termsQuery(Constants.DAY,days.split(",")));
			}

			SearchResponse res1 = client.prepareSearch(Indexes.ZERO_HOUR_PROB_PREDICTION).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(bqb)
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.BHASKARSTORYID).size(storyids.size())
							.subAggregation(AggregationBuilders.topHits("topHits").size(200)
									.sort(Constants.DAY_HOUR,SortOrder.ASC)))
					.setSize(0).execute().actionGet();			
			Terms stories = res1.getAggregations().get("stories");
			for(Terms.Bucket story:stories.getBuckets()){
				List<Map<String, Object>> storyDetails = new ArrayList<>();
				TopHits topHits = story.getAggregations().get("topHits");
				for(SearchHit hit:topHits.getHits().getHits()){				
					storyDetails.add(hit.getSource());
				}
				records.put(story.getKeyAsString(), storyDetails);
			}		

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving get0HourFbVelocityGraph.", e);
		}
		log.info("Retrieving get0HourFbVelocityGraph; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<StoryDetail> get0HourFbVelocity(WisdomQuery query){
		long startTime = System.currentTimeMillis();
		List<StoryDetail> records = new ArrayList<>();
		Map<String,StoryDetail> storyMap = new HashMap<>();
		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();
			int interval = (-1)*(Integer.parseInt(query.getInterval()));
			qb.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(DateUtil.addHoursToCurrentTime(interval)).lte(DateUtil.getCurrentDateTime()));
			//qb.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte("2017-10-29T12:00:00Z").lte("2017-10-29T17:00:00Z"));

			if(query.getChannel_slno()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}

			SearchResponse res = client.prepareSearch(Indexes.STORY_UNIQUE_DETAIL).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).addSort(Constants.STORY_PUBLISH_TIME, SortOrder.DESC)
					.setSize(query.getCount()*5).execute().actionGet();

			List<String> stories = new ArrayList<>();
			for (SearchHit hit : res.getHits().getHits()) {
				StoryDetail story = new StoryDetail();				
				story = gson.fromJson(hit.getSourceAsString(), StoryDetail.class);
				Map<String, Object> source = hit.getSource();
				String storyid = (String)source.get(Constants.STORY_ID_FIELD);
				stories.add(storyid);
				storyMap.put(storyid, story);
			}			

			int day = DateUtil.getWeekDay(DateUtil.getCurrentDateTime());
			String days = "";
			if(day==1){
				days = "Sun,Mon";
			}
			else if(day==2){
				days = "Mon,Tue";
			}
			else if(day==3){
				days = "Tue,Wed";
			}
			else if(day==4){
				days = "Wed,Thu";
			}
			else if(day==5){
				days = "Thu,Fri";
			}
			else if(day==6){
				days = "Fri,Sat";
			}
			else if(day==7){
				days = "Sat,Sun";
			}

			BoolQueryBuilder bqb= QueryBuilders.boolQuery()
					.must(QueryBuilders.termsQuery(Constants.BHASKARSTORYID, stories))
					.mustNot(QueryBuilders.rangeQuery(Constants.HOUR).gte(0).lte(6));

			String day_hour = "";
			if(DateUtil.getHour(DateUtil.getCurrentDateTime())<10){
				day_hour= (DateUtil.getWeekDay(DateUtil.getCurrentDateTime())-1)+"0"+DateUtil.getHour(DateUtil.getCurrentDateTime()); 
			}
			else{
				day_hour= (DateUtil.getWeekDay(DateUtil.getCurrentDateTime())-1)+""+DateUtil.getHour(DateUtil.getCurrentDateTime());
			}

			int dayHour = Integer.parseInt(day_hour);

			BoolQueryBuilder dayRangeQb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(Constants.DAY_HOUR).gte(dayHour));
			if(day == 7){
				dayRangeQb = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
						.should(QueryBuilders.rangeQuery(Constants.DAY_HOUR).gte(dayHour))
						.should(QueryBuilders.rangeQuery(Constants.DAY_HOUR).lte(24)));
			}

			SearchResponse res1 = client.prepareSearch(Indexes.ZERO_HOUR_PROB_PREDICTION).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(bqb)
					.addAggregation(AggregationBuilders.terms("story").field(Constants.BHASKARSTORYID)
							.subAggregation(AggregationBuilders.terms("maxRange").field(Constants.MAX_REACH_CLASS).size(1).order(Order.term(false))
									.subAggregation(AggregationBuilders.terms("maxProb").field(Constants.MAX_REACH_PROB).size(1).order(Order.term(false))
											.subAggregation(AggregationBuilders.filter("after", QueryBuilders.rangeQuery(Constants.DAY_HOUR).gte(dayHour))
													.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DAY_HOUR, SortOrder.ASC)))
											.subAggregation(AggregationBuilders.filter("before", QueryBuilders.rangeQuery(Constants.DAY_HOUR).lt(dayHour))
													.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DAY_HOUR, SortOrder.ASC)))))
							.subAggregation(AggregationBuilders.filter("next24hours",QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(Constants.DAY,days.split(",")))
									.must(dayRangeQb))
									.subAggregation(AggregationBuilders.terms("maxRange").field(Constants.MAX_REACH_CLASS).size(1).order(Order.term(false))
											.subAggregation(AggregationBuilders.terms("maxProb").field(Constants.MAX_REACH_PROB).size(1).order(Order.term(false))
													.subAggregation(AggregationBuilders.filter("after", QueryBuilders.rangeQuery(Constants.DAY_HOUR).gte(dayHour))
															.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DAY_HOUR, SortOrder.ASC)))
													.subAggregation(AggregationBuilders.filter("before", QueryBuilders.rangeQuery(Constants.DAY_HOUR).lt(dayHour))
															.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DAY_HOUR, SortOrder.ASC))))))
							.size(query.getCount()))					
					.setSize(0).execute().actionGet();
			Terms storyTerms = res1.getAggregations().get("story");			
			for(Terms.Bucket storyBucket:storyTerms.getBuckets()){
				StoryDetail story = new StoryDetail();
				Terms rangeTerms = storyBucket.getAggregations().get("maxRange");
				for(Terms.Bucket maxRange:rangeTerms.getBuckets()){
					Terms ProbTerms = maxRange.getAggregations().get("maxProb");
					for(Terms.Bucket maxProb:ProbTerms.getBuckets()){
						TopHits topHit;
						Filter afterFilter = maxProb.getAggregations().get("after");
						if(afterFilter.getDocCount()>0){
							topHit = afterFilter.getAggregations().get("top");
						}
						else{
							Filter beforeFilter = maxProb.getAggregations().get("before");
							topHit = beforeFilter.getAggregations().get("top");							
						}
						Map<String, Object> source = topHit.getHits().getHits()[0].getSource();
						story = storyMap.get(source.get(Constants.BHASKARSTORYID).toString());
						story.setBestDay(source.get(Constants.DAY).toString());
						story.setBestHour(source.get(Constants.HOUR).toString());
						story.setMaxRange(source.get(Constants.MAX_REACH_CLASS).toString());
						story.setMaxProb(source.get(Constants.MAX_REACH_PROB).toString());
					}
				}

				Filter filter = storyBucket.getAggregations().get("next24hours");	
				Terms filteredRangeTerms = filter.getAggregations().get("maxRange");
				for(Terms.Bucket maxRange:filteredRangeTerms.getBuckets()){
					Terms ProbTerms = maxRange.getAggregations().get("maxProb");
					for(Terms.Bucket maxProb:ProbTerms.getBuckets()){
						TopHits topHit;
						Filter afterFilter = maxProb.getAggregations().get("after");
						if(afterFilter.getDocCount()>0){
							topHit = afterFilter.getAggregations().get("top");
						}
						else{
							Filter beforeFilter = maxProb.getAggregations().get("before");
							topHit = beforeFilter.getAggregations().get("top");							
						}
						Map<String, Object> source = topHit.getHits().getHits()[0].getSource();
						story.setBestNearestDay(source.get(Constants.DAY).toString());
						story.setBestNearestHour(source.get(Constants.HOUR).toString());
						story.setBestNearestHourrange(source.get(Constants.MAX_REACH_CLASS).toString());
						story.setBestNearestHourProb(source.get(Constants.MAX_REACH_PROB).toString());
					}
				}
				records.add(story);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving get0HourFbVelocity.", e);
		}
		log.info("Retrieving get0HourFbVelocity; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;

	}

	/*public List<CompetitorStory> getSimilarStories(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<CompetitorStory> records = new ArrayList<>();
		String parameter = query.getParameter();		
		try {	

			BoolQueryBuilder qbFilter = new BoolQueryBuilder();

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));

			BoolQueryBuilder qb = new BoolQueryBuilder();

			qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, query.getTitle()));

			SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.topHits("top").size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.size(query.getCount())
							.subAggregation(AggregationBuilders.max(parameter).field(parameter))
							.order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();

			List<String> stories = new ArrayList<>();
			Terms resTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket resBuckets : resTermAgg.getBuckets()) {				
				CompetitorStory story = new CompetitorStory();
				TopHits topHits = resBuckets.getAggregations().get("top");	
				SearchHit[] searchHits= topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					story = gson.fromJson(hit.getSourceAsString(), CompetitorStory.class);
				}
				records.add(story);
			}			

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSimilarStories.", e);
		}
		log.info("Retrieving getSimilarStories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}*/

	public List<CompetitorStory> getSimilarStories(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<CompetitorStory> records = new ArrayList<>();
		try {	

			BoolQueryBuilder qbFilter = new BoolQueryBuilder();

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			if(query.getStoryid()!=null)
			{
				qbFilter.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
			}
			BoolQueryBuilder qb = new BoolQueryBuilder();
			//qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, query.getTitle()).fuzziness(query.getFuzziness()));
			qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, query.getTitle()));
			SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
					.highlighter(new HighlightBuilder().field(Constants.TITLE))
					.setQuery(qb)
					.setSize(query.getCount()).execute().actionGet();
			for (SearchHit hit : res.getHits().getHits()) {
				CompetitorStory story = new CompetitorStory();

				story =	gson.fromJson(hit.getSourceAsString(), CompetitorStory.class);
				story.setScore((double) hit.getScore());				
				if (hit.getHighlightFields().get(Constants.TITLE) != null) {
					String titleText = "";
					Text[] texts = hit.getHighlightFields().get(Constants.TITLE).getFragments();

					for(Text text:texts){
					titleText += text.toString();
					}
					story.setTitle(titleText);
					//story.setTitle(hit.getHighlightFields().get(Constants.TITLE).getFragments()[0].toString());
				}
				if(story.getScore() > query.getScore()){
					records.add(story);
				}						
			}						

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSimilarStories.", e);
		}
		log.info("Retrieving getSimilarStories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<Comment> getComments(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<Comment> records = new ArrayList<>();
		try {	

			BoolQueryBuilder qb = new BoolQueryBuilder();

			qb.must(QueryBuilders.termQuery(Constants.IS_REPLY, query.isReply()));
			if(query.isReply()){
				qb.must(QueryBuilders.termQuery(Constants.IN_REPLY_TO_COMMENT_ID, query.getStoryid()));
			}
			else {
				qb.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
			}
			if(query.getHost()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
			}			
			SearchResponse res = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.setSize(query.getCount()).addSort(Constants.DATE_TIME_FIELD, SortOrder.DESC).execute().actionGet();

			for (SearchHit hit : res.getHits().getHits()) {
				Comment comment = new Comment();
				comment =	gson.fromJson(hit.getSourceAsString(), Comment.class);
				records.add(comment);
			}						

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getComments.", e);
		}
		log.info("Retrieving getComments; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String, Object> getStoriesForCommentDashboard(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		Map<String, Object> records = new HashMap<>();
		try {
			Map<String, Long> storyMap = new LinkedHashMap<>();

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(query.getEndDate()).gte(query.getStartDate()));
			if(query.getChannel_slno()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getHost()!=null){
				qb.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
			}

			SearchResponse res = client.prepareSearch(Indexes.DB_COMMENT).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
					.addAggregation(AggregationBuilders.terms("stories").field(Constants.STORY_ID_FIELD).size(query.getCount()))
					.setSize(0).execute().actionGet();

			long storyCount = 0;
			long commentCount = res.getHits().getTotalHits();
			Cardinality storyCountAgg = res.getAggregations().get("storyCount");
			storyCount = storyCountAgg.getValue();
			Terms storyAgg = res.getAggregations().get("stories");
			for(Terms.Bucket storyBucket:storyAgg.getBuckets()){
				storyMap.put(storyBucket.getKeyAsString(), storyBucket.getDocCount());
			}

			MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
					.add(Indexes.STORY_UNIQUE_DETAIL, MappingTypes.MAPPING_REALTIME, storyMap.keySet()) 
					.get();

			List<StoryDetail> storyList = new ArrayList<>();

			for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
				GetResponse response = itemResponse.getResponse();
				if (response.isExists()) {  
					StoryDetail story = new StoryDetail();						
					story =	gson.fromJson(response.getSourceAsString(), StoryDetail.class);
					story.setCommentCount(storyMap.get(story.getStoryid()));
					storyList.add(story);
				}
			}	

			records.put("stories", storyList);
			records.put("storyCount", storyCount);
			records.put("commentCount", commentCount);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getStoriesForCommentDashboard.", e);
		}
		log.info("Retrieving getStoriesForCommentDashboard; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String,Double> getVideosCtr(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		Map<String,Double> records = new HashMap<>();
		try {
			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(query.getEndDate()).gte(query.getStartDate()));
			qb.must(QueryBuilders.termsQuery(Constants.VIDEO_ID, query.getStoryid().split(",")));

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.VIDEO_IDENTIFICATION_DETAIL,query.getStartDate(),query.getEndDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("videos").field(Constants.VIDEO_ID).size(query.getStoryid().split(",").length)
							.subAggregation(AggregationBuilders.sum(Constants.IMPRESSIONS).field(Constants.IMPRESSIONS))
							.subAggregation(AggregationBuilders.sum(Constants.CLICKS).field(Constants.CLICKS)))
					.setSize(0).execute().actionGet();

			Terms videoAgg = res.getAggregations().get("videos");
			for(Terms.Bucket videoBucket:videoAgg.getBuckets()){

				Sum impAgg = videoBucket.getAggregations().get(Constants.IMPRESSIONS);
				Double impressions = impAgg.getValue();

				Sum clicksAgg = videoBucket.getAggregations().get(Constants.CLICKS);
				Double clicks = clicksAgg.getValue();

				Double ctr = 0.0;
				if(impressions!=0.0){
					ctr = ((clicks*100)/impressions);
				}

				records.put(videoBucket.getKeyAsString(), ctr);				
			}									

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getVideosCtr.", e);
		}
		log.info("Retrieving getVideosCtr; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public Map<String, Object> getWidgetWiseVideosCtr(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		Map<String,Object> finalMap = new LinkedHashMap<>();		
		try {

			String interval = query.getInterval();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);			

			BoolQueryBuilder qb = new BoolQueryBuilder();
			qb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).lte(query.getEndDate()).gte(query.getStartDate()));
			qb.must(QueryBuilders.termsQuery(Constants.CHANNEL_ID, query.getChannel_slno()));

			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.VIDEO_IDENTIFICATION_DETAIL,query.getStartDate(),query.getEndDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
							.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(Histogram.Order.KEY_DESC)
							.subAggregation(AggregationBuilders.terms(Constants.SEC_TRACK).field(Constants.SEC_TRACK).size(50)							
									.subAggregation(AggregationBuilders.terms(Constants.WIDGET_REC_POS).field(Constants.WIDGET_REC_POS).order(Order.term(true)).size(50)
											.subAggregation(AggregationBuilders.sum(Constants.IMPRESSIONS).field(Constants.IMPRESSIONS))
											.subAggregation(AggregationBuilders.sum(Constants.CLICKS).field(Constants.CLICKS)))))					
					.setSize(0).execute().actionGet();

			Histogram histogramAgg = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogramAgg.getBuckets()) {
				Map<String,Map<Integer,Double>> records = new HashMap<>();
				if(bucket.getDocCount()>0){					
					Terms widgetAgg = bucket.getAggregations().get(Constants.SEC_TRACK);
					for(Terms.Bucket widgetBucket:widgetAgg.getBuckets()){

						Map<Integer, Double> posMap = new TreeMap<>();
						Terms posAgg = widgetBucket.getAggregations().get(Constants.WIDGET_REC_POS);
						for(Terms.Bucket posBucket:posAgg.getBuckets()){

							Sum impAgg = posBucket.getAggregations().get(Constants.IMPRESSIONS);
							Double impressions = impAgg.getValue();

							Sum clicksAgg = posBucket.getAggregations().get(Constants.CLICKS);
							Double clicks = clicksAgg.getValue();

							Double ctr = 0.0;
							if(impressions!=0.0){
								ctr = clicks/impressions;
							}
							posMap.put(Integer.valueOf(posBucket.getKeyAsString()), ctr);
						}
						records.put(widgetBucket.getKeyAsString(), posMap);
					}
				}
				finalMap.put(bucket.getKeyAsString(), records);				
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getWidgetWiseVideosCtr.", e);
		}
		log.info("Retrieving getWidgetWiseVideosCtr; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return finalMap;
	}


	public List<KRAreport> getKraReport(WisdomQuery query) {
		ArrayList<KRAreport> kraReportList = new ArrayList<>();
		long startTime = System.currentTimeMillis();
		try{
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			if (query.getHost_type()!=null) {
				boolQuery.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
			}			

			if (query.getChannel_slno() !=null) {
				boolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}

			String field = Constants.SUPER_CAT_NAME;

			if (query.getType()==0) {	

				SearchResponse res = client
						.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate()))
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery)
						.addAggregation(AggregationBuilders.filter("dateRange", QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate()).lte(query.getEndDate()))
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
								.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS))
								.subAggregation(AggregationBuilders.cardinality("head_count").field(Constants.UID))
								.subAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
								.subAggregation(AggregationBuilders.filter("lastDay", 
										QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getEndDate()).lte(query.getEndDate()))
										.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))))
						.addAggregation(AggregationBuilders.filter("mtd", QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(DateUtil.getFirstDateOfMonth()).lte(DateUtil.getCurrentDate().replaceAll("_", "-")))
								.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)))
						.setSize(0).execute().actionGet();	

				KRAreport kra = new KRAreport();			

				Filter dateRange  = res.getAggregations().get("dateRange");			

				Sum totalpvs = dateRange.getAggregations().get("pvs");
				kra.setPvs((long) totalpvs.getValue());

				Sum totaUpvs = dateRange.getAggregations().get("uvs");
				kra.setUpvs((long) totaUpvs.getValue());

				Cardinality headCount = dateRange.getAggregations().get("head_count");
				kra.setHead_count((long) headCount.getValue());

				Cardinality storyCount = dateRange.getAggregations().get("storyCount");
				kra.setStoryCount((long) storyCount.getValue());

				Map<String, Object> sharbilityMap = getOverAllSharability(query);
				kra.setSharability((Double)sharbilityMap.get(Constants.SHAREABILITY));
				kra.setShares((Long)sharbilityMap.get(Constants.SHARES));

				// ## session growth start 
				Long current_sessions = getSessionsCount(query);

				String previousMonthStartDate = DateUtil.getDateOfPreviousMonth(query.getStartDate(), "yyyy-MM-dd");				
				String previousMonthEndDate = DateUtil.getDateOfPreviousMonth(query.getEndDate(), "yyyy-MM-dd");			

				query.setStartDate(query.getEndDate());
				kra.setLast_day(getSessionsCount(query));

				query.setStartDate(previousMonthStartDate);

				query.setEndDate(previousMonthEndDate);				

				Double previous_sessions = getSessionsCount(query).doubleValue();
				//Double previous_sessions = 100.0;				
				Double sessionsGrowth = 0.0; 

				if(previous_sessions != 0) {
					sessionsGrowth =  ((current_sessions - previous_sessions)/previous_sessions)*100;
				}
				kra.setSessions_growth(sessionsGrowth);
				/* session growth end */

				kra.setSessions(current_sessions);

				query.setStartDate(query.getEndDate());

				kra.setLast_day(getSessionsCount(query));

				query.setStartDate(DateUtil.getFirstDateOfMonth());
				query.setEndDate(DateUtil.getCurrentDate().replaceAll("_", "-"));

				Long mtdSessions = getSessionsCount(query);
				kra.setMtd(mtdSessions);

				Long projection = (mtdSessions/DateUtil.getCurrentDayOfMonth())*DateUtil.getNumberOfDaysInCurrentMonth();
				kra.setProjection(projection);
				//## session growth end 

				kraReportList.add(kra);		

			}

			// # If type is 1 
			if (query.getType()==1) {

				if (query.getSuper_cat_name() !=null) {
					boolQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_NAME, query.getSuper_cat_name()));
					field = Constants.UID;
				}
				if (query.getUid() !=null) {
					boolQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
				}
				SearchResponse res = client
						.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, query.getStartDate(), query.getEndDate()))
						.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery)
						.addAggregation(AggregationBuilders.terms(field).field(field).size(50)
								.subAggregation(AggregationBuilders.topHits("top").sort(Constants.DATE_TIME_FIELD, SortOrder.DESC).size(1))
								.subAggregation(AggregationBuilders.filter("dateRange", QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getStartDate()).lte(query.getEndDate()))
										.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
										.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS))
										.subAggregation(AggregationBuilders.cardinality("head_count").field(Constants.UID))
										.subAggregation(AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
										.subAggregation(AggregationBuilders.filter("lastDay", 
												QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(query.getEndDate()).lte(query.getEndDate()))
												.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))))
								.subAggregation(AggregationBuilders.filter("mtd", QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).gte(DateUtil.getFirstDateOfMonth()).lte(DateUtil.getCurrentDate().replaceAll("_", "-")))
										.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))))
						.setSize(0).execute().actionGet();	

				Terms kraAgg = res.getAggregations().get(field);		
				for (Terms.Bucket kraBucket : kraAgg.getBuckets()) {
					KRAreport kra = new KRAreport();			

					Filter dateRange  = kraBucket.getAggregations().get("dateRange");			

					Sum totalpvs = dateRange.getAggregations().get("pvs");
					kra.setPvs((long) totalpvs.getValue());

					Sum totaUpvs = dateRange.getAggregations().get("uvs");
					kra.setUpvs((long) totaUpvs.getValue());

					Cardinality headCount = dateRange.getAggregations().get("head_count");
					kra.setHead_count((long) headCount.getValue());

					Cardinality storyCount = dateRange.getAggregations().get("storyCount");
					kra.setStoryCount((long) storyCount.getValue());


					/*
					Filter lastDay  = dateRange.getAggregations().get("lastDay");
					Sum lastDayPvs = lastDay.getAggregations().get("pvs");
					kra.setLast_day((int) lastDayPvs.getValue()).longValue());

					Filter mtd = kraBucket.getAggregations().get("mtd");
					Sum mtdCount = mtd.getAggregations().get("pvs");
					Long mtdPvs = (int) mtdCount.getValue()).longValue();
					kra.setMtd(mtdPvs);

					Long projection = (mtdPvs/DateUtil.getCurrentDayOfMonth())*DateUtil.getNumberOfDaysInCurrentMonth();
					kra.setProjection(projection);*/

					TopHits topHit = kraBucket.getAggregations().get("top");
					Map<String,Object> source = new HashMap<>();
					if(topHit.getHits().getHits().length>0){
						source = topHit.getHits().getHits()[0].getSource();
					}

					if(source!=null){
						if(field.equals(Constants.SUPER_CAT_NAME))
						{
							List<String> superCatNameList = new ArrayList<>();
							int superCatId = 0;
							String superCatName = "";
							if(source.get(Constants.SUPER_CAT_NAME) instanceof Integer) {
								superCatName = String.valueOf((Integer) source.get(Constants.SUPER_CAT_NAME)); 
							}
							else {
								superCatName =(String)source.get(Constants.SUPER_CAT_NAME);
							}

							if(source.get(Constants.SUPER_CAT_ID) instanceof Integer) {
								superCatId = (Integer)source.get(Constants.SUPER_CAT_ID);
							}
							else if(source.get(Constants.SUPER_CAT_ID) instanceof String&&StringUtils.isNotBlank((String) source.get(Constants.SUPER_CAT_ID))){
								superCatId = Integer.valueOf((String) source.get(Constants.SUPER_CAT_ID));
							}
							kra.setSuper_cat_id(superCatId);
							superCatNameList.add(superCatName);
							query.setSuper_cat_name(superCatNameList);
							Map<String, Object> sharbilityMap = getOverAllSharability(query);
							kra.setSharability((Double)sharbilityMap.get(Constants.SHAREABILITY));
							kra.setShares((Long)sharbilityMap.get(Constants.SHARES));
							query.setSession_type(SessionTypeConstants.SUPER_CAT_NAME);
						}
						else if(field.equals(Constants.UID))
						{
							List<String> author_id = new ArrayList<>();
							int authorId = (Integer)source.get(Constants.UID);
							author_id.add(String.valueOf(authorId));
							query.setUid(author_id);
							Map<String, Object> sharbilityMap = getOverAllSharability(query);
							kra.setSharability((Double)sharbilityMap.get(Constants.SHAREABILITY));
							kra.setShares((Long)sharbilityMap.get(Constants.SHARES));
							kra.setAuthor_id(authorId);
							kra.setAuthor_name((String)source.get(Constants.AUTHOR_NAME));
							query.setSession_type(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
						}

						kra.setSessions(getSessionsCount(query));
						/*
						query.setStartDate(query.getEndDate());

						kra.setLast_day(getSessionsCount(query));

						query.setStartDate(DateUtil.getFirstDateOfMonth());
						query.setEndDate(DateUtil.getCurrentDate().replaceAll("_", "-"));

						Long mtdSessions = getSessionsCount(query);
						kra.setMtd(mtdSessions);

						Long projection = (mtdSessions/DateUtil.getCurrentDayOfMonth())*DateUtil.getNumberOfDaysInCurrentMonth();
						kra.setProjection(projection);*/

						// ## session growth start 
						Long current_sessions = getSessionsCount(query);

						String previousMonthStartDate = DateUtil.getDateOfPreviousMonth(query.getStartDate(), "yyyy-MM-dd");				
						String previousMonthEndDate = DateUtil.getDateOfPreviousMonth(query.getEndDate(), "yyyy-MM-dd");			

						query.setStartDate(query.getEndDate());
						kra.setLast_day(getSessionsCount(query));

						query.setStartDate(previousMonthStartDate);

						query.setEndDate(previousMonthEndDate);				

						Double previous_sessions = getSessionsCount(query).doubleValue();
						//Double previous_sessions = 100.0;				
						Double sessionsGrowth = 0.0; 

						if(previous_sessions != 0) {
							sessionsGrowth =  ((current_sessions - previous_sessions)/previous_sessions)*100;
						}
						kra.setSessions_growth(sessionsGrowth);

						/*## session growth end */

						kra.setSessions(current_sessions);

						query.setStartDate(query.getEndDate());

						kra.setLast_day(getSessionsCount(query));

						query.setStartDate(DateUtil.getFirstDateOfMonth());
						query.setEndDate(DateUtil.getCurrentDate().replaceAll("_", "-"));

						Long mtdSessions = getSessionsCount(query);
						kra.setMtd(mtdSessions);

						Long projection = (mtdSessions/DateUtil.getCurrentDayOfMonth())*DateUtil.getNumberOfDaysInCurrentMonth();
						kra.setProjection(projection);
						//## session growth end 

					}

					kraReportList.add(kra);			
				}
			}
			kraReportList.sort(new Comparator<KRAreport>() {
				@Override
				public int compare(KRAreport o1, KRAreport o2) {
					// TODO Auto-generated method stub
					return -o1.getShares().compareTo(o2.getShares());
				}
			});	
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getKraReport.", e);
		}
		log.info("Retrieving getKraReport; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);

		return kraReportList;
	}

	public Map<String, Object> getSharability(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		double max_shareability = 0.0;
		double max_shares = 0.0;
		Map<String, Object> finalMap = new HashMap<>();		
		try {
			List<String> storyIdtList = new ArrayList<>();
			storyIdtList = Arrays.asList(query.getStoryid().split(","));

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			boolQuery.must(QueryBuilders.termsQuery(Constants.BHASKARSTORYID, storyIdtList));
			if(query.getUid()!=null) {
				boolQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
			}			

			SearchResponse response = client.prepareSearch(Indexes.FB_DASHBOARD).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.BHASKARSTORYID).size(storyIdtList.size())
							.subAggregation(AggregationBuilders.sum("shares").field(Constants.SHARES))
							.subAggregation(AggregationBuilders.sum("unique_reach").field(Constants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum("ia_timespent").field(Constants.IA_TIMESPENT))
							).execute().actionGet();

			Terms responseAgg = response.getAggregations().get("storyid");		
			for (Terms.Bucket shareBucket : responseAgg.getBuckets()){
				Map<String, Object> recordMap = new HashMap<>();
				double shareability = 0.0;
				double shares = 0;
				long unique_reach = 0;
				long ia_timespent = 0;
				String storyid = null;

				storyid = (String) shareBucket.getKeyAsString();

				Sum totalshares = shareBucket.getAggregations().get("shares");
				shares = totalshares.getValue();
				recordMap.put(Constants.SHARES, shares);
				if(shares>max_shares) {
					max_shares = shares;
				}

				Sum uniqueReach = shareBucket.getAggregations().get("unique_reach");
				unique_reach = (long) uniqueReach.getValue();
				recordMap.put(Constants.UNIQUE_REACH, unique_reach);

				Sum iaTimeSpent = shareBucket.getAggregations().get("ia_timespent");
				ia_timespent = (long) iaTimeSpent.getValue();
				recordMap.put(Constants.IA_TIMESPENT, ia_timespent);

				if (unique_reach != 0) {
					shareability = ((shares / unique_reach) * 100);
					if(shareability>max_shareability) {
						max_shareability = shareability;
					}
				}				
				recordMap.put(Constants.SHAREABILITY, shareability);
				finalMap.put(storyid, recordMap);
			}
			finalMap.put(Constants.MAX_SHAREABILITY, max_shareability);
			finalMap.put(Constants.MAX_SHARES, max_shares);

			for(String key:finalMap.keySet()) {
				if(!Constants.MAX_SHAREABILITY.equals(key) && !Constants.MAX_SHARES.equals(key)) {
					Map<String, Object> map = (HashMap<String, Object>)finalMap.get(key);
					map.put(Constants.NORMALISED_SHAREABILITY, ((Double)map.get(Constants.SHAREABILITY))/max_shareability);	
				} 
			}			
		} 
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSharability.", e);
		}
		log.info("Retrieving getSharability; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return finalMap;
	}

	private Map<String, Object> getOverAllSharability(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> result = new HashMap<>();
		double shareability = 0.0;	
		long totalshares = 0;
		double totalUniqueReach = 0.0;

		BoolQueryBuilder qb = getSocialDecodeQuery(query);		
		try {	

			SearchResponse response = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).setSize(0)
					.addAggregation(AggregationBuilders.sum("shares").field(Constants.SHARES))
					.addAggregation(AggregationBuilders.sum("unique_reach").field(Constants.UNIQUE_REACH))
					.execute().actionGet();

			if (response.getHits().getHits() !=null) {
				Sum shares = response.getAggregations().get("shares");
				totalshares = (long) shares.getValue();
				Sum uniqueReach = response.getAggregations().get("unique_reach");
				totalUniqueReach = uniqueReach.getValue();
				if (totalUniqueReach != 0) {
					shareability = ((totalshares / totalUniqueReach) * 100);
				} 
			}

			result.put(Constants.SHARES, totalshares);
			result.put(Constants.SHAREABILITY, shareability);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getOverAllSharability.", e);
		}
		log.info("Retrieving getOverAllSharability; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return (result);
	}	

	private Long getSessionsCount(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		long totalsessions = 0;
		try {
			String startDate = DateUtil.getCurrentDateTime();		
			String endDate = DateUtil.getCurrentDateTime();		
			if (query.getStartPubDate() != null&&query.getEndPubDate() != null) {
				startDate = query.getStartPubDate();
				endDate = query.getEndPubDate();
			}
			else if (query.getStartDate() != null&&query.getEndDate() != null) {
				startDate = query.getStartDate();
				endDate = query.getEndDate();
			}

			BoolQueryBuilder bqb = new BoolQueryBuilder();

			if (query.getSuper_cat_name() !=null) {
				bqb.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_NAME, query.getSuper_cat_name()));
			}

			if(query.getChannel_slno() !=null) {
				bqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}

			if(query.getUid() !=null) {
				bqb.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
			}

			if(query.getSession_type() !=null) {
				bqb.must(QueryBuilders.termQuery(Constants.SESSION_TYPE, query.getSession_type()));
			}

			if(query.getHost_type() !=null) {
				bqb.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
			}
			
			if(query.getSessionTracker() !=null&&query.getSession_type().equals(SessionTypeConstants.TRACKER)) {
				bqb.must(QueryBuilders.termsQuery(Constants.TRACKER, query.getSessionTracker()));
			}
			
			if(query.getRef_platform() !=null&&query.getSession_type().equals(SessionTypeConstants.REF_PLATFORM)) {
				bqb.must(QueryBuilders.termsQuery(Constants.REF_PLATFORM, query.getRef_platform()));
			}

			String indexName = Indexes.CATEGORISED_SESSION_COUNT;

			if(StringUtils.equals(SessionTypeConstants.AUTHOR, query.getSession_type())&&query.getStartPubDate() != null&&query.getEndPubDate() != null) {
				indexName = Indexes.AUTHORS_SESSION_COUNT;
				bqb.must(QueryBuilders.rangeQuery(Constants.PUB_DATE).gte(startDate).lte(endDate));
				/*To get all records of the given range of published date from all the indexes till today
				from the given startPubDate*/
				endDate = DateUtil.getCurrentDate().replaceAll("_", "-");
			}
			else {
				bqb.must(QueryBuilders.rangeQuery(Constants.DATE).gte(startDate).lte(endDate));
			}
			
			SearchResponse response = client.prepareSearch(IndexUtils.getMonthlyIndexes(indexName, startDate,endDate)).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(bqb).setSize(0)					
					.addAggregation(AggregationBuilders.sum("sessions").field(Constants.SESSION_COUNT))
					.execute().actionGet();

			Sum sessions = response.getAggregations().get("sessions");
			totalsessions = (long) sessions.getValue();

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSessionsCount.", e);
		}
		log.info("Retrieving getSessionsCount; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return totalsessions;
	}	

	public Map<String, LinkedList<String>> getStoriesForFlickerAutomation(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		String interval[] = {"48","72"};
		if(query.getInterval()!=null){
			interval = query.getInterval().split(",");
		}
		Map<String, LinkedList<String>> finalMap = new LinkedHashMap<>();
		try {

			LinkedList<String> topFiveStoriesList = new LinkedList<>();
			LinkedList<String> afterFiveStoriesList = new LinkedList<>();
			LinkedList<String> topTenUniqueStoriesList = new LinkedList<>();
			LinkedList<String> lastFiftyStoriesList = new LinkedList<>();

			String startDate = DateUtil.addHoursToCurrentTime((-1)*Integer.parseInt(interval[0])).replaceAll("_", "-");
			String endDate = DateUtil.getCurrentDate().replaceAll("_", "-");

			if (query.getStartDate() != null) {
				startDate = query.getStartDate();
			}
			if (query.getEndDate() != null) {
				endDate = query.getEndDate();
			}
			BoolQueryBuilder bqb = new BoolQueryBuilder();
			bqb = getQuery(query);
			bqb.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(startDate).to(endDate));
			bqb.mustNot(QueryBuilders.termQuery(Constants.IMAGE, ""));
			int size = 15;

			SearchResponse res = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDate, endDate))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb).setSize(0)
					.addAggregation(AggregationBuilders.terms("storyid").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
							.order(Order.aggregation("pvs", false)).size(size))
					.execute().actionGet();

			Terms storyAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket storyBucket : storyAgg.getBuckets()) {

				String storyIds = storyBucket.getKeyAsString();
				if (storyIds != null) {
					if( topFiveStoriesList.size() < 5) {
						topFiveStoriesList.add(storyIds);
					} else {
						afterFiveStoriesList.add(storyIds);
					}
				}
			}
			finalMap.put("topFivePVS48hours", topFiveStoriesList);
			finalMap.put("topTenPVS48hours", afterFiveStoriesList);

			BoolQueryBuilder bqb2 = getQuery(query);
			bqb2.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, topFiveStoriesList));
			bqb2.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, afterFiveStoriesList));
			bqb2.mustNot(QueryBuilders.termQuery(Constants.IMAGE, ""));
			SearchResponse res2 = client.prepareSearch(Indexes.STORY_UNIQUE_DETAIL)
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb2)
					.addSort(Constants.STORY_MODIFIED_TIME, SortOrder.DESC).setSize(10).setFetchSource(false).execute()
					.actionGet();

			for (SearchHit hit : res2.getHits().getHits()) {
				topTenUniqueStoriesList.add(hit.getId());
				finalMap.put("topTenModifiedDates", topTenUniqueStoriesList);
			}

			startDate = DateUtil.addHoursToCurrentTime((-1)*Integer.parseInt(interval[0])).replaceAll("_", "-");
			BoolQueryBuilder bqb3 = getQuery(query);
			bqb3.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).from(startDate).to(endDate));
			bqb3.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, topFiveStoriesList));
			bqb3.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, afterFiveStoriesList));
			bqb3.mustNot(QueryBuilders.termsQuery(Constants.STORY_ID_FIELD, topTenUniqueStoriesList));
			bqb3.mustNot(QueryBuilders.termQuery(Constants.IMAGE, ""));
			int count = 50;

			SearchResponse res3 = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDate, endDate))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb3).setSize(0)
					.addAggregation(AggregationBuilders.terms("storyids").field(Constants.STORY_ID_FIELD)
							.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))
							.order(Order.aggregation("pvs", false)).size(count))
					.execute().actionGet();
			Terms storyidsAgg = res3.getAggregations().get("storyids");
			for (Terms.Bucket storyBucket : storyidsAgg.getBuckets()) {
				String storyIds = storyBucket.getKeyAsString();
				if (storyIds != null) {
					lastFiftyStoriesList.add(storyIds);
					finalMap.put("topFiftyPVS72hours", lastFiftyStoriesList);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getStoriesForFlickerAutomation.", e);
		}
		log.info("Retrieving getStoriesForFlickerAutomation; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return finalMap;
	}


	public Map<String, Object> getAuditSiteData(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> auditSiteDataMap = new TreeMap<>();
		try {
			double page_depth = 0.0;
			double unique_page_depth = 0.0;
			double frequency = 0.0;
			long total_sessions = 0;
			long pageViews = 0;
			long unique_users = 0;
			long uniquePageViews = 0;

			Map<String, Long> users = getUniqueVisitors(query);
			Map<String, Map<String, Long>> pageviews = getPageViews(query);
			Map<String, Long> sessions = getSessionsForAuditSite(query);

			for (String key : sessions.keySet()) {
				AuditSiteModel asd = new AuditSiteModel();

				total_sessions = sessions.get(key);
				asd.setSessions(total_sessions);

				if (users.get(key) != null) {
					unique_users = users.get(key);
				}
				asd.setUsers(unique_users);

				if (pageviews.get(key) != null) {
					pageViews = ((Map<String, Long>) pageviews.get(key)).get(Constants.PVS);
					uniquePageViews = ((Map<String, Long>) pageviews.get(key)).get(Constants.UVS);
				}
				asd.setPageviews(pageViews);
				asd.setUniquePageviews(uniquePageViews);

				if (unique_users != 0) {
					frequency = total_sessions / unique_users;
					asd.setFrequency(frequency);
				}

				if (total_sessions != 0) {
					page_depth = (double) pageViews / total_sessions;
					asd.setPD_Session(page_depth);
					unique_page_depth = (double) uniquePageViews / total_sessions;
					asd.setUPD_Session(unique_page_depth);
				}
				auditSiteDataMap.put(key, asd);
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getAuditSiteData.", e);
		}
		log.info("Retrieving getAuditSiteData; Execution Time:(Seconds) "+ (System.currentTimeMillis() - startTime) / 1000.0);
		return auditSiteDataMap;
	}

	private BoolQueryBuilder getGAQuery(WisdomQuery query) {
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (query.getHost() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
		}
		if (query.getChannel_slno() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		}
		if (query.getTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getTracker()) {
				trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
			}
			boolQuery.must(trackersBoolQuery);
			// boolQuery.must(QueryBuilders.termsQuery(Constants.TRACKER,
			// query.getTracker()));
		}
		if (query.getExcludeTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getExcludeTracker()) {
				trackersBoolQuery.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
			}
			boolQuery.mustNot(trackersBoolQuery);
			// boolQuery.mustNot(QueryBuilders.termsQuery(Constants.TRACKER,
			// query.getExcludeTracker()));
		}

		if (query.getSuper_cat_id() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}	
		if (query.getSpl_tracker() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.SPL_TRACKER, query.getSpl_tracker()));
		}
		return boolQuery;
	}

	private Map<String, Map<String,Long>> getPageViews(WisdomQuery query) {
		Map<String, Map<String, Long>> pageViewsFinalMap = new HashMap<>();
		String startDate = DateUtil.getCurrentDateTime();
		String endDate = DateUtil.getCurrentDateTime();
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			startDate = query.getStartPubDate();
			endDate = query.getEndPubDate();
		} else if (query.getStartDate() != null && query.getEndDate() != null) {
			startDate = query.getStartDate();
			endDate = query.getEndDate();
		}

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (startDate != null && endDate != null) {
			boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(startDate).to(endDate));
		}

		String interval = "";

		if (query.getInterval() != null && query.getInterval().equalsIgnoreCase(Constants.DAY)) {
			interval = Constants.DAY;
		} else {
			interval = Constants.MONTH;
		}
		DateHistogramInterval histInterval = new DateHistogramInterval(interval);

		boolQuery.must(getGAQuery(query));

		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDate, endDate))
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.dateHistogram("histo").field(Constants.DATE_TIME_FIELD)
						.dateHistogramInterval(histInterval).format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.sum("pageViews").field(Constants.PVS))
						.subAggregation(AggregationBuilders.sum("uniquePageViews").field(Constants.UVS)))
				.execute().actionGet();

		Histogram dateHistoiInterval = res.getAggregations().get("histo");
		for (Histogram.Bucket bucket : dateHistoiInterval.getBuckets()) {

			String key = bucket.getKeyAsString();
			if (bucket.getDocCount() > 0) {
				Map<String, Long> pgvwsMap = new HashMap<>();
				Sum pgvs = bucket.getAggregations().get("pageViews");
				Long pageViews = (long) pgvs.getValue();
				pgvwsMap.put(Constants.PVS, pageViews);

				Sum upgvs = bucket.getAggregations().get("uniquePageViews");
				Long uniquePageViews = (long) upgvs.getValue();
				pgvwsMap.put(Constants.UVS, uniquePageViews);
				pageViewsFinalMap.put(key, pgvwsMap);
			}

		}
		return pageViewsFinalMap;
	}

	private Map<String, Long> getUniqueVisitors(WisdomQuery query) {
		Map<String, Long> uvsMap = new HashMap<>();
		String startDate = DateUtil.getCurrentDateTime();
		String endDate = DateUtil.getCurrentDateTime();
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			startDate = query.getStartPubDate();
			endDate = query.getEndPubDate();
		} else if (query.getStartDate() != null && query.getEndDate() != null) {
			startDate = query.getStartDate();
			endDate = query.getEndDate();
		}

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		if (startDate != null && endDate != null) {
			boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE).from(startDate).to(endDate));
		}
		if (query.getDomaintype() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.DOMAINTYPE, query.getDomaintype()));
		}

		String interval = "";

		if (query.getInterval() != null && query.getInterval().equalsIgnoreCase(Constants.DAY)) {
			interval = Constants.DAY;
		} else {
			interval = Constants.MONTH;
		}
		if (query.getInterval() != null) {
			boolQuery.must(QueryBuilders.termsQuery(Constants.INTERVAL, interval));
		}

		boolQuery.must(getGAQuery(query));
		DateHistogramInterval histInterval = new DateHistogramInterval(interval);

		SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.UVS_COUNT, startDate, endDate))
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.dateHistogram("histo").field(Constants.DATE_TIME_FIELD)
						.dateHistogramInterval(histInterval).format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.sum("uvs").field(Constants.UVS)))
				.execute().actionGet();

		Histogram dateHistoiInterval = res.getAggregations().get("histo");
		for (Histogram.Bucket bucket : dateHistoiInterval.getBuckets()) {
			if (bucket.getDocCount() > 0) {
				String key = bucket.getKeyAsString();
				Sum unique_visitors = bucket.getAggregations().get("uvs");
				Long uvs = (long) unique_visitors.getValue();
				uvsMap.put(key, uvs);
			}
		}
		return uvsMap;
	}


	private Map<String, Long> getSessionsForAuditSite(WisdomQuery query){
		Map<String, Long> sessionsMap = new HashMap<>();
		String startDate = DateUtil.getCurrentDateTime();
		String endDate = DateUtil.getCurrentDateTime();
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			startDate = query.getStartPubDate();
			endDate = query.getEndPubDate();
		} else if (query.getStartDate() != null && query.getEndDate() != null) {
			startDate = query.getStartDate();
			endDate = query.getEndDate();
		}
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		if (startDate != null && endDate != null) {
			boolQuery.must(QueryBuilders.rangeQuery(Constants.DATE).from(startDate).to(endDate));
		}

		String interval = "";

		if (query.getInterval() != null && query.getInterval().equalsIgnoreCase(Constants.DAY)) {
			interval = Constants.DAY;
		} 
		else {
			interval = Constants.MONTH;
		}
		boolQuery.must(getGAQuery(query));
		DateHistogramInterval histInterval = new DateHistogramInterval(interval);
		SearchResponse response = client
				.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.CATEGORISED_SESSION_COUNT, startDate, endDate))
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolQuery).setSize(0)
				.addAggregation(AggregationBuilders.dateHistogram("histo").field(Constants.DATE_TIME_FIELD)
						.dateHistogramInterval(histInterval).format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.sum("sessions").field(Constants.SESSION_COUNT))).execute().actionGet();
		Histogram dateHistoiInterval = response.getAggregations().get("histo");
		for (Histogram.Bucket bucket : dateHistoiInterval.getBuckets()) {			
			if (bucket.getDocCount() > 0) {
				String key = bucket.getKeyAsString();
				Sum sessions = bucket.getAggregations().get("sessions");
				Long sessions_count = (long) sessions.getValue();
				sessionsMap.put(key, sessions_count);
			}
		}		
		return sessionsMap;
	}

	public Map<String, Object> getMISReports(WisdomQuery query) {

		long startTime = System.currentTimeMillis();
		Map<String, Object> result = new HashMap<>();

		//String startDate = DateUtil.getCurrentDate();
		String endDate = DateUtil.getCurrentDate();

		if (query.getEndDate() != null) {
			//startDate = query.getStartDate();
			endDate = query.getEndDate();
		}

		ArrayList<String> dates = new ArrayList<>();
		dates.add(endDate);
		dates.add(DateUtil.getDateOfPreviousMonth(endDate, "yyyy-MM-dd"));

		BoolQueryBuilder builder = QueryBuilders.boolQuery();

		if (query.getHost_type() != null && !query.getHost_type().isEmpty()) {
			builder.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
		}

		if (query.getChannel_slno() != null && !query.getChannel_slno().isEmpty()) {
			builder.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		}

		if (query.getIdentifierValue() != null ) {
			builder.must(QueryBuilders.termsQuery(Constants.IDENTIFIER_VALUE, query.getIdentifierValue()));
		}

		if (StringUtils.isNotBlank(query.getEndDate())) {
			builder.must(QueryBuilders.termsQuery(Constants.DATE, dates));
		} else {
			throw new DBAnalyticsException("Missing date param.");
		}



		SearchResponse response = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_REPORT, endDate, endDate)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(builder).setSize(0)
				.addAggregation(AggregationBuilders.terms(Constants.CHANNEL_SLNO).field(Constants.CHANNEL_SLNO).size(100)
						.subAggregation(AggregationBuilders.terms(Constants.HOST_TYPE).field(Constants.HOST_TYPE).size(10)
								.subAggregation(AggregationBuilders.terms(Constants.IDENTIFIER_VALUE).field(Constants.IDENTIFIER_VALUE).size(50)
										.subAggregation(AggregationBuilders.topHits("top").size(5).sort(Constants.DATE,SortOrder.DESC)))))
				.execute().actionGet();

		Terms channelAgg = response.getAggregations().get(Constants.CHANNEL_SLNO);
		for(Terms.Bucket channelTerms:channelAgg.getBuckets()){
			Map<String, Object> channelMap = new HashMap<>();
			String channel = channelTerms.getKeyAsString();
			Terms hostTypeAgg = channelTerms.getAggregations().get(Constants.HOST_TYPE);
			for(Terms.Bucket hostTerms:hostTypeAgg.getBuckets()){
				String hostType = hostTerms.getKeyAsString();
				Map<String, List<Object>> identifiersDataMap = new HashMap<>();
				Terms identifierAgg = hostTerms.getAggregations().get(Constants.IDENTIFIER_VALUE);
				for(Terms.Bucket identifierTerm:identifierAgg.getBuckets()){
					String identifier = identifierTerm.getKeyAsString();
					List<Object> identifierData = new LinkedList<>();
					TopHits topHits = identifierTerm.getAggregations().get("top");
					for(SearchHit hit:topHits.getHits()){
						identifierData.add(hit.getSource());
					}
					identifiersDataMap.put(identifier, identifierData);
				}
				channelMap.put(hostType, identifiersDataMap);
			}
			result.put(channel, channelMap);
		}

		long endTime = System.currentTimeMillis();
		log.info("Execution Time for MIS Reports: " + (endTime - startTime) + "ms.");
		return result;
	}

	public List<TopAuthorsResponse> getAuthorsListForNewsLetter(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		List<TopAuthorsResponse> records = new ArrayList<>();		
		try {
			String startPubDate = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endPubDate = DateUtil.getCurrentDateTime();
			List<String> autoAuthorsNames = Arrays.asList("Bhaskar Automation","Divyabhaskar Automation","BBC Automation");
			List<Integer> autoAuthorsuids	 = Arrays.asList(254,255,256);
			if (query.getStartPubDate() != null) {
				startPubDate = query.getStartPubDate();
			}
			if (query.getEndPubDate() != null) {
				endPubDate = query.getEndPubDate();
			}

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(startPubDate).lte(endPubDate))
					.mustNot(QueryBuilders.termsQuery(Constants.AUTHOR_NAME, autoAuthorsNames))
					.mustNot(QueryBuilders.termsQuery(Constants.UID, autoAuthorsuids));
					if(query.getChannel_slno()!=null) {
						boolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
					}

			if (query.getUid() != null) {
				boolQuery.must(QueryBuilders.termsQuery(Constants.UID, query.getUid()));
			}

			String includes[] = { Constants.AUTHOR_NAME };
			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("author_id").field(Constants.UID).size(100)
							.order(Order.aggregation("tuvs", false))							
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(AggregationBuilders.sum("tuvs").field(Constants.UVS))
							.subAggregation(
									AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1).sort(Constants.DATE_TIME_FIELD, SortOrder.DESC))
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS)).subAggregation(
											AggregationBuilders.sum("uvs").field(Constants.UVS))))
					.setSize(0).execute().actionGet();

			Terms authorBuckets = res.getAggregations().get("author_id");
			for (Terms.Bucket author : authorBuckets.getBuckets()) {
				TopAuthorsResponse authorDetail = new TopAuthorsResponse();				
				TopHits topHits = author.getAggregations().get("top");
				authorDetail.setAuthor_name(
						topHits.getHits().getHits()[0].getSource().get(Constants.AUTHOR_NAME).toString());

				Cardinality storyCount = author.getAggregations().get("storyCount");
				authorDetail.setStory_count((int)storyCount.getValue());

				authorDetail.setUid(author.getKeyAsString());

				Sum tpvs = author.getAggregations().get("tpvs");
				authorDetail.setTotalpvs(((long) tpvs.getValue()));

				Sum tuvs = author.getAggregations().get("tuvs");
				authorDetail.setTotaluvs(((long) tuvs.getValue()));				

				List<String> author_id = new ArrayList<>();
				int authorId = Integer.parseInt(author.getKeyAsString());
				author_id.add(String.valueOf(authorId));
				query.setUid(author_id);

				Map<String, Object> sharbilityMap = getOverAllSharability(query);
				//authorDetail.setSharability((Double)sharbilityMap.get(Constants.SHAREABILITY));
				authorDetail.setShares((Long)sharbilityMap.get(Constants.SHARES));
				authorDetail.setSessions(getSessionsCount(query));
				authorDetail.setAuthor_stories(getStoriesList(query));								
				records.add(authorDetail);				
				records.sort(new Comparator<TopAuthorsResponse>() {
					@Override
					public int compare(TopAuthorsResponse o1, TopAuthorsResponse o2) {
						return -o1.getSessions().compareTo(o2.getSessions());
					}
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving Authors List For News Letter.", e);
		}
		log.info("Retrieving top authors; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}


	private List<String> getLastDatesOfMonth(WisdomQuery query) {

		String endDate = DateUtil.getCurrentDate().replaceAll("_", "/");

		String startDate = DateUtil.getDateOfPreviousYear(DateUtil.getCurrentDate().replaceAll("_", "-"), "yyyy-MM-dd")
				.replaceAll("-", "/");

		if(query.getStartDate()!=null) {
			startDate = query.getStartDate().replaceAll("-", "/");
		}
		if(query.getStartDate()!=null) {
			endDate = query.getEndDate().replaceAll("-", "/");
		}		
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuu/MM/dd", Locale.ROOT);
		DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("MM", Locale.ROOT);
		DateTimeFormatter yearFormatter = DateTimeFormatter.ofPattern("uuuu", Locale.ROOT);
		YearMonth endMonth = YearMonth.parse(endDate, dateFormatter);
		List<String> lastDateList = new ArrayList<>();
		for (YearMonth month = YearMonth.parse(startDate, dateFormatter); !month.isAfter(endMonth); month = month
				.plusMonths(1)) {
			YearMonth yearMonth = YearMonth.of(Integer.parseInt(month.format(yearFormatter).toString()),
					Integer.parseInt(month.format(monthFormatter).toString()));
			LocalDate last = yearMonth.atEndOfMonth();
			lastDateList.add(last.toString());
		}
		return lastDateList;}


	public Map<String, Object> getYTDReport(WisdomQuery query) throws ParseException {
		long startTime = System.currentTimeMillis();
		Map<String, Object> finalMap = new HashMap<>();
		try {
			List<String> datesList = getLastDatesOfMonth(query);				
			BoolQueryBuilder channelwiseBQB = new BoolQueryBuilder();
			BoolQueryBuilder overallBQB = new BoolQueryBuilder();	
			BoolQueryBuilder ytdBoolQuery = new BoolQueryBuilder();
			if (query.getChannel_slno() != null && !query.getChannel_slno().isEmpty()) {
				ytdBoolQuery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if (datesList != null) {
				ytdBoolQuery.must(QueryBuilders.termsQuery(Constants.DATE, datesList));
			}

			if (query.getFilter() != null && !query.getFilter().isEmpty()) {

				if (StringUtils.equals(Constants.WITHOUT_UC, query.getFilter())) {
					overallBQB.must(QueryBuilders.termQuery(Constants.IDENTIFIER_VALUE, Constants.WITHOUT_UC_OVERALL));				
					channelwiseBQB.must(QueryBuilders.termQuery(Constants.IDENTIFIER_VALUE, Constants.WITHOUT_UC));
				}			
				else if (StringUtils.equals(Constants.WITH_UC, query.getFilter())) {				
					overallBQB.must(QueryBuilders.termQuery(Constants.IDENTIFIER_VALUE, Constants.OVERALL));				
					channelwiseBQB.must(QueryBuilders.termQuery(Constants.IDENTIFIER_VALUE, Constants.CHANNEL_OVERALL));		
				}
			}
			overallBQB.must(ytdBoolQuery);
			channelwiseBQB.must(ytdBoolQuery);
			String startDate = DateUtil.getCurrentDate();
			String endDate = DateUtil.getCurrentDate();		

			SearchResponse overAllRes = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_REPORT, startDate, endDate)).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(overallBQB).setSize(100).execute().actionGet();
			SearchHit[] overallHits = overAllRes.getHits().getHits();
			//System.out.println("overAllRes :"+overAllRes);
			Map<String, Object> overAllMap = new TreeMap<>();
			Map<String, Object> overallWebMap = new TreeMap<>();
			Map<String, Object> overallWapMap = new TreeMap<>();
			Map<String, Object> overallAndroidMap = new TreeMap<>();
			Map<String, Object> overallIosMap = new TreeMap<>();
			for (SearchHit hit : overallHits) {				
				Map<String, Object> source = hit.getSource();
				String hostType = (String) source.get(Constants.HOST_TYPE);

				if (hostType.equals("w")) {
					overallWebMap.put((String) source.get(Constants.DATE), hit.getSource());
				} 			
				else if (hostType.equals("m")) {
					overallWapMap.put((String) source.get(Constants.DATE), hit.getSource());
				} 			
				else if (hostType.equals("a")) {
					overallAndroidMap.put((String) source.get(Constants.DATE), hit.getSource());
				} 
				else if (hostType.equals("i"))
				{
					overallIosMap.put((String) source.get(Constants.DATE), hit.getSource());
				} 
			}	
			overAllMap.put("Web", overallWebMap);
			overAllMap.put("Wap", overallWapMap);
			overAllMap.put("Android", overallAndroidMap);
			overAllMap.put("Ios", overallIosMap);
			finalMap.put("overall", overAllMap);			

			SearchResponse channelwise_response = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_REPORT, startDate, endDate))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(channelwiseBQB)
					.addAggregation(AggregationBuilders.terms(Constants.CHANNEL_SLNO).field(Constants.CHANNEL_SLNO).size(50)
							.subAggregation(AggregationBuilders.topHits("topHits").size(50)))
					.setSize(0).execute().actionGet();

			Terms channelTerms = channelwise_response.getAggregations().get(Constants.CHANNEL_SLNO);
			for(Terms.Bucket channelBucket:channelTerms.getBuckets()){
				String channel = channelBucket.getKeyAsString();
				Map<String, Object> webMap = new TreeMap<>();
				Map<String, Object> wapMap = new TreeMap<>();
				Map<String, Object> androidMap = new TreeMap<>();
				Map<String, Object> iosMap = new TreeMap<>();
				Map<String, Object> channelMap = new TreeMap<>();
				TopHits hits = channelBucket.getAggregations().get("topHits");
				for (SearchHit hit : hits.getHits()) {
					Map<String, Object> source = hit.getSource();

					String hostType = (String) source.get(Constants.HOST_TYPE);
					if (hostType.equals("w")) {
						webMap.put((String) source.get(Constants.DATE), hit.getSource());					
					} else if (hostType.equals("m")) {
						wapMap.put((String) source.get(Constants.DATE), hit.getSource());
					} else if (hostType.equals("a")) {
						androidMap.put((String) source.get(Constants.DATE), hit.getSource());
					} else if (hostType.equals("i")) {
						iosMap.put((String) source.get(Constants.DATE), hit.getSource());
					}				
				}
				channelMap.put("Web", webMap);
				channelMap.put("Wap", wapMap);
				channelMap.put("Android", androidMap);
				channelMap.put("Ios", iosMap);
				finalMap.put(channel, channelMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getYTDReport", e);
		}
		log.info("Retrieving getYTDReport; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);

		return finalMap;
	}

	public Map<String, Object> getDailyData(WisdomQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> dataMap = new LinkedHashMap<>();
		try {
			BoolQueryBuilder bqb = new BoolQueryBuilder();
			if (query.getChannel_slno() != null) {
				bqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if (query.getHost() != null) {
				bqb.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
			}

			String date = DateUtil.getCurrentDate();
			if(query.getStartDate()!=null) {
				date = query.getStartDate().replaceAll("-", "_");
			}

			String indexName = "realtime_" + date;		
			SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb)
					.setSize(0).addAggregation(AggregationBuilders.cardinality("uvs").field(Constants.SESSION_ID_FIELD))
					.addAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)).execute()
					.actionGet();		 

			long pvs = res.getHits().getTotalHits();
			dataMap.put("pvs", pvs);

			Cardinality unique_visitors = res.getAggregations().get("uvs");
			dataMap.put("uvs", unique_visitors.getValue());

			Cardinality sessions = res.getAggregations().get("sessions");
			dataMap.put("sessions", sessions.getValue());
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving Daily Data for verification", e);
		}
		log.info("Retrieving getDailyData; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);

		return dataMap;}


	public VideoReport getVideoReport(WisdomQuery query) {	
		long startTime = System.currentTimeMillis();
		VideoReport vdo =new VideoReport();

		try{
			String startDate = DateUtil.getCurrentDate().replaceAll("_", "-");
			
			if (query.getStartDate() != null) {
				startDate = query.getStartDate();
			}

			String endDate = DateUtil.getCurrentDate().replaceAll("_", "-");
			if (query.getEndDate() != null) {
				endDate = query.getEndDate();
			}

			BoolQueryBuilder vBqb = new BoolQueryBuilder();
			if(query.getStartDate()!=null && query.getEndDate()!=null) {
				vBqb.must(QueryBuilders.rangeQuery(Constants.DATE).gte(startDate).lte(endDate));
			}
			if (query.getHost_type() != null && !query.getHost_type().isEmpty()) {
				vBqb.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
			}
			if (query.getChannel_slno() != null && !query.getChannel_slno().isEmpty()) {
				vBqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}

			SearchResponse res = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_USERS,
							startDate, endDate))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(vBqb).setSize(200).execute().actionGet();


			long total_users=0;
			long total_sessions=0;
			long monthly_active_users=0;
			long unique_visitors = 0;
			for (SearchHit hit : res.getHits().getHits()) {					
				Map<String, Object> source = hit.getSource();
				total_users += (double) source.get(VideoReportConstants.UNIQUE_VISITORS);
				total_sessions += (double) source.get(VideoReportConstants.SESSIONS);
				monthly_active_users += (double) source.get(VideoReportConstants.UNIQUE_VISITORS_PREV_30_DAYS);
			}

			// #START of Prepare Search to get Daily Active Users

			/*{
			String startDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate(), -30);
		}*/
			SearchResponse dauRes = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_USERS,	DateUtil.getCurrentDate().replaceAll("_", "-"), DateUtil.getCurrentDate().replaceAll("_", "-")))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(vBqb).setSize(0)
					.addAggregation(AggregationBuilders.sum("unique_users").field(VideoReportConstants.UNIQUE_VISITORS))
					.execute().actionGet();

			Sum users = dauRes.getAggregations().get("unique_users");
			unique_visitors = (long) users.getValue();
			// #END of Prepare Search to get Daily Active Users

			vdo.setTotal_users(total_users);
			vdo.setTotal_sessions(total_sessions);
			vdo.setDaily_active_users(unique_visitors/30);
			vdo.setMonthly_active_users(monthly_active_users);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getVideoReport.", e);
		}
		log.info("Retrieving getVideoReport; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return vdo;
	}

	/*private Long getDailyActiveUsers(WisdomQuery query) {
		long unique_visitors = 0;

		String endDate = DateUtil.getCurrentDate();
		if (query.getEndDate() != null) {
			endDate = query.getEndDate();
		}	
		String startDate = DateUtil.getCurrentDate();
		if (query.getEndDate() != null) {
			startDate = query.getStartDate();
		}
		{
			String startDate = DateUtil.getPreviousDate(DateUtil.getCurrentDate(), -30);
		}		

		BoolQueryBuilder vBqb = new BoolQueryBuilder();

		if (query.getStartDate() != null && query.getEndDate() != null) {
			vBqb.must(QueryBuilders.rangeQuery(Constants.DATE).gte(startDate).lte(endDate));
		}

		if (query.getHost_type() != null && !query.getHost_type().isEmpty()) {
			vBqb.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
		}
		if (query.getChannel_slno() != null && !query.getChannel_slno().isEmpty()) {
			vBqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
		}

		SearchResponse res = client
				.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.AGGREGATE_USERS,
						DateUtil.getCurrentDate().replaceAll("_", "-"), DateUtil.getCurrentDate().replaceAll("_", "-")))
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(vBqb).setSize(0)
				.addAggregation(AggregationBuilders.sum("unique_users").field(VideoReportConstants.UNIQUE_VISITORS))
				.execute().actionGet();

		Sum users = res.getAggregations().get("unique_users");
				unique_visitors += (long) users.getValue();

		long DAU = unique_visitors / 30;
		return DAU;

	}*/

	public List<CompetitorStory> getKeywordBasedSimilarStories(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<CompetitorStory> records = new LinkedList<>();
		try {	

			BoolQueryBuilder qbFilter = new BoolQueryBuilder();
			
			String parameter = query.getParameter();
			
			parameter = "total_engagement";

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getCompetitor()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}

			BoolQueryBuilder qb = new BoolQueryBuilder();
			//qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, query.getTitle()).fuzziness(query.getFuzziness()));
			qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, Arrays.toString(query.getKeywords().toArray())));
			
			SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
					.highlighter(new HighlightBuilder().field(Constants.TITLE))
					.setQuery(qb)
					.addSort(parameter, SortOrder.DESC)
					.setSize(query.getCount()).execute().actionGet();
			for (SearchHit hit : res.getHits().getHits()) {
				CompetitorStory story = new CompetitorStory();

				story =	gson.fromJson(hit.getSourceAsString(), CompetitorStory.class);
				//story.setScore((double) hit.getScore());				
				if (hit.getHighlightFields().get(Constants.TITLE) != null) {
					String titleText = "";
					Text[] texts = hit.getHighlightFields().get(Constants.TITLE).getFragments();

					for(Text text:texts){
					titleText += text.toString();
					}
					story.setHighlightedTitle(titleText);
					query.setStoryid(story.getStoryid());
					story.setSentimentScore(getSentimentScore(query));
					//story.setTitle(hit.getHighlightFields().get(Constants.TITLE).getFragments()[0].toString());
				}
				records.add(story);
				/*if(story.getScore() > query.getScore()){
					records.add(story);
				}*/						
			}						

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getKeywordBasedSimilarStories.", e);
		}
		log.info("Retrieving getKeywordBasedSimilarStories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public List<Map<String, Object>> getKeywordBasedSimilarStoriesCount(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> result = new LinkedList<>();
		LinkedHashMap<String, Long> map = new LinkedHashMap<>();
		try {
			BoolQueryBuilder qbFilter = new BoolQueryBuilder();

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getCompetitor()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}

			ArrayList<String> keywordList = (ArrayList<String>)query.getKeywords();
			MultiSearchRequestBuilder request = client.prepareMultiSearch();
			for(String keyword:keywordList){
				BoolQueryBuilder qb = new BoolQueryBuilder();
				qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, keyword));
				request.add(client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb).setSize(0));
			}
			
			MultiSearchResponse respone = request.get();
			int index = 0;
			for(MultiSearchResponse.Item item:respone.getResponses())
			{				
				SearchResponse res = item.getResponse();
				long count = 0;
				if(res!=null)
				{
					count = res.getHits().getTotalHits();
				}
				map.put(keywordList.get(index), count);
				index++;
			}
			
			 Set<Entry<String, Long>> set = map.entrySet();
		     List<Entry<String, Long>> list = new LinkedList<Entry<String, Long>>(set);
		       
			Collections.sort(list, new Comparator<Map.Entry<String, Long>>()
	        {
	            public int compare( Map.Entry<String, Long> o1, Map.Entry<String, Long> o2 )
	            {
	                return (o2.getValue()).compareTo( o1.getValue() );
	            }
	        } );
			
			Map<String, Object> keywordMap;
			for(Entry<String, Long> entry:list){
				keywordMap = new HashMap<>();
				keywordMap.put("keyword", entry.getKey());
				keywordMap.put("count", entry.getValue());
				result.add(keywordMap);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getKeywordBasedSimilarStoriesCount.", e);
		}
		log.info("Retrieving getKeywordBasedSimilarStoriesCount; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}
	
	public List<Map<String, Object>> getKeywordBasedSimilarStoriesCountAlphabetical(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> result = new LinkedList<>();
		TreeMap<String, Long> map = new TreeMap<>();
		try {
			BoolQueryBuilder qbFilter = new BoolQueryBuilder();

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getCompetitor()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}

			ArrayList<String> keywordList = (ArrayList<String>)query.getKeywords();
			MultiSearchRequestBuilder request = client.prepareMultiSearch();
			for(String keyword:keywordList){
				BoolQueryBuilder qb = new BoolQueryBuilder();
				qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, keyword));
				request.add(client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb).setSize(0));
			}
			
			MultiSearchResponse respone = request.get();
			int index = 0;
			for(MultiSearchResponse.Item item:respone.getResponses())
			{				
				SearchResponse res = item.getResponse();
				long count = 0;
				if(res!=null)
				{
					count = res.getHits().getTotalHits();
				}
				map.put(keywordList.get(index), count);
				index++;
			}
			
			Map<String, Object> keywordMap;
			for(Entry<String, Long> entry:map.entrySet()){
				keywordMap = new HashMap<>();
				keywordMap.put("keyword", entry.getKey());
				keywordMap.put("count", entry.getValue());
				result.add(keywordMap);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getKeywordBasedSimilarStoriesCount.", e);
		}
		log.info("Retrieving getKeywordBasedSimilarStoriesCount; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}
	
	public Map<String, Object> getKeywordBasedSimilarStoriesCountByInterval(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		Map<String, Object> result = new HashMap<>();
		String interval = query.getInterval();
		DateHistogramInterval histInterval = new DateHistogramInterval(interval);
		Histogram.Order order = null;

		if (query.isOrderAsc())
			order = Histogram.Order.KEY_ASC;
		else
			order = Histogram.Order.KEY_DESC;
		try {
			BoolQueryBuilder qbFilter = new BoolQueryBuilder();

			qbFilter.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if(query.getChannel_slno()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, query.getChannel_slno()));
			}
			if(query.getCompetitor()!=null){
				qbFilter.must(QueryBuilders.termsQuery(Constants.COMPETITOR, query.getCompetitor()));
			}

			ArrayList<String> keywordList = (ArrayList<String>)query.getKeywords();
			MultiSearchRequestBuilder request = client.prepareMultiSearch();
			for(String keyword:keywordList){
				BoolQueryBuilder qb = new BoolQueryBuilder();
				qb.filter(qbFilter).must(QueryBuilders.matchQuery(Constants.TITLE, keyword));
				request.add(client.prepareSearch(Indexes.FB_COMPETITOR).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb).setSize(0)
						.addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
								.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order)));
			}
			
			MultiSearchResponse respone = request.get();
			int index = 0;
			for(MultiSearchResponse.Item item:respone.getResponses())
			{				
				SearchResponse res = item.getResponse();
				Histogram histogram = res.getAggregations().get("interval");
				List<Object> wordCountList = new LinkedList<>();
				for (Histogram.Bucket bucket : histogram.getBuckets()) {
					Map<String, Object> countMap = new HashMap<>();
					countMap.put("count", bucket.getDocCount());
					countMap.put("date",bucket.getKeyAsString());
					wordCountList.add(countMap);
				}
				result.put(keywordList.get(index), wordCountList);
				index++;
			}		

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getKeywordBasedSimilarStoriesCount.", e);
		}
		log.info("Retrieving getKeywordBasedSimilarStoriesCount; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}
	
	public List<Object> getTotalEngagementByInterval(WisdomQuery query)	
	{
		long startTime = System.currentTimeMillis();
		List<Object> result = new LinkedList<>();
		String interval = query.getInterval();
		DateHistogramInterval histInterval = new DateHistogramInterval(interval);
		Histogram.Order order = null;

		String parameter = query.getParameter();
		parameter = "total_engagement";
		
		if (query.isOrderAsc())
			order = Histogram.Order.KEY_ASC;
		else
			order = Histogram.Order.KEY_DESC;
		try {
			BoolQueryBuilder qb = new BoolQueryBuilder();

			if(query.getStoryid()!=null){
				qb.must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid()));
			}
			 SearchResponse res = client.prepareSearch(Indexes.FB_COMPETITOR_HOURLY).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(qb).setSize(0)
						.addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
								.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order)
								.subAggregation(AggregationBuilders.sum(parameter).field(parameter)))
						.execute().actionGet();
			
				Histogram histogram = res.getAggregations().get("interval");
				for (Histogram.Bucket bucket : histogram.getBuckets()) {
					Sum param_agg = bucket.getAggregations().get(parameter);
					int param = (int) param_agg.getValue();
					Map<String, Object> countMap = new HashMap<>();
					countMap.put("count", param);
					countMap.put("date",bucket.getKeyAsString());
					result.add(countMap);
				}			

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getTotalEngagementByInterval.", e);
		}
		log.info("Retrieving getTotalEngagementByInterval; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}
	
	private Double getSentimentScore(WisdomQuery query)
	{
		long startTime = System.currentTimeMillis();
		double result = 2;
		try{

			QueryBuilder qb = QueryBuilders.termQuery(Constants.STORY_ID_FIELD, query.getStoryid());
			SearchResponse res = client.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.FB_COMMENTS, query.getStartDate(), query.getEndDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).setSize(0)
					.addAggregation(AggregationBuilders.avg(Constants.SCORE).field(Constants.SCORE))
					.execute().actionGet();

			Avg scoreAgg = res.getAggregations().get(Constants.SCORE);

			if(res. getHits().getTotalHits()>0)
			{
				result = scoreAgg.getValue();
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSentimentScore for storyid: "+query.getStoryid(), e);
		}
		log.info("Retrieving getSentimentScore; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}

	public LinkedHashMap<String, Object> getNotificationSessions(WisdomQuery query) {
		LinkedHashMap<String, Object> records = new LinkedHashMap<>();
		long startTime = System.currentTimeMillis();

		try {
			String indexDate = query.getStartDate().replaceAll("-", "_");
			String indexName = "realtime_" + indexDate;
			
			BoolQueryBuilder qb = new BoolQueryBuilder();
			if(query.getHost_type()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.HOST_TYPE, query.getHost_type()));
			}
			if(query.getHost()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.HOST, query.getHost()));
			}
			if(query.getTracker()!=null)
			{
				qb.must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, query.getTracker()));
			}
			
			SearchResponse res = client.prepareSearch(indexName)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).setSize(0)
					.addAggregation(AggregationBuilders.cardinality(Constants.SESS_ID).field(Constants.SESS_ID))
					.execute().actionGet();
			Cardinality sessionsAgg = res.getAggregations().get(Constants.SESS_ID);
			Long sessions = sessionsAgg.getValue();
			records.put(indexDate, sessions);
		} 
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getNotificationSessions.", e);
		}
		log.info("Retrieving getNotificationSessions; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}

	public List<HashMap<String, Object>> getSocialAnalysis(WisdomQuery query) {
		List<HashMap<String, Object>> records = new LinkedList<>();
		long startTime = System.currentTimeMillis();
		try {
			String parameter = query.getParameter();
			boolean order = query.isOrderAsc();
			int count = query.getCount();
			String field = query.getField();
			BoolQueryBuilder qb = getSocialDecodeQuery(query);
			SearchResponse res = client.prepareSearch(Indexes.FB_DASHBOARD)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).setSize(0)
					.addAggregation(AggregationBuilders.terms("field").field(field).size(count)
							.order(Order.aggregation(parameter, order))
							.subAggregation(AggregationBuilders.sum(Constants.TOTAL_REACH).field(Constants.TOTAL_REACH))
							.subAggregation(AggregationBuilders.sum(Constants.UNIQUE_REACH).field(Constants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(Constants.LINK_CLICKS).field(Constants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.cardinality("story_count").field(Constants.STORY_ID_FIELD)))
					.execute().actionGet();
			Terms fieldTerms = res.getAggregations().get("field");
			for(Terms.Bucket fieldBucket:fieldTerms.getBuckets()){
				HashMap<String, Object> fieldMap = new HashMap<>();
				Sum total_reach_agg = fieldBucket.getAggregations().get(Constants.TOTAL_REACH);
				Long total_reach = ((Double)total_reach_agg.getValue()).longValue();
				Sum unique_reach_agg = fieldBucket.getAggregations().get(Constants.UNIQUE_REACH);
				Long unique_reach = ((Double)unique_reach_agg.getValue()).longValue();
				Sum link_clicks_agg = fieldBucket.getAggregations().get(Constants.LINK_CLICKS);
				Long link_clicks = ((Double)link_clicks_agg.getValue()).longValue();
				Cardinality story_count_agg = fieldBucket.getAggregations().get("story_count");
				Long story_count = story_count_agg.getValue();
				fieldMap.put(field,fieldBucket.getKeyAsString());
				fieldMap.put(Constants.TOTAL_REACH,total_reach);
				fieldMap.put(Constants.UNIQUE_REACH,unique_reach);
				fieldMap.put(Constants.LINK_CLICKS,link_clicks);
				fieldMap.put("story_count",story_count);
				records.add(fieldMap);
			}			
		} 
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSocialAnalysis.", e);
		}
		log.info("Retrieving getSocialAnalysis; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public HashMap<String, Object> getCompleteStoryDetail(WisdomQuery query) {
		HashMap<String, Object> records = new HashMap<>();
		long startTime = System.currentTimeMillis();
		try {
			String storyid = query.getStoryid();
			QueryBuilder fbQb = QueryBuilders.termQuery(Constants.BHASKARSTORYID, storyid);
			SearchResponse fbRes = client.prepareSearch(Indexes.FB_DASHBOARD)
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(fbQb).setSize(1)
					.execute().actionGet();
			Map<String, Object> fbSource = 	fbRes.getHits().getHits()[0].getSource();
			records.putAll(fbSource);
			
			BoolQueryBuilder realtimeQb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(Constants.STORY_ID_FIELD, storyid))
					.must(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, query.getTracker()));
			
			SearchResponse realtimeRes = client.prepareSearch(IndexUtils.getYearlyIndex(Indexes.STORY_DETAIL))
					.setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(realtimeQb).setSize(0)
					.addAggregation(AggregationBuilders.sum(Constants.PVS).field(Constants.PVS))
					.addAggregation(AggregationBuilders.sum(Constants.UVS).field(Constants.UVS))
					.execute().actionGet();
			
			Sum pvsAgg = realtimeRes.getAggregations().get(Constants.PVS);
			double pvs = pvsAgg.getValue();
			Sum uvsAgg = realtimeRes.getAggregations().get(Constants.UVS);
			double uvs = uvsAgg.getValue();
			double ctr = 0.0;
			if(((Number)records.get(Constants.LINK_CLICKS)).doubleValue()!=0&&((Number)records.get(Constants.TOTAL_REACH)).doubleValue()!=0){
				ctr = ((Number)records.get(Constants.LINK_CLICKS)).doubleValue()/((Number)records.get(Constants.TOTAL_REACH)).doubleValue();
			}
			records.put(Constants.CTR, ctr);
			records.put(Constants.PVS, pvs);
			records.put(Constants.UVS, uvs);			
		} 
		catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving getSocialAnalysis.", e);
		}
		log.info("Retrieving getSocialAnalysis; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	
	public Map<String,Object>getDataForSocialTotal(WisdomQuery query) {
		Map<String, Object> finalResultMap= new  LinkedHashMap<>();
		Map<String, Object> currentResultMap = new LinkedHashMap<>();
		Map<String, Object> prevResultMap = new LinkedHashMap<>();
		
		String startDate = DateUtil.getCurrentDateTime();
		//String endDate = DateUtil.getCurrentDateTime();
		
		if(query.getStartDate()!=null) {
		 startDate = query.getStartDate();
		}		
		/*if(query.getEndDate()!=null) {
		 endDate = query.getEndDate();
		}*/	
		
		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, Arrays.asList(521,3849)));
		bqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(startDate).to(startDate));		
		
		SearchResponse res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb)
				.addAggregation(AggregationBuilders.sum("total_shares").field(Constants.SHARES))
				.addAggregation(AggregationBuilders.sum("total_reach").field(Constants.UNIQUE_REACH))
				.addAggregation(AggregationBuilders.sum("overall_reach").field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum("total_link_clicks").field(Constants.LINK_CLICKS))
				.addAggregation(AggregationBuilders.filter("links",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"))
						.subAggregation(AggregationBuilders.sum("total_link_shares").field(Constants.SHARES))						
						.subAggregation(AggregationBuilders.sum("total_link_reach").field(Constants.UNIQUE_REACH)))
				.setSize(0).execute().actionGet();
		
		Sum ts = res.getAggregations().get("total_shares");
		Long total_shares = (long) ts.getValue();
		currentResultMap.put("total_shares", total_shares);
		
		Sum tr = res.getAggregations().get("total_reach");
		Long total_reach=(long) tr.getValue();
		currentResultMap.put("total_reach", total_reach);

		Sum tlc = res.getAggregations().get("total_link_clicks");
		Double  t_clicks=(double) tlc.getValue();
		
		Sum overallreach = res.getAggregations().get("overall_reach");
		Long ovreach=(long) overallreach.getValue();
		
		double ctr =  ((t_clicks/ovreach)*100);		
		currentResultMap.put("CTR",ctr);
		
		Filter linkData=res.getAggregations().get("links");		
		Sum tls = linkData.getAggregations().get("total_link_shares");
		Long total_link_shares = (long) tls.getValue();
		currentResultMap.put("link_shares", total_link_shares);
		
		Sum tlr = linkData.getAggregations().get("total_link_reach");
		Long total_link_reach = (long) tlr.getValue();
		currentResultMap.put("link_reach", total_link_reach);
		
		
		BoolQueryBuilder boolq = new BoolQueryBuilder();
		boolq.must(QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"));
		boolq.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, Arrays.asList(521,3849)));
		boolq.must(QueryBuilders.rangeQuery(Constants.CREATED_DATETIME).from(startDate).to(startDate));		
		
		SearchResponse ctr_res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(boolq)				
				.addAggregation(AggregationBuilders.sum("overall_reach").field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum("total_link_clicks").field(Constants.LINK_CLICKS))				
				.setSize(0).execute().actionGet();
		
		Sum ctr_tlc = ctr_res.getAggregations().get("total_link_clicks");
		Double  ctr_t_clicks=(double) ctr_tlc.getValue();
		
		Sum ctr_overallreach = ctr_res.getAggregations().get("overall_reach");
		Long ctr_ovreach=(long) ctr_overallreach.getValue();
		
		double posted_date_ctr=0;
		if(!(ctr_t_clicks==0) && !(ctr_ovreach==0)) {
		posted_date_ctr =  ((ctr_t_clicks/ctr_ovreach)*100);	
		}
		else {
			ctr_t_clicks=(double) 0;
			ctr_ovreach=(long) 0;
		}
		currentResultMap.put("posted_date_CTR", posted_date_ctr);
		
		
		BoolQueryBuilder prevbqb = new BoolQueryBuilder();
		prevbqb.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, Arrays.asList(521,3849)));
		prevbqb.must(QueryBuilders.rangeQuery(Constants.DATE_TIME_FIELD).from(DateUtil.getPreviousDate(startDate.replaceAll("-", "_"), -7).replaceAll("_", "-"))
				.to(DateUtil.getPreviousDate(startDate.replaceAll("-", "_"), -7).replaceAll("_", "-")));
	
		SearchResponse prev_res = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(prevbqb)
				.addAggregation(AggregationBuilders.sum("prev_total_shares").field(Constants.SHARES))
				.addAggregation(AggregationBuilders.sum("prev_total_reach").field(Constants.UNIQUE_REACH))
				.addAggregation(AggregationBuilders.sum("prev_overall_reach").field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum("prev_total_link_clicks").field(Constants.LINK_CLICKS))
				.addAggregation(AggregationBuilders.filter("prev_links",QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"))
						.subAggregation(AggregationBuilders.sum("total_prev_link_shares").field(Constants.SHARES))
						.subAggregation(AggregationBuilders.sum("total_prev_link_reach").field(Constants.UNIQUE_REACH)))
				.setSize(0).execute().actionGet();
		
		
		Sum ts_prev = prev_res.getAggregations().get("prev_total_shares");
		Long total_prev_shares = (long) ts_prev.getValue();
		prevResultMap.put("total_shares", total_prev_shares);
		
		Sum tr_prev = prev_res.getAggregations().get("prev_total_reach");
		Long total_prev_reach=(long) tr_prev.getValue();
		prevResultMap.put("total_reach", total_prev_reach);

		
		Sum prevtlc = prev_res.getAggregations().get("prev_total_link_clicks");
		Double  prev_clicks=(double) prevtlc.getValue();
		
		Sum prev_overallreach = prev_res.getAggregations().get("prev_overall_reach");
		Long prev_ovreach=(long) prev_overallreach.getValue();
		
		double prev_ctr =  ((prev_clicks/prev_ovreach)*100);		
		prevResultMap.put("CTR",prev_ctr);
		
		Filter prev_linkData = prev_res.getAggregations().get("prev_links");
		
		Sum tls_prev = prev_linkData.getAggregations().get("total_prev_link_shares");
		Long total_prev_link_shares = (long) tls_prev.getValue();		
		prevResultMap.put("link_shares", total_prev_link_shares);
		
		Sum tlr_prev = prev_linkData.getAggregations().get("total_prev_link_reach");
		Long total_prev_link_reach = (long) tlr_prev.getValue();		
		prevResultMap.put("link_reach", total_prev_link_reach);
		
		BoolQueryBuilder prevboolquery = new BoolQueryBuilder();
		prevboolquery.must(QueryBuilders.termsQuery(Constants.STATUS_TYPE, "link"));
		prevboolquery.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, Arrays.asList(521,3849)));
		prevboolquery.must(QueryBuilders.rangeQuery(Constants.CREATED_DATETIME).from(DateUtil.getPreviousDate(startDate.replaceAll("-", "_"), -7).replaceAll("_", "-"))
				.to(DateUtil.getPreviousDate(startDate.replaceAll("-", "_"), -7).replaceAll("_", "-")));
	
		SearchResponse prev_res_ctr = client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(prevboolquery)				
				.addAggregation(AggregationBuilders.sum("prev_overall_reach").field(Constants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum("prev_total_link_clicks").field(Constants.LINK_CLICKS))				
				.setSize(0).execute().actionGet();
		
		Sum prevtlc_ctr = prev_res_ctr.getAggregations().get("prev_total_link_clicks");
		Double  prev_clicks_ctr=(double) prevtlc_ctr.getValue();
		
		Sum prev_overallreach_ctr = prev_res_ctr.getAggregations().get("prev_overall_reach");
		Long prev_ovreach_ctr=(long) prev_overallreach_ctr.getValue();
		
		double prev_posted_date_ctr =  ((prev_clicks_ctr/prev_ovreach_ctr)*100);		
		prevResultMap.put("posted_date_CTR",prev_posted_date_ctr);
		
		finalResultMap.put("total_sessions", getTotalSocialSessions(query));
		finalResultMap.put("current_data", currentResultMap);
		finalResultMap.put("previous_data", prevResultMap);		
		return finalResultMap;
		}
	
	
	private Map<String, Object> getTotalSocialSessions(WisdomQuery query) {
		Map<String, Object> total_session_count = new HashMap<>();
		try {
			String startDatetime = query.getStartDate();
			String endDatetime = startDatetime;
			List<String> trackersList = Arrays.asList("instant","fbo","fbo1","fbo10","money","Fbo20","Fbo21","Fbo22","Fbo23","Fbo24",
				    "Fbo25","Fbo26","Fbo27","ABC", "bgp","fbina","fpaamn", "fpaid","fpamn", "fpamn1", "fpamn6", "gpromo","Gr",
				    "NJB","NJNM", "NJP","NJP1", "NJS", "nmp","opd","Vpaid","xyz", "mnp","mini");
			List<Integer> channels = Arrays.asList(521,3849,3322);
			BoolQueryBuilder totalSession = new BoolQueryBuilder();
			totalSession.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, channels));
			totalSession.must(QueryBuilders.rangeQuery(query.getDateField()).gte(startDatetime).lte(startDatetime));			
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : trackersList) {
					trackersBoolQuery.should(QueryBuilders.termsQuery(Constants.TRACKER_SIMPLE, tracker));
				}
				totalSession.must(trackersBoolQuery);
			SearchResponse sr = client
					.prepareSearch(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, startDatetime, startDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(totalSession)
					.addAggregation(AggregationBuilders.sum(Constants.UPVS).field(Constants.UVS)).setSize(0).execute()
					.actionGet();

			Sum total_session = sr.getAggregations().get(Constants.UPVS);
			long t_sessions = (long) total_session.getValue();
			total_session_count.put("total_session", t_sessions);
			Long top20StoriesCount=0L;
			query.setChannel_slno(channels);
			query.setStartDate(startDatetime);
			query.setEndDate(endDatetime);
			query.setCount(100);	
			query.setDateField("datetime");
			query.setStatus_type("link");
			query.setTracker(trackersList);						
			
			FacebookInsightsPage records  = getFacebookInsightsForDate(query);			
			List<FacebookInsights> stories  = records.getStories();		
			ArrayList<String>bhaskarStoryIds = new ArrayList<>();
			
			List<FacebookInsights> first20StoriesList = stories.stream().limit(20).collect(Collectors.toList());			
			for(FacebookInsights single_stories:first20StoriesList) {
				bhaskarStoryIds.add(single_stories.getBhaskarstoryid());				
				top20StoriesCount+= single_stories.getSessions();	
			}
			total_session_count.put("top_20_session_count", top20StoriesCount);	
			total_session_count.put("Videos_Data", getVideoData(startDatetime,startDatetime,bhaskarStoryIds.toString().replaceAll("\\[|\\]", ""),"521"));
			total_session_count.put("top_20_Stroies", bhaskarStoryIds);
			//getVideoData(startDatetime,startDatetime,bhaskarStoryIds.toString().replaceAll("\\[|\\]", ""),"521");
			
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving sessions", e);
		}
		return total_session_count;
	}
	
	void getMySSQLCOnnection(String ip, String port, String user, String pass, String database) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			log.error(e);
		}
		Connection con = null;
		try {
			con = DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + database, user, pass);
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		//System.out.println("connection created with MySql");
		connection = con;
	}	

	Map<String,Object> getVideoData(String startDatetime, String endDatetime, String bhaskarStoryIds, String channel) {		
		Map<String,Object> videoMap = new HashMap<>();
		//String res = null;
		String total_video_views = "";
		String top_20_views = "";
		if (connection == null)
			getMySSQLCOnnection(ip, "3306", "vid_analytics", "vid_analytics", "video_analytics");

		CallableStatement st = null;
		try {
			st = connection.prepareCall("{call video_views_top20(?,?,?)}");
			st.setString(1, startDatetime);
			st.setString(2, endDatetime);
			//st.setString(3, bhaskarStoryIds);
			st.setString(3, channel);

			ResultSet resultSet = st.executeQuery();

			while (resultSet.next()) {
				total_video_views = resultSet.getString(1);
				top_20_views = resultSet.getString(2);
			}
			videoMap.put("total_VV", total_video_views);
			videoMap.put("top20_VV", top_20_views);
		} catch (SQLException e) {
			getMySSQLCOnnection(ip, "3306", "vid_analytics", "vid_analytics", "video_analytics");
			videoMap = getVideoData(startDatetime,endDatetime,bhaskarStoryIds,channel);
			e.printStackTrace();
		}
		return videoMap;

	}
	public static void main(String[] args) throws Exception {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		WisdomQueryExecutorService wqes = new WisdomQueryExecutorService();
		WisdomQuery query = new WisdomQuery();

		ArrayList<Integer> channel_slno = new ArrayList<>();
		channel_slno.add(521);
		channel_slno.add(3849);
		//channel_slno.add(960);
		//channel_slno.add(3850);
		//channel_slno.add(4371);
		//channel_slno.add(4444);
		//channel_slno.add(5483);
		//channel_slno.add(10401);
		//channel_slno.add(10402);
		//channel_slno.add(3322);
		//channel_slno.add(3849);
		ArrayList<String> host_type = new ArrayList<>();
		host_type.add("m");
		host_type.add("w");
		List<String> trackers = new ArrayList<>();
		//trackers.add("dbnotify");
		trackers = Arrays.asList("instant","fbo","fbo1","fbo10","money","Fbo20","Fbo21","Fbo22","Fbo23","Fbo24","Fbo25","Fbo26","Fbo27","ABC","bgp","fbina","fpaamn","fpaid","fpamn","fpamn1","fpamn6","gpromo","Gr","NJB","NJNM","NJP","NJP1","NJS","nmp","opd","Vpaid","xyz","mnp","mini");
		
		//competitors.add("news-trending-instant2");
		//competitors.add("news-trending-instant3");
		//competitors.add("news-trending-instant4");
		//ArrayList<String> host_type = new ArrayList<>();
		List<String> excludeTracker = new ArrayList<>();
		ArrayList<Integer> super_cat_id = new ArrayList<>();
		super_cat_id.add(1);
		ArrayList<String> ga_cat_name = new ArrayList<>();
		ga_cat_name.add("Infotainment");
		ArrayList<String> profileId = new ArrayList<>();
		profileId.add("294260637288");
		profileId.add("197812740349394");
		ArrayList<String> uid = new ArrayList<>();
		uid.add("497");


		ArrayList<Integer> host = new ArrayList<>();
		//host.add(521);
		//host.add(5);
		host.add(1);
		host.add(2);
		host.add(3);
		host.add(4);
		host.add(48);
		//host.add(21);
		List<String> competitors = Arrays.asList("aajtak","abpnews","dainikjagran","dainikbhaskar","TimesofIndia","ZeeNewsHindi","IndiaToday","Amarujala","ndtv","indianexpress","hindustantimes","thehindu","scroll.in","TheWireHindi","cnnnews18");
		ArrayList<String> keywords = new ArrayList<>();
		keywords.add("Congress");
		//keywords.add("ipl");
	  	/*String keywordArr[] ="Abdul Nasser Mahdani, Administrative Reforms Commission, admiral, Advanced Light Helicopter, Advanced Numerical Research & Analysis Group, Aerial Delivery Research & Development Establishment, Aero India, Aeronautical Development Establishment, AFNET, Agni, agreement, Ahmadiya, AIADMK, AIDMK, AIDUF, AIMIM, Air Force, aircraft, aircraft carrier, Ajit Doval, AK-47, Akali Dal, Akash Missile, Akbarudin Owaisi, Akhil Bharatiya Vidyarthi Parishad, Akhilesh Yadav, Al Badr, Alhe Hadees, All Assam Students Union, ambush, Amit Shah, ammunition, AQIS, ARDE, Arjun Tank, Armament Research & Development Establishment, armored, armoured, army, arson, artillery, Arun Jaitley, Assadudin Owaisi, Assam Rifles, assassin, ATTF, Aviation Research Centre, Azam Khan, Babbar Khalsa, Babbar Khalsa International, Bajrang Dal, Bangladesh, BDL, BEL, BEML, Bharat Dynamics, Bharat Electronics, Bharatiya Janata Party, bilateral cooperation, bilateral relations, BJP, BKI, blast, Bodo, bomb, border, Border Roads Organisation, BrahMos, Brigadier, BSF, budget, business, Cabinet, Cabinet Secretariat, cease-fire, ceasefire, CEMILAC, CENJOWS, Center for Artificial Intelligence & Robotics, Center for Fire, Center for Military Airworthiness & Certification, Centre for Air Borne Systems, Centre for Artificial Intelligence and Robotics, CFEES, chamber of commerce, chopper, CISF, clashes, claymore, CoAS, Coast Guard, cocaine, Colonel, combat, Combat Vehicles Research & Development Establishment, commander, commando, communal, confrontation, Congress party, conversion, conversions, Cooperation, CORPAT, coup, CPI, CPI(M), CPI-M, cross-border, CRPF, CSIR, curfew, CVRDE, Daesh, DBT, DEBEL, Defence, Defence Intelligence Agency, Defexpo, Deonband, Department of Agricultural Research and Education, Department of Atomic Energy, DESIDOC, Devendra Fadnavis, DFRL, Dharam Jagaran Samanvay Samiti, diplomacy, diplomatic, DIPR, Directorate of Air Intelligence, Directorate of Navy Intelligence, disinvestment, DLJ, DLRL, DMK, DMRL, DMSRDE, DRDE, DRDL, DRDO, DRI, DRL, drought, drug, drugs, DST, DTRL, Durga Vahini, eavesdrop, eavesdropping, Economic Agreement, Economic cooperation, economy, election, elections, electricity, Electronics & Radar Development Establishment, energy, explosion, Explosive and Environment Safety, explosives, extremist, fatwa, fauj, fauji, FDI, FICCI, FII, FIIs, Finance, firing, fiscal policy, Forces, foreign aid, Foreign Minister, Foreign Ministry, Foreign Office, Foreign Secretary, frigate, Gas Turbine Research Establishment, GDP, governor, grenade, grenades, GTRE, HAL, Hamid Ansari, hawala, HAWS, Helina, HEMRL, heroin, High Energy Materials Research Laboratory, Hindu Dharma Sena, Hindu Janajagruti Samiti, Hindu Mahasabha, Hindu Makkal Katchi, Hindu Rashtra Sena, Hindustan Aeronautics, Hindustan Shipyard, Hindutva, Hizb, Hizbul, HNLC, HSL, Hurriyat, IBSAMAR, IMF, Improvised Explosive Device, incursion, Indian Mujahidin, Indian National Congress, INDRA, infiltration, inflation, INMAS, INS, Institute of Nuclear Medicine & Allied Sciences, Institute of Systems Studies & Analysis, Institute of Technology Management, Instruments Research & Development Establishment, insurgent, Integrated Test Range, Intelligence Bureau, internal security, internally displaced, international border, IRDE, ISIL, ISIS, Islamic State, ISRO, ISSA, ITBP, ITM, ITR, Jaish, Jaitapur, Jamaat, Jamaat-e-Islami, Jamaat-Islami Hind, Jamiatul, Janata Dal, Javdekar, jawan, Jayalalitha, JeM, JKLF, Joint Cipher Bureau, Kalashnikov, Kamtapur, Kangleipak, KCF, KCP, Khalistan, Khalistan Commando Force, Khalistan Zindabad Force, Khaplang, KLO, KOMODO, Kudankulam, KYKL, KZF, LAC, landmine, Laser Science & Technology Centre, Lashkar, LASTEC, LCA, LDF, lieutenant, Line of actual control, Line of Actual Control, line of control, LoC, looting, love jihad, LRDE, M&A, Make in India, Malik, Mamata Banerjee, Mann ki baat, Maoist, Maoists, MARCOS, Masarat Alam, mashawarat, MEA, media, memorandum, Mergers & Acquisitions, Mergers and Acquisitions, MIDHANI, MIG, MiG-29, migrants, militant, military, Military, militia, mine sweeper, mined, Minister of External Affairs, Mirage, Mirage 2000, Mirwaiz, missile, missionary, Mitra Shakti, Mnohar Parrikar, monetary policy, mortar, MoU, MPLF, MTRDC, Mufti, Muivah, mujahid, Mujahideen, Mulayam Singh Yadav, Muslimeen, narcotic, narcotics, Narendra Modi, Narora, National Conference, National Congress, National Security Advisor, National Technical Research Organization, Naval Materials Research Laboratory, Naval Physical & Oceanographic Laboratory, Naval Science & Technological Laboratory, Navy, Naxal, naxalite, NCB, NDFB, NDRF, neighborly, neighbourly, Nepal, Nepali, Nirbhay, NIRDESH, Nirmala, Nitish Kumar, NLFT, NMRL, NPCIL, NPOL, NSA, NSCN, NSG, NSTL, NTRO, nuclear energy, nuclear power, nuclear weapon, nuke, nukes, OFB, OGW, opium, ordnance, Ordnance Factory Board, pact, para regiment, paratrooper, Parkash Singh Badal, patrolling, PDP, Peace Council, PFI, PLA, poll, polls, pollution, Poppy, Popular Front of India, Pranab Mukherjee, PREPAK, price hike, Prime Minister, protest, public health, PXE, qaeda, qaida, R&AW, R&DE, radical, Radicalisation, Rafale, Raghuram Rajan, Rahul Gandhi, Rajnath Singh, Raksha Mantri, Rashtrawadi Shiv Sena, RCI, rebel, rebels, reconnaissance, refugee, Research & Development Establishment, Research and Analysis Wing, Research Center Imarat, Reserve Bank of India, right-wing, riot, Rohingya, RPF, RPG, RSS, SAARC, SAG, Sakshi, Salafi, Salahuddin, Samajwadi Party, Sampriti, SAMPRITI, Sangh, SASE, Scientific Analysis Group, SDPI, SEBI, secession, secessionist, security, Separatist, Shah, Sharia, shelling, Shia, Shiv Sena, shoulder fired, shura council, Siddaramaiah, Signals Intelligence Directorate, SIMBEX, smuggle, SNDP, sniper, snooping, Snow & Avalanche Study Establishment, solar power, soldier, Solid State Physics Laboratory, special forces, SPYDER, Sri Ram Sene, SSB, SSPL, stone pelting, Student Islamic Movement of India, Student Islamic Organisation, Students Islamic Organization, Submarine, Sukhbir Singh Badal, Sukhoi, Sunni, Sushma Swaraj, Swadeshi Jagaran Manch, Syed Ahmed Bukhari, Syed Ali Shah Geelani, Tahrik, Taleban, Taliban, TBRL, Tehreeq, Tejas, telecom, Telecom Regulatory Authority of India, telecommunication, Terminal Ballistics Research Laboratory, terror, terrorist, Terrorist, Thackrey, trade, TRAI, treaty, Tri-Services, Trinamool, troop, trooper, troops, UAV, UJC, ulema council, United Democratic Front, United Liberated Front of Assam, United Nations, UNLF, vandalism, Vastanvi, Vehicle Research & Development Establishment, violence, virus, Vishwa Hindu Parishad, vote, VRDE, warfare, wind power, WMD, World Trade Organization, WTO".split(",");
		for(String keyword:keywordArr)
		{
			keywords.add(keyword);
		}*/
	  	//query.setKeywords(keywords);
		//competitors.add("scroll.in");
		//ga_cat_name.add("Bollywood");
		//ga_cat_name.add("Celebs");


		//		query.setStartDate("2018-04-01");
		//	query.setEndDate("2018-04-5");		
		//query.setCount(20);		
		//query.setChannel_slno(channel_slno);
		//query.setInterval("month");
		//query.setDateField("datetime");
		//query.setCompetitor(competitors);
		//query.setProfile_id(profileId);
		//query.setGa_cat_name(ga_cat_name);
		//getMajorSectionReportDataquery.setHost(host);
		//query.setCount(20);
		//query.setTracker(trackers);
		// query.setScore(11);
		//query.setStartPubDate("2018-04-01");
		//query.setEndPubDate("2018-04-30");
		//query.setCompareStartDate("2018-04-24T00:00:00Z");
		//query.setCompareEndDate("2018-04-24T10:43:56Z");
		//query.setPrevDayRequired(true);
		//query.isOrderAsc();

		//excludeTracker = Arrays.asList("news-m-ucb","news-ucb","news-m-ucb_1","news-ucb_1");
		//query.setTracker(trackers);
		//channel_slno.add(521);
		//query.setWeekDay("Tue");
		query.setStartDate("2018-06-27");	
	    //query.setEndDate("2018-06-27");
	    //query.setDateField("datetime");	    
		//query.setOrderAsc(false);
		//query.setCategory_type("ga_cat_name");
		//query.setUid(uid);
		//query.setCat_id(channel_slno);
		//query.setDomaintype("channel_slno");
		//query.setHost(host);		
		//query.setStatus_type("link");
		//query.setSuper_cat_id(super_cat_id);
		//query.setIdentifierType("super_cat_id");
		//query.setIdentifierValue("4");
		//query.setDate("2018-03-14");
		//query.setProfile_id(Arrays.asList("294260637288", "197812740349394"));
		//query.setPrediction("shares");
		//query.setStoryid("759078590776834_2285559798128698");		
		//query.setCategorySize(5);
		//query.setSim_date_count(2);
		//query.setParameter("total_engagement");
		//query.setSuper_cat_name(ga_cat_name);
		//query.setType(0);
		//query.setFilter("withoutUc");
		//query.setSession_type(SessionTypeConstants.AUTHOR);
		//query.setHost_type(host_type);
		//query.setIdentifierValue(Arrays.asList("channelOverall"));
		//query.setMinutes("15");
		//query.setExcludeTracker(excludeTracker);
		//query.setInterval("1h");
		//query.setCategory_type("super_cat_name");
		//query.setPrediction("engagement");
		//query.setWidget_name("HP_NEW");
		//query.setTitle("PM Narendra Modi brings in the mother of all scholarships, offers bright PhD students Rs 80,000/month");
		//AggregateReportKey gaQuery = new AggregateReportKey("521", "31", "4", "super_cat_id", "2018-03-14");
		//query.setFilter(Constants.WITHOUT_UC);
		
		System.out.println(" " + gson.toJson(wqes.getDataForSocialTotal(query)));
	}

}
