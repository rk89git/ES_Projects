package com.db.wisdom.product.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Indexes;
import com.db.common.constants.Indexes.WisdomIndexes;
import com.db.common.constants.MappingTypes;
import com.db.common.constants.WisdomConstants;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.wisdom.product.model.FacebookInsights;
import com.db.wisdom.product.model.FacebookInsightsPage;
import com.db.wisdom.product.model.SlideDetail;
import com.db.wisdom.product.model.SourcePerformance;
import com.db.wisdom.product.model.StoryDetail;
import com.db.wisdom.product.model.StoryPerformance;
import com.db.wisdom.product.model.Timeline;
import com.db.wisdom.product.model.TopAuthorsResponse;
import com.db.wisdom.product.model.WisdomPage;
import com.db.wisdom.product.model.WisdomProductQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WisdomProductQueryExecutorService {

	private Client client = null;

	private static Logger log = LogManager.getLogger(WisdomProductQueryExecutorService.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
	
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public WisdomProductQueryExecutorService() {
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
	
	public WisdomPage getTopStoryListing(WisdomProductQuery query) {
		long start = System.currentTimeMillis();
		List<StoryDetail> stories = new ArrayList<>();
		WisdomPage page = new WisdomPage();
		stories = getStoriesList(query);
		page = getTopStoriesListingTopBar(query);
		page.setStories(stories);
		log.info(
				"Total execution time for Top Story Listing (Seconds): " + (System.currentTimeMillis() - start) / 1000);
		return page;
	}

	private WisdomPage getTopStoriesListingTopBar(WisdomProductQuery query) {
		Boolean prevDay = false;
		long startTime = System.currentTimeMillis();
		String histogramField = WisdomConstants.DATETIME;
		BoolQueryBuilder qb = new BoolQueryBuilder();
		WisdomPage page = new WisdomPage();
		long mpvs = 0;
		long wpvs = 0;
		long apvs = 0;
		long ipvs = 0;
		Long tpvs = 0L;
		long mupvs = 0;
		long wupvs = 0;
		long aupvs = 0;
		long iupvs = 0;
		Long tupvs = 0L;
		Long socialTraffic = 0L;
		Long storyCount = 0L;
		BoolQueryBuilder bqb = new BoolQueryBuilder();
		String[] indexName = new String[2];
		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
		}
		else if(query.getStartDate() != null && query.getEndDate() != null){
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, query.getStartDate(), query.getEndDate());
		}
		else{
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate());			
		}
		/**
		 * If input query has filter based on publish date then set histogram
		 * field to story_pubtime and set datetime enable false
		 */
		if (query.getStartPubDate() != null) {
			histogramField = WisdomConstants.PUBLISHED_DATE;
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
							.should(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE)
									.gte(DateUtil.getPreviousDate().replaceAll("_", "-"))
									.lte(DateUtil.addHoursToTime(DateUtil.getCurrentDateTime(), -24)))
							.should(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE)
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
					SearchResponse prevDayRes = client.prepareSearch(IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL_HOURLY, DateUtil.getPreviousDate(), DateUtil.getCurrentDate()))
							.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(bqb)
							.addAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(WisdomConstants.HTTP_REFERER, query.getSocialReferer()))
									.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS)))
							.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
							.addAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
									.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
							.addAggregation(AggregationBuilders.cardinality("storyCount").field(WisdomConstants.STORY_ID))
							.setSize(0).execute().actionGet();
					Filter filter = prevDayRes.getAggregations().get("socialTraffic");
					Sum socialTrafficAgg = filter.getAggregations().get("tpvs");
					socialTraffic = new Double(socialTrafficAgg.getValue()).longValue();
					Sum totalPvs = prevDayRes.getAggregations().get("tpvs");
					tpvs = Double.valueOf(totalPvs.getValue()).longValue();
					Sum totalupvs = prevDayRes.getAggregations().get("tupvs");
					tupvs = Double.valueOf(totalupvs.getValue()).longValue();
					Cardinality storyCountAggregation = prevDayRes.getAggregations().get("storyCount");
					// Max slide = bucket.getAggregations().get("slide");
					// slideCount = new Double(slide.getValue()).intValue();
					Terms hostType = prevDayRes.getAggregations().get("platform");
					for (Terms.Bucket host : hostType.getBuckets()) {
						Sum pvs = host.getAggregations().get("pvs");
						Sum upvs = host.getAggregations().get("upvs");
						if (host.getKey().equals(WisdomConstants.MOBILE)) {
							mpvs = new Double(pvs.getValue()).longValue();
							mupvs = new Double(upvs.getValue()).longValue();
						} else if (host.getKey().equals(WisdomConstants.WEB)) {
							wpvs = new Double(pvs.getValue()).longValue();
							wupvs = new Double(upvs.getValue()).longValue();
						} else if (host.getKey().equals(WisdomConstants.ANDROID)) {
							apvs = new Double(pvs.getValue()).longValue();
							aupvs = new Double(upvs.getValue()).longValue();
						} else if (host.getKey().equals(WisdomConstants.IPHONE)) {
							ipvs = new Double(pvs.getValue()).longValue();
							iupvs = new Double(upvs.getValue()).longValue();
						}
					}
					page.setPreviousDayPvs(tpvs);
					page.setPreviousDayStoryCount(new Long(storyCountAggregation.getValue()));
					page.setPreviousDayMPvs(mpvs);
					page.setPreviousDayWPvs(wpvs);
					page.setPreviousDayAPvs(apvs);
					page.setPreviousDayIPvs(ipvs);
					page.setPreviousDayUpvs(tupvs);
					page.setPreviousDayMUpvs(mupvs);
					page.setPreviousDayWUpvs(wupvs);
					page.setPreviousDayAUpvs(aupvs);
					page.setPreviousDayIUpvs(iupvs);
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
							.subAggregation(AggregationBuilders.filter("socialTraffic",QueryBuilders.termsQuery(WisdomConstants.HTTP_REFERER, query.getSocialReferer()))
									.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS)))
							.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
							.subAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS)).subAggregation(
											AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
							.subAggregation(
									AggregationBuilders.cardinality("storyCount").field(WisdomConstants.STORY_ID)))
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
						mupvs = 0;
						wupvs = 0;
						aupvs = 0;
						iupvs = 0;
						tupvs = 0L;
						socialTraffic = 0L;
						String bucketDate = bucket.getKeyAsString();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						Date date = sdf.parse(bucketDate);
						String date1 = sdf.format(date);
						Filter filter = bucket.getAggregations().get("socialTraffic");
						Sum socialTrafficAgg = filter.getAggregations().get("tpvs");
						socialTraffic = new Double(socialTrafficAgg.getValue()).longValue();
						Sum totalPvs = bucket.getAggregations().get("tpvs");
						tpvs = Double.valueOf(totalPvs.getValue()).longValue();
						Sum totalupvs = bucket.getAggregations().get("tupvs");
						tupvs = Double.valueOf(totalupvs.getValue()).longValue();
						Cardinality storyCountAggregation = bucket.getAggregations().get("storyCount");
						Terms hostType = bucket.getAggregations().get("platform");
						for (Terms.Bucket host : hostType.getBuckets()) {
							Sum pvs = host.getAggregations().get("pvs");
							Sum upvs = host.getAggregations().get("upvs");
							if (host.getKey().equals(WisdomConstants.MOBILE)) {
								mpvs = new Double(pvs.getValue()).longValue();
								mupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.WEB)) {
								wpvs = new Double(pvs.getValue()).longValue();
								wupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.ANDROID)) {
								apvs = new Double(pvs.getValue()).longValue();
								aupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.IPHONE)) {
								ipvs = new Double(pvs.getValue()).longValue();
								iupvs = new Double(upvs.getValue()).longValue();
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
							page.setCurrentDayUpvs(tupvs);
							page.setCurrentDayMUpvs(mupvs);
							page.setCurrentDayWUpvs(wupvs);
							page.setCurrentDayAUpvs(aupvs);
							page.setCurrentDayIUpvs(iupvs);
						} else {
							page.setPreviousDayPvs(tpvs);
							page.setPreviousDayStoryCount(new Long(storyCountAggregation.getValue()));
							page.setPreviousDayMPvs(mpvs);
							page.setPreviousDayWPvs(wpvs);
							page.setPreviousDayAPvs(apvs);
							page.setPreviousDayIPvs(ipvs);
							page.setPreviousDayUpvs(tupvs);
							page.setPreviousDayMUpvs(mupvs);
							page.setPreviousDayWUpvs(wupvs);
							page.setPreviousDayAUpvs(aupvs);
							page.setPreviousDayIUpvs(iupvs);
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
						socialTraffic += new Double(socialTrafficAgg.getValue()).longValue();
						Sum totalPvs = bucket.getAggregations().get("tpvs");
						tpvs += Double.valueOf(totalPvs.getValue()).longValue();
						Sum totalupvs = bucket.getAggregations().get("tupvs");
						tupvs += Double.valueOf(totalupvs.getValue()).longValue();
						Cardinality storyCountAggregation = bucket.getAggregations().get("storyCount");
						storyCount += new Long(storyCountAggregation.getValue());
						Terms hostType = bucket.getAggregations().get("platform");
						for (Terms.Bucket host : hostType.getBuckets()) {
							Sum pvs = host.getAggregations().get("pvs");
							Sum upvs = host.getAggregations().get("upvs");
							if (host.getKey().equals(WisdomConstants.MOBILE)) {
								mpvs = new Double(pvs.getValue()).longValue();
								mupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.WEB)) {
								wpvs = new Double(pvs.getValue()).longValue();
								wupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.ANDROID)) {
								apvs = new Double(pvs.getValue()).longValue();
								aupvs = new Double(upvs.getValue()).longValue();
							} else if (host.getKey().equals(WisdomConstants.IPHONE)) {
								ipvs = new Double(pvs.getValue()).longValue();
								iupvs = new Double(upvs.getValue()).longValue();
							}
						}
						page.setCurrentDayPvs(tpvs);
						page.setCurrentDayStoryCount(storyCount);
						page.setCurrentDayMPvs(mpvs);
						page.setCurrentDayWPvs(wpvs);
						page.setCurrentDayAPvs(apvs);
						page.setCurrentDayIPvs(ipvs);
						page.setCurrentDaySocialTraffic(socialTraffic);
						page.setCurrentDayUpvs(tupvs);
						page.setCurrentDayMUpvs(mupvs);
						page.setCurrentDayWUpvs(wupvs);
						page.setCurrentDayAUpvs(aupvs);
						page.setCurrentDayIUpvs(iupvs);
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
	}

	private List<StoryDetail> getStoriesList(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		List<StoryDetail> records = new ArrayList<>();

		String includes[] = { WisdomConstants.PUBLISHED_DATE, WisdomConstants.AUTHOR, WisdomConstants.TITLE, WisdomConstants.ARTICLE_IMAGE,
				WisdomConstants.URL, WisdomConstants.AUTHOR_ID, WisdomConstants.DOMAIN_ID, WisdomConstants.SLIDES };
		String[] indexName = new String[2];
		if(StringUtils.isNotBlank(query.getStoryid())){
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate());
		}		
		else if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, query.getStartPubDate(), query.getEndPubDate());
		}
		else{
			indexName = IndexUtils.getMonthlyIndexes(WisdomIndexes.WISDOM_STORY_DETAIL, query.getStartDate(), query.getEndDate());
		}
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(getQuery(query))
				.addAggregation(AggregationBuilders.terms("stories").field(WisdomConstants.STORY_ID)
						.size(query.getCount()).order(Order.aggregation("tupvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
						.subAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS)).subAggregation(
										AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
						.subAggregation(AggregationBuilders.terms("version").field(WisdomConstants.MODIFIED_DATE).order(Order.term(false)).size(1)
								.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {}).size(1)))
						.subAggregation(AggregationBuilders.terms("sourceTraffic").field(WisdomConstants.HTTP_REFERER)
								.size(query.getElementCount()).order(Order.aggregation("pvs", false)).subAggregation(
										AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
								.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
						.subAggregation(AggregationBuilders.terms("pageViewsTracker").field(WisdomConstants.TRACKER).size(10)
								.order(Order.aggregation("pvs", false))
								.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
								.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS))))
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
				Long tupvs = 0L;
				long mupvs = 0;
				long wupvs = 0;
				long aupvs = 0;
				long iupvs = 0;
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
				long siteupvs = 0;
				long uvContribution = 0;

				String storyid = story.getKeyAsString();
				storyDetail.setStory_id(storyid);
				storyList.add(storyid);

				Sum totalPvs = story.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				storyDetail.setTotalpvs(tpvs);

				Sum totalupvs = story.getAggregations().get("tupvs");
				tupvs = Double.valueOf(totalupvs.getValue()).longValue();
				storyDetail.setTotalupvs(tupvs);

				Terms version = story.getAggregations().get("version");
				TopHits topHits = version.getBuckets().get(0).getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				storyDetail.setAuthor(source.get(WisdomConstants.AUTHOR).toString());
				storyDetail.setTitle(source.get(WisdomConstants.TITLE).toString());
				if (storyDetail.getTitle().contains("????")) {
					log.warn("Ignoring record because of invalid title. Title: " + storyDetail.getTitle());
					continue;
				}

				storyDetail.setPublished_date((String) source.get(WisdomConstants.PUBLISHED_DATE));
				storyDetail.setUrl((String) source.get(WisdomConstants.URL));
				storyDetail.setArticle_image((String) source.get(WisdomConstants.ARTICLE_IMAGE));
				if (source.get(WisdomConstants.AUTHOR_ID) instanceof Integer)
					storyDetail.setAuthor_id(((Integer) source.get(WisdomConstants.AUTHOR_ID)).toString());
				else
					storyDetail.setAuthor_id((String) source.get(WisdomConstants.AUTHOR_ID));
				if (source.get(WisdomConstants.DOMAIN_ID) instanceof Integer)
					storyDetail.setDomain_id(String.valueOf(source.get(WisdomConstants.DOMAIN_ID)));
				else
					storyDetail.setDomain_id((String) source.get(WisdomConstants.DOMAIN_ID));
				if (source.get(WisdomConstants.SLIDES) instanceof Integer)
					slideCount = (Integer) source.get(WisdomConstants.SLIDES);
				else if(StringUtils.isBlank((String)source.get(WisdomConstants.SLIDES))){
					slideCount = 0;
				}
				else 
					slideCount = Integer.valueOf((String) source.get(WisdomConstants.SLIDES));

				// To calculate pvContribution for story detail page calculate
				// total pvs of site upto current time
				// from the pub_date of that story

				if (query.isStoryDetail()) {
					SearchResponse totalRes = client.prepareSearch(indexName)
							.setTypes(MappingTypes.MAPPING_REALTIME)
							.setQuery(QueryBuilders.boolQuery()
									.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()))
									.must(QueryBuilders.rangeQuery(WisdomConstants.DATETIME).gte((String) source.get(WisdomConstants.PUBLISHED_DATE)).lte(
													DateUtil.getCurrentDateTime())))
							.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)).setSize(0).execute()
							.actionGet();

					Sum totalSitePvs = totalRes.getAggregations().get("tpvs");
					sitePvs = Double.valueOf(totalSitePvs.getValue()).longValue();
					Sum totalSiteupvs = totalRes.getAggregations().get("tupvs");
					siteupvs = Double.valueOf(totalSiteupvs.getValue()).longValue();
				}

				if (sitePvs != 0) {
					pvContribution = new Double((tpvs.doubleValue() / sitePvs) * 100).longValue();
				}

				if (siteupvs != 0) {
					uvContribution = new Double((tupvs.doubleValue() / siteupvs) * 100).longValue();
				}

				storyDetail.setPvContribution(pvContribution);
				storyDetail.setUvContribution(uvContribution);
				storyDetail.setSlides(slideCount);
				Terms hostType = story.getAggregations().get("platform");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum upvs = host.getAggregations().get("upvs");
					if (host.getKey().equals(WisdomConstants.MOBILE)) {
						mpvs = new Double(pvs.getValue()).longValue();
						mupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.WEB)) {
						wpvs = new Double(pvs.getValue()).longValue();
						wupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.ANDROID)) {
						apvs = new Double(pvs.getValue()).longValue();
						aupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.IPHONE)) {
						ipvs = new Double(pvs.getValue()).longValue();
						iupvs = new Double(upvs.getValue()).longValue();
					}
				}

				storyDetail.setMpvs(mpvs);
				storyDetail.setWpvs(wpvs);
				storyDetail.setApvs(apvs);
				storyDetail.setIpvs(ipvs);
				storyDetail.setMupvs(mupvs);
				storyDetail.setWupvs(wupvs);
				storyDetail.setAupvs(aupvs);
				storyDetail.setIupvs(iupvs);
				if (query.getPlatform().contains(WisdomConstants.WEB)) {
					tupvs = wupvs + mupvs;
				} else {
					tupvs = aupvs + iupvs;
				}

				storyDetail.setTotalupvs(tupvs);

				// formula for slide depth: (pv/(uv*slide))*100

				if (slideCount != 0) {
					if (wupvs != 0)
						wSlideDepth = (long) ((Long.valueOf(wpvs).doubleValue() / (wupvs * slideCount)) * 100);
					if (mupvs != 0)
						mSlideDepth = (long) ((Long.valueOf(mpvs).doubleValue() / (mupvs * slideCount)) * 100);
					if (aupvs != 0)
						aSlideDepth = (long) ((Long.valueOf(apvs).doubleValue() / (aupvs * slideCount)) * 100);
					if (iupvs != 0)
						iSlideDepth = (long) ((Long.valueOf(ipvs).doubleValue() / (iupvs * slideCount)) * 100);
					if (tupvs != 0)
						slideDepth = (long) ((Long.valueOf(tpvs).doubleValue() / (tupvs * slideCount)) * 100);
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
					Sum upvs = trafficS.getAggregations().get("upvs");
					map.put("pvs", new Double(pvs.getValue()).longValue());
					map.put("upvs", new Double(upvs.getValue()).longValue());
					sourceTraffic.put(trafficS.getKeyAsString(), map);
					if (trafficS.getKeyAsString().contains(WisdomConstants.FACEBOOK)) {
						storyDetail.setIsFacebook(Boolean.TRUE);
					}
				}
				Terms pvTracker = story.getAggregations().get("pageViewsTracker");
				for (Terms.Bucket tracker : pvTracker.getBuckets()) {

					Map<String, Long> map = new HashMap<>();
					if (pageViewsTracker.size() < query.getElementCount()) {
						Sum pvs = tracker.getAggregations().get("pvs");
						Sum upvs = tracker.getAggregations().get("upvs");
						map.put("pvs", new Double(pvs.getValue()).longValue());
						map.put("upvs", new Double(upvs.getValue()).longValue());
						pageViewsTracker.put(tracker.getKeyAsString(), map);
					}
					if (tracker.getKeyAsString().contains(WisdomConstants.UCB)) {
						storyDetail.setIsUCB(Boolean.TRUE);
					}
				}				
				storyDetail.setSourceWiseTraffic(sourceTraffic);
				storyDetail.setPageViewsTracker(pageViewsTracker);
				records.add(storyDetail);
			} catch (Exception e) {
				e.printStackTrace();
				log.warn("Ignoring story " + story.getKey() + ", Caused by: " + e.getMessage());
				continue;
			}
		}

		if (query.getExcludeTracker() != null) {
			List<String> socialTracker = query.getSocialReferer();
			for (String excludeTracker : query.getExcludeTracker()) {
				if (socialTracker.contains(excludeTracker)) {
					setFacebookFlag = false;
					break;
				}
			}
		}

		log.info("Retrieving top stories; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public List<TopAuthorsResponse> getTopAuthorsList(WisdomProductQuery query) {
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
					.must(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE).gte(startPubDate).lte(endPubDate))
					.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()))
					.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));
			if (query.getAuthorId() != null) {
				boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.AUTHOR_ID, query.getAuthorId()));
			}
			System.out.println("boolQuery :"+boolQuery);
			String includes[] = { WisdomConstants.AUTHOR };
			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(boolQuery)
					.addAggregation(AggregationBuilders.terms("author_id").field(WisdomConstants.AUTHOR_ID).size(100)
							.order(Order.aggregation("tupvs", false))
							.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
							.subAggregation(AggregationBuilders.cardinality("storyCount").field(WisdomConstants.STORY_ID))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1).sort(WisdomConstants.DATETIME, SortOrder.DESC))
							.subAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
									.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS))))
					.setSize(0).execute().actionGet();			
			System.out.println("res :"+res);
			Terms authorBuckets = res.getAggregations().get("author_id");
			for (Terms.Bucket author : authorBuckets.getBuckets()) {
				if(StringUtils.isNotBlank(author.getKeyAsString())){

					TopAuthorsResponse authorDetail = new TopAuthorsResponse();
					authorDetail.setMpvs(0);
					authorDetail.setWpvs(0);
					authorDetail.setAuthor_id(author.getKeyAsString());
					Sum tpvs = author.getAggregations().get("tpvs");
					authorDetail.setTotalpvs((new Double(tpvs.getValue())).longValue());
					Sum tupvs = author.getAggregations().get("tupvs");
					authorDetail.setTotalupvs((new Double(tupvs.getValue())).longValue());
					Cardinality storyCount = author.getAggregations().get("storyCount");
					authorDetail.setStory_count(new Long(storyCount.getValue()).intValue());
					TopHits topHits = author.getAggregations().get("top");					
					if(topHits !=null) {
					authorDetail.setAuthor(topHits.getHits().getHits()[0].getSource().get(WisdomConstants.AUTHOR).toString());
					}
					Terms hostType = author.getAggregations().get("platform");
					for (Terms.Bucket host : hostType.getBuckets()) {
						Sum pvs = host.getAggregations().get("pvs");
						Sum upvs = host.getAggregations().get("upvs");
						if (host.getKey().equals(WisdomConstants.MOBILE)) {
							authorDetail.setMpvs(new Double(pvs.getValue()).intValue());
							authorDetail.setMupvs(new Double(upvs.getValue()).intValue());
						} else if (host.getKey().equals(WisdomConstants.WEB)) {
							authorDetail.setWpvs(new Double(pvs.getValue()).intValue());
							authorDetail.setWupvs(new Double(upvs.getValue()).intValue());
						}
					}

					records.add(authorDetail);
				
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving top authors.", e);
		}
		log.info("Retrieving top authors; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public Map<String, Object> getAuthorsMonthlyData(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> authorsMap = new HashMap<>();
		// try {
		BoolQueryBuilder qb = getQuery(query);
		String startPubDate = DateUtil.getCurrentDate().replaceAll("_", "-");
		;
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

		QueryBuilder fqb = qb
				.filter(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE).gte(startPubDate).lte(endPubDate));
		System.out.println("query :"+fqb);
		SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(fqb)
				.addAggregation(AggregationBuilders.terms("author_id").field(WisdomConstants.AUTHOR_ID).size(100)
						.order(Order.aggregation("tupvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
						.subAggregation(AggregationBuilders.cardinality("stories").field(WisdomConstants.STORY_ID)))
				.setSize(0).execute().actionGet();
		Terms authorBuckets = res.getAggregations().get("author_id");
		for (Terms.Bucket author : authorBuckets.getBuckets()) {
			if(StringUtils.isNotBlank(author.getKeyAsString())){
			TopAuthorsResponse authorDetail = new TopAuthorsResponse();
			Sum tpvs_agg = author.getAggregations().get("tpvs");
			long tpvs = (new Double(tpvs_agg.getValue())).longValue();
			Sum tupvs_agg = author.getAggregations().get("tupvs");
			long tupvs = (new Double(tupvs_agg.getValue())).longValue();
			Cardinality stories = author.getAggregations().get("stories");
			long storyCount = stories.getValue();
			long avgStoryCount = 0;
			long avgPvs = 0;
			long avgupvs = 0;
			if (daysCount != 0) {
				avgStoryCount = storyCount / daysCount;
			}
			if (storyCount != 0) {
				avgPvs = tpvs / storyCount;
				avgupvs = tupvs / storyCount;
			}
			authorDetail.setAvgPvs(avgPvs);
			authorDetail.setAvgUpvs(avgupvs);
			authorDetail.setAvgStoryCount(avgStoryCount);
			authorDetail.setCurrentMonthPvs(tpvs);
			authorsMap.put(author.getKeyAsString(), authorDetail);
		}}
		/*
		 * } catch (Exception e) { e.printStackTrace();
		 * log.error("Error while AuthorsMonthlyData.", e); }
		 */
		log.info("Retrieving AuthorsMonthlyData; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return authorsMap;
	}
	
	
	private BoolQueryBuilder getQuery(WisdomProductQuery query) {
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		if (!StringUtils.isBlank(query.getStoryid())) {
			boolQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
		}
		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();
		if (!StringUtils.isBlank(query.getStartDate())) {
			startDatetime = query.getStartDate();
		}
		if (!StringUtils.isBlank(query.getEndDate())) {
			endDatetime = query.getEndDate();
		}
		if (query.getPlatform() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));
		}

		if (query.getStartPubDate() != null && query.getEndPubDate() != null) {
			if (DateUtil.getNumDaysBetweenDates(query.getStartPubDate(),
					query.getEndPubDate()) > WisdomConstants.CALENDAR_LIMIT) {
				throw new DBAnalyticsException(
						"Date range more than " + WisdomConstants.CALENDAR_LIMIT + " days");
			}
			boolQuery.must(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE).gte(query.getStartPubDate())
					.lte(query.getEndPubDate()));
		} else if (query.isEnableDate()) {
			if (DateUtil.getNumDaysBetweenDates(startDatetime,
					endDatetime) > WisdomConstants.CALENDAR_LIMIT) {
				throw new DBAnalyticsException(
						"Date range more than " + WisdomConstants.CALENDAR_LIMIT + " days");
			}
			boolQuery.must(QueryBuilders.rangeQuery(WisdomConstants.DATETIME).gte(startDatetime).lte(endDatetime));
		}
		if (query.getAuthorId() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.AUTHOR_ID, query.getAuthorId()));
		}
		if (query.getDomainId() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));
		}
		if (query.getTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getTracker()) {
				trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
			}
			boolQuery.must(trackersBoolQuery);
			// boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.TRACKER,
			// query.getTracker()));
		}
		if (query.getExcludeTracker() != null) {
			BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
			for (String tracker : query.getExcludeTracker()) {
				trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
			}
			boolQuery.mustNot(trackersBoolQuery);
			// boolQuery.mustNot(QueryBuilders.termsQuery(WisdomConstants.TRACKER,
			// query.getExcludeTracker()));
		}
		if (query.getCategoryId() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.CATEGORY_ID, query.getCategoryId()));
		}
		if (query.getSectionId() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.SECTION_ID, query.getSectionId()));
		}
		if (query.getRelative_url() != null) {
			boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.REALTIVE_URL, query.getRelative_url()));
		}
		return boolQuery;
	}

	public List<StoryDetail> getAuthorStoriesList(WisdomProductQuery query) {
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

		String includes[] = {WisdomConstants.PUBLISHED_DATE,WisdomConstants.AUTHOR,WisdomConstants.TITLE};
		SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, startPubDate, endPubDate)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.rangeQuery(WisdomConstants.PUBLISHED_DATE).gte(startPubDate).lte(endPubDate))
						.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()))
						.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomain()))
						.must(QueryBuilders.termsQuery(WisdomConstants.AUTHOR_ID, query.getAuthorId())))
				.addAggregation(AggregationBuilders.terms("stories").field(WisdomConstants.STORY_ID).size(100)
						.order(Order.aggregation("tupvs", false))
						.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
						.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
								.size(1).sort(WisdomConstants.DATETIME, SortOrder.DESC))
						.subAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
								.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
								.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS))))
				.addAggregation(AggregationBuilders.sum("authorPvs").field(WisdomConstants.PVS)).setSize(0).execute()
				.actionGet();
		if (res.getHits().getTotalHits() == 0) {
			throw new DBAnalyticsException("No author found with id " + query.getAuthorId());
		}
		Sum author_tpvs = res.getAggregations().get("authorPvs");
		long authorTpvs = new Double(author_tpvs.getValue()).longValue();
		Terms storyBuckets = res.getAggregations().get("stories");
		for (Terms.Bucket story : storyBuckets.getBuckets()) {
			StoryDetail storyDetail = new StoryDetail();
			storyDetail.setStory_id((story.getKeyAsString()));
			Sum tpvs = story.getAggregations().get("tpvs");
			storyDetail.setTotalpvs((new Double(tpvs.getValue())).longValue());
			Sum tupvs = story.getAggregations().get("tupvs");
			storyDetail.setTotalupvs((new Double(tupvs.getValue())).longValue());
			TopHits topHits = story.getAggregations().get("top");
			Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
			storyDetail.setAuthor(source.get(WisdomConstants.AUTHOR).toString());
			storyDetail.setPublished_date(source.get(WisdomConstants.PUBLISHED_DATE).toString());
			storyDetail.setTitle(source.get(WisdomConstants.TITLE).toString());
			Terms hostType = story.getAggregations().get("platform");
			for (Terms.Bucket host : hostType.getBuckets()) {
				Sum pvs = host.getAggregations().get("pvs");
				Sum upvs = host.getAggregations().get("upvs");
				if (host.getKey().equals(WisdomConstants.MOBILE)) {
					storyDetail.setMpvs(new Double(pvs.getValue()).longValue());
					storyDetail.setMupvs(new Double(upvs.getValue()).longValue());
				} else if (host.getKey().equals(WisdomConstants.WEB)) {
					storyDetail.setWpvs(new Double(pvs.getValue()).longValue());
					storyDetail.setWupvs(new Double(upvs.getValue()).longValue());
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
	
	public List<StoryDetail> getStoryDetail(WisdomProductQuery query) {
		query.setEnableDate(false);
		// query.setStoryDetail(true);
		return getStoriesList(query);
	}
	
	public List<SlideDetail> getSlideWisePvs(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		List<SlideDetail> records = new ArrayList<>();
		String[] fields = {WisdomConstants.APVS, WisdomConstants.IPVS, WisdomConstants.MPVS, WisdomConstants.WPVS,WisdomConstants.SLIDE_NUMBER};
		if (query.getField() != null) {
			fields = query.getField().split(",");
	 	}
		// Commented code is to remove slides greater than than max slide count
		// from story_detail.	

		SearchResponse sr = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()))
				.addAggregation(AggregationBuilders.max("slideCount").field(WisdomConstants.SLIDES)).setSize(0).execute()
				.actionGet();
		
		SearchResponse res = client.prepareSearch(Indexes.WisdomIndexes.WISDOM_STORY_SEQUENCE).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()))
				.addSort(WisdomConstants.SLIDE_NUMBER, SortOrder.ASC).setFetchSource(fields, new String[] {}).setSize(50).execute()
				.actionGet();
		
		Double slideCount = ((Max) sr.getAggregations().get("slideCount")).getValue();
		
		for (SearchHit hit : res.getHits().getHits()) {
			SlideDetail slide = new SlideDetail();
			Map<String, Object> source = hit.getSource();
			if(source.get(WisdomConstants.SLIDE_NUMBER) !=null && (!(source.get(WisdomConstants.SLIDE_NUMBER) instanceof String)))			
			{			
			if ((Integer) source.get(WisdomConstants.SLIDE_NUMBER) <= slideCount) {
				slide.setSlideNo((Integer) source.get(WisdomConstants.SLIDE_NUMBER));
				long mpvs = 0;

				if (query.getExcludeTracker() == null) {
					if (source.get(WisdomConstants.UCBPVS) != null)
						mpvs = mpvs + ((Integer) source.get(WisdomConstants.UCBPVS)).longValue();
				}

				if (source.get(WisdomConstants.MPVS) != null) {
					mpvs = mpvs + ((Integer) source.get(WisdomConstants.MPVS)).longValue();
					slide.setMpvs(mpvs);
				}

				if (source.get(WisdomConstants.WPVS) != null)
					slide.setWpvs(((Integer) source.get(WisdomConstants.WPVS)).longValue());

				if ((Integer) source.get(WisdomConstants.SLIDE_NUMBER) != 0) {
					if (source.get(WisdomConstants.APVS) != null)
						slide.setApvs(((Integer) source.get(WisdomConstants.APVS)).longValue());

					if (source.get(WisdomConstants.IPVS) != null)
						slide.setIpvs(((Integer) source.get(WisdomConstants.IPVS)).longValue());
				}

				records.add(slide);
			}}
		}

		log.info("Retrieving getSlideWisePvs; Slide Count: " + records.size() + "; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public List<StoryPerformance> getStoryPerformance(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		List<StoryPerformance> records = new ArrayList<>();
		try {

			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()))
					.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));

			if (query.getExcludeTracker() != null) {
				qb.mustNot(QueryBuilders.termsQuery(WisdomConstants.TRACKER, query.getExcludeTracker()));
			}
			
			String includes[] = {WisdomConstants.MODIFIED_DATE};
			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.dateHistogram("days").field(WisdomConstants.DATETIME)
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
							//.subAggregation(AggregationBuilders.max("version").field(WisdomConstants.MODIFIED_DATE))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {}).sort(WisdomConstants.MODIFIED_DATE, SortOrder.DESC).size(1))
							) 
					.setSize(0).execute().actionGet();

			Histogram interval = res.getAggregations().get("days");
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					StoryPerformance performance = new StoryPerformance();
					Sum tpvs = bucket.getAggregations().get("tpvs");
					performance.setTpvs(((Double) tpvs.getValue()).longValue());
					Sum tupvs = bucket.getAggregations().get("tupvs");
					performance.setTupvs(((Double) tupvs.getValue()).longValue());
					//Max version = bucket.getAggregations().get("version");
					//performance.setVersion(((Double) version.getValue()).intValue());
					TopHits topHits = bucket.getAggregations().get("top");
					Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
					performance.setModified_date((String) source.get(WisdomConstants.MODIFIED_DATE));
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
	
	public SourcePerformance getTrackerwisePerformanceGraph(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		SourcePerformance sourcePerformance = new SourcePerformance();
		List<StoryPerformance> records = new ArrayList<>();
		long total_pvs = 0L;
		long total_upvs = 0L;
		try {

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(QueryBuilders.boolQuery()
							.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()))
							.must(QueryBuilders.termsQuery(WisdomConstants.TRACKER, query.getTracker()))
							.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform())))
					.addAggregation(AggregationBuilders.dateHistogram("days").field(WisdomConstants.DATETIME)
							.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
							.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)))
					.setSize(0).execute().actionGet();

			Histogram interval = res.getAggregations().get("days");
			for (Histogram.Bucket bucket : interval.getBuckets()) {
				if(bucket.getDocCount()>0){
					StoryPerformance performance = new StoryPerformance();
					Sum tpvs = bucket.getAggregations().get("tpvs");
					performance.setDate(bucket.getKeyAsString());
					performance.setTpvs(((Double) tpvs.getValue()).longValue());
					total_pvs += ((Double) tpvs.getValue()).longValue();
					Sum tupvs = bucket.getAggregations().get("tupvs");
					performance.setTupvs(((Double) tupvs.getValue()).longValue());
					total_upvs += ((Double) tupvs.getValue()).longValue();
					records.add(performance);
				}
			}
			sourcePerformance.setPerformance(records);
			sourcePerformance.setPvs(total_pvs);
			sourcePerformance.setUpvs(total_upvs);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getTrackerwisePerformanceGraph.", e);
		}
		log.info("Retrieving getTrackerwisePerformanceGraph; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return sourcePerformance;
	}
	
	public TreeMap<String, StoryDetail> getVersionwisePerformance(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		TreeMap<String, StoryDetail> records = new TreeMap<>();
		try {
			String includes[] = { WisdomConstants.TITLE, WisdomConstants.SLIDES };
			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()))
					.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));

			if (query.getExcludeTracker() != null) {
				qb.mustNot(QueryBuilders.termsQuery(WisdomConstants.TRACKER, query.getExcludeTracker()));
			}

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.addAggregation(AggregationBuilders.terms("modified_date").field(WisdomConstants.MODIFIED_DATE).size(50)
							.subAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
							.subAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1))
							.subAggregation(AggregationBuilders.terms("platform").field(WisdomConstants.PLATFORM).size(5)
									.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS)).subAggregation(
											AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
							.subAggregation(AggregationBuilders.terms("sourceTraffic").field(WisdomConstants.HTTP_REFERER)
									.size(5).order(Order.aggregation("pvs", false)).subAggregation(
											AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
									.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS)))
							.subAggregation(AggregationBuilders.terms("pageViewsTracker").field(WisdomConstants.TRACKER)
									.size(5).order(Order.aggregation("pvs", false))
									.subAggregation(AggregationBuilders.sum("pvs").field(WisdomConstants.PVS))
									.subAggregation(AggregationBuilders.sum("upvs").field(WisdomConstants.UPVS))))
					.setSize(0).execute().actionGet();

			Terms modifiedDateAgg = res.getAggregations().get("modified_date");
			for (Terms.Bucket modifiedDateBucket : modifiedDateAgg.getBuckets()) {
				StoryDetail storyDetail = new StoryDetail();
				Map<String, Map<String, Long>> sourceTraffic = new LinkedHashMap<>();
				Map<String, Map<String, Long>> pageViewsTracker = new LinkedHashMap<>();
				Long tpvs = 0L;
				Long tupvs = 0L;
				Long mpvs = 0L;
				Long wpvs = 0L;
				Long apvs = 0L;
				Long ipvs = 0L;
				Long mupvs = 0L;
				Long wupvs = 0L;
				Long aupvs = 0L;
				Long iupvs = 0L;
				int slideCount = 0;
				Long slideDepth = 0L;
				String modfiedDate = modifiedDateBucket.getKeyAsString();
				storyDetail.setModified_date(modfiedDate);
				Sum totalPvs = modifiedDateBucket.getAggregations().get("tpvs");
				tpvs = Double.valueOf(totalPvs.getValue()).longValue();
				Sum totalupvs = modifiedDateBucket.getAggregations().get("tupvs");
				tupvs = Double.valueOf(totalupvs.getValue()).longValue();
				storyDetail.setTotalpvs(tpvs);
				storyDetail.setTotalupvs(tupvs);
				TopHits topHits = modifiedDateBucket.getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				if (source.get(WisdomConstants.TITLE) != null)
					storyDetail.setTitle(source.get(WisdomConstants.TITLE).toString());
				// if(source.get(Constants.PGTOTAL)==null)
				if (source.get(WisdomConstants.SLIDES) != null) {
					if(source.get(WisdomConstants.SLIDES) instanceof Integer) {
					slideCount = Integer.valueOf(source.get(WisdomConstants.SLIDES).toString());
					storyDetail.setSlides(slideCount);}
				}
				Terms platform = modifiedDateBucket.getAggregations().get("platform");
				for (Terms.Bucket host : platform.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");
					Sum upvs = host.getAggregations().get("upvs");
					if (host.getKey().equals(WisdomConstants.MOBILE)) {
						mpvs = new Double(pvs.getValue()).longValue();
						mupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.WEB)) {
						wpvs = new Double(pvs.getValue()).longValue();
						wupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.ANDROID)) {
						apvs = new Double(pvs.getValue()).longValue();
						aupvs = new Double(upvs.getValue()).longValue();
					} else if (host.getKey().equals(WisdomConstants.IPHONE)) {
						ipvs = new Double(pvs.getValue()).longValue();
						iupvs = new Double(upvs.getValue()).longValue();
					}
				}
				if (slideCount != 0 && tupvs != 0) {
					slideDepth = new Double((tpvs.doubleValue() / (tupvs * slideCount)) * 100).longValue();
				}
				storyDetail.setSlideDepth(slideDepth);
				Terms traffic = modifiedDateBucket.getAggregations().get("sourceTraffic");
				for (Terms.Bucket trafficS : traffic.getBuckets()) {
					Map<String, Long> map = new HashMap<>();
					Sum pvs = trafficS.getAggregations().get("pvs");
					Sum upvs = trafficS.getAggregations().get("upvs");
					map.put("pvs", new Double(pvs.getValue()).longValue());
					map.put("upvs", new Double(upvs.getValue()).longValue());
					sourceTraffic.put(trafficS.getKeyAsString(), map);
				}
				Terms pvTracker = modifiedDateBucket.getAggregations().get("pageViewsTracker");
				for (Terms.Bucket tracker : pvTracker.getBuckets()) {

					Map<String, Long> map = new HashMap<>();
					Sum pvs = tracker.getAggregations().get("pvs");
					Sum upvs = tracker.getAggregations().get("upvs");
					map.put("pvs", new Double(pvs.getValue()).longValue());
					map.put("upvs", new Double(upvs.getValue()).longValue());
					pageViewsTracker.put(tracker.getKeyAsString(), map);
				}
				storyDetail.setMpvs(mpvs);
				storyDetail.setWpvs(wpvs);
				storyDetail.setApvs(apvs);
				storyDetail.setIpvs(ipvs);
				storyDetail.setMupvs(mupvs);
				storyDetail.setWupvs(wupvs);
				storyDetail.setAupvs(aupvs);
				storyDetail.setIupvs(iupvs);
				storyDetail.setSourceWiseTraffic(sourceTraffic);
				storyDetail.setPageViewsTracker(pageViewsTracker);
				records.put(modfiedDate, storyDetail);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getVersionwisePerformance.", e);
		}
		log.info("Retrieving getVersionwisePerformance; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public List<Timeline> getStoryTimeline(WisdomProductQuery query) {
		long startTime = System.currentTimeMillis();
		List<Timeline> records = new ArrayList<>();
		try {
			// String indexName = "realtime_" + DateUtil.getCurrentDate();
			MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();

			if (query.getFlickerTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));
				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getFlickerTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(WisdomConstants.DATETIME, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)));

			}

			if (query.getRhsTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getRhsTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(WisdomConstants.DATETIME, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)));

			}

			if (query.getForganicTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getForganicTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(WisdomConstants.DATETIME, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)));

			}

			if (query.getFpaidTracker() != null) {
				BoolQueryBuilder boolQuery = new BoolQueryBuilder();
				boolQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
				boolQuery.must(QueryBuilders.termsQuery(WisdomConstants.PLATFORM, query.getPlatform()));

				BoolQueryBuilder trackersBoolQuery = new BoolQueryBuilder();
				for (String tracker : query.getFpaidTracker()) {
					trackersBoolQuery.should(QueryBuilders.prefixQuery(WisdomConstants.TRACKER, tracker));
				}
				boolQuery.must(trackersBoolQuery);
				multiSearchRequestBuilder
				.add(client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate())).setTypes(MappingTypes.MAPPING_REALTIME)
						.setQuery(boolQuery).setSize(1).addSort(WisdomConstants.DATETIME, SortOrder.ASC)
						.addAggregation(AggregationBuilders.sum("tpvs").field(WisdomConstants.PVS))
						.addAggregation(AggregationBuilders.sum("tupvs").field(WisdomConstants.UPVS)));

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
							response.getHits().getHits()[0].getSource().get(WisdomConstants.DATETIME).toString());
					Sum tpvs = response.getAggregations().get("tpvs");
					timeline.setTpvs(((Double) tpvs.getValue()).longValue());
					Sum tupvs = response.getAggregations().get("tupvs");
					timeline.setTupvs(((Double) tupvs.getValue()).longValue());
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
	
	public ArrayList<Map<String, Object>> getAuthorsList(WisdomProductQuery query){		
		long startTime = System.currentTimeMillis();
		
		String startDate = DateUtil.getCurrentDate();		
		if(StringUtils.isNotBlank(query.getStartDate())) {
		 startDate = query.getStartDate();
		}
		
		String endDate = DateUtil.getCurrentDate();
		if(StringUtils.isNotBlank(query.getEndDate())){
			endDate = query.getEndDate();
		}
		
		ArrayList<Map<String, Object>> authorsList = new ArrayList<>();
		try {
			BoolQueryBuilder authorsBoolQuery = new BoolQueryBuilder();
			/*if(query.getProfile_id() != null){
				authorsBoolQuery.must(QueryBuilders.termQuery(WisdomConstants.PROFILE_ID, query.getProfile_id()));
			}*/
			if(query.getDomainId()!= null){
				authorsBoolQuery.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));	
			}
			String includes[] = {WisdomConstants.AUTHOR,WisdomConstants.AUTHOR_ID,WisdomConstants.PROFILE_ID,WisdomConstants.DOMAIN_ID};
			SearchResponse response = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_STORY_DETAIL, startDate, endDate))
								.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(authorsBoolQuery)
								.addAggregation(AggregationBuilders.terms("authors").field(WisdomConstants.AUTHOR)
										.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {}).size(1)))
								.execute().actionGet();				
			Terms authorsAggration = response.getAggregations().get("authors");
			Map<String, Object> authorsMap = new HashMap<>();
			
			for (Terms.Bucket authorsBucket : authorsAggration.getBuckets()) {
				TopHits topHits = authorsBucket.getAggregations().get("top");
				Map<String, Object> source = topHits.getHits().getHits()[0].getSource();
				if(StringUtils.isNotBlank((String) source.get(WisdomConstants.AUTHOR))) {				
					authorsMap.put(WisdomConstants.AUTHOR, (String) source.get(WisdomConstants.AUTHOR));
				}
				if(source.get(WisdomConstants.AUTHOR_ID) instanceof String) {
					if(StringUtils.isNotBlank((String) source.get(WisdomConstants.AUTHOR_ID))) {				
						authorsMap.put(WisdomConstants.AUTHOR_ID, Integer.valueOf((String) source.get(WisdomConstants.AUTHOR_ID)) );
					}
				}
				else if(source.get(WisdomConstants.AUTHOR_ID) instanceof Integer) {									
						authorsMap.put(WisdomConstants.AUTHOR_ID, (Integer) source.get(WisdomConstants.AUTHOR_ID));					
				}
				
				if(StringUtils.isNotBlank((String) source.get(WisdomConstants.PROFILE_ID))) {
					authorsMap.put(WisdomConstants.PROFILE_ID, (String) source.get(WisdomConstants.PROFILE_ID));
				}
				if(StringUtils.isNotBlank((String) source.get(WisdomConstants.DOMAIN_ID))) {
					authorsMap.put(WisdomConstants.DOMAIN_ID, (String) source.get(WisdomConstants.DOMAIN_ID));
				}			
			}
			authorsList.add(authorsMap);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while getAuthorsList.", e);
		}
		log.info("Retrieving top authors list; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return authorsList;
	}	
	
	public FacebookInsightsPage getFacebookInsights(WisdomProductQuery query) {
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
					.mustNot(QueryBuilders.termQuery(WisdomConstants.TITLE, ""))
					.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));

			// Apply category filtering if needed
			if (query.getCategoryId() != null && query.getCategoryId().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.CATEGORY_ID, query.getCategoryId()));
			}
			/*if (query.getSuper_cat_id() != null) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.SUPER_CAT_ID, query.getSuper_cat_id()));
			}
			else if (query.getGa_cat_name() != null && query.getGa_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
			}

			else if (query.getPp_cat_name() != null && query.getPp_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
			}

			if (query.getStatus_type() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.STATUS_TYPE, query.getStatus_type()));
			}
			*/
			if (query.getProfile_id() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.PROFILE_ID, query.getProfile_id()));
			}

			if (query.getFacebook_profile_id() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.FACEBOOK_PROFILE_ID, query.getFacebook_profile_id()));
			}

			if (query.getType() != null) {
				if (query.getType() == 1) {
					qb.must(QueryBuilders.termQuery(WisdomConstants.IA_FLAG, "1"));
				} else if (query.getType() == 0) {
					qb.mustNot(QueryBuilders.termQuery(WisdomConstants.IA_FLAG, "1"));
				}
			}

			QueryBuilder fqb = qb.filter(QueryBuilders.existsQuery(WisdomConstants.STORY_ID));
			
			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HISTORY, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(WisdomConstants.FACEBOOK_STORY_ID)
							.subAggregation(AggregationBuilders.topHits("top").size(1)
									.sort(WisdomConstants.DATETIME, SortOrder.DESC))
							.subAggregation(AggregationBuilders.max(parameter).field(parameter)).size(query.getCount())
							.order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();

			Terms resTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket resBuckets : resTermAgg.getBuckets()) {

				TopHits topHits = resBuckets.getAggregations().get("top");
				SearchHit[] searchHits = topHits.getHits().getHits();

				for (SearchHit hit : searchHits) {
					try {
						FacebookInsights story = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);
						//System.out.println("story :"+story);
						double unique_reach=0.0;
						if(story.getUnique_reach()!=null) {
							unique_reach  = story.getUnique_reach();
						}						 							
						int shares = story.getShares();
						double sharability = 0.0;
						if(unique_reach>0){
							sharability = (shares/unique_reach)*100;
						}
						story.setShareability(sharability);						
						Map<String, Object> source = hit.getSource();
						totalLink_clicks += (Integer) source.get(WisdomConstants.LINK_CLICKS);
						totalTotal_reach += (Integer) source.get(WisdomConstants.TOTAL_REACH);
						if(source.get(WisdomConstants.UNIQUE_REACH)!=null)
						{
						totalUnique_reach += (Integer) source.get(WisdomConstants.UNIQUE_REACH);
						}
						
						totalShares += (Integer) source.get(WisdomConstants.SHARES);
						if ((Integer) source.get(WisdomConstants.LINK_CLICKS) == 0
								|| (Integer) source.get(WisdomConstants.TOTAL_REACH) == 0)
							story.setCtr(0.0);
						else
							story.setCtr((((Integer) source.get(WisdomConstants.LINK_CLICKS)).doubleValue()
									/ (Integer) source.get(WisdomConstants.TOTAL_REACH)) * 100.0);
						// story.setCreated_datetime(source.get(WisdomConstants.CREATED_DATETIME).toString());
						/*
						 * if(source.get(WisdomConstants.BHASKARSTORYID) instanceof
						 * Integer){
						 * story.setStoryid(((Integer)source.get(WisdomConstants.
						 * BHASKARSTORYID)).toString()); records.add(story); }
						 * else
						 * if(!StringUtils.isBlank((String)source.get(WisdomConstants.
						 * BHASKARSTORYID))){
						 * story.setStoryid(source.get(WisdomConstants.BHASKARSTORYID)
						 * .toString()); records.add(story); } else {
						 * story.setStoryid("0"); records.add(story);
						 * log.warn("bhaskarstoryid missing for fb_storyid: "
						 * +hit.getId()); }
						 */
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
	}
	
	public FacebookInsightsPage getFacebookInsightsForDate(WisdomProductQuery query) {
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
					.mustNot(QueryBuilders.termQuery(WisdomConstants.TITLE, ""))
					.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));

			// Apply category filtering if needed
			if (query.getCategoryId() != null && query.getCategoryId().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.CATEGORY_ID, query.getCategoryId()));
			}
			/*if (query.getSuper_cat_id() != null) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.SUPER_CAT_ID, query.getSuper_cat_id()));
			}
			else if (query.getGa_cat_name() != null && query.getGa_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
			}

			else if (query.getPp_cat_name() != null && query.getPp_cat_name().size() > 0) {
				qb.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
			}*/

			if (query.getStatus_type() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.STATUS_TYPE, query.getStatus_type()));
			}

			if (query.getProfile_id() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.PROFILE_ID, query.getProfile_id()));
			}

			if (query.getFacebook_profile_id() != null) {
				qb.must(QueryBuilders.termQuery(WisdomConstants.FACEBOOK_PROFILE_ID, query.getFacebook_profile_id()));
			}
			QueryBuilder fqb = qb.filter(
					QueryBuilders.existsQuery(WisdomConstants.STORY_ID));

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HOURLY, startDatetime, endDatetime))
					
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
					.addAggregation(AggregationBuilders.terms("storyid").field(WisdomConstants.STORY_ID)
							.subAggregation(AggregationBuilders
									.topHits("top").size(1).sort(WisdomConstants.DATETIME, SortOrder.DESC))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
									.field(WisdomConstants.REACTION_THANKFUL))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
									.field(WisdomConstants.REPORT_SPAM_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.VIDEO_VIEWS).field(WisdomConstants.VIDEO_VIEWS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_VIDEO_VIEWS)
									.field(WisdomConstants.UNIQUE_VIDEO_VIEWS))
							.size(query.getCount()).order(Order.aggregation(parameter, false)))
					.setSize(0).execute().actionGet();

			Terms resTermAgg = res.getAggregations().get("storyid");
			for (Terms.Bucket bucket : resTermAgg.getBuckets()) {

				TopHits topHits = bucket.getAggregations().get("top");
				SearchHit hit = topHits.getHits().getHits()[0];
				FacebookInsights insights = gson.fromJson(hit.getSourceAsString(), FacebookInsights.class);

				Sum total_reach_agg = bucket.getAggregations().get(WisdomConstants.TOTAL_REACH);
				insights.setTotal_reach(new Double(total_reach_agg.getValue()).longValue());
				totalTotal_reach += new Double(total_reach_agg.getValue()).intValue();

				Sum unique_reach_agg = bucket.getAggregations().get(WisdomConstants.UNIQUE_REACH);
				insights.setUnique_reach(new Double(unique_reach_agg.getValue()).longValue());
				totalUnique_reach += new Double(unique_reach_agg.getValue()).intValue();

				Sum link_clicks_agg = bucket.getAggregations().get(WisdomConstants.LINK_CLICKS);
				insights.setLink_clicks(new Double(link_clicks_agg.getValue()).longValue());
				totalLink_clicks += new Double(link_clicks_agg.getValue()).intValue();

				Sum reaction_angry_agg = bucket.getAggregations().get(WisdomConstants.REACTION_ANGRY);
				insights.setReaction_angry(new Double(reaction_angry_agg.getValue()).intValue());

				Sum reaction_haha_agg = bucket.getAggregations().get(WisdomConstants.REACTION_HAHA);
				insights.setReaction_haha(new Double(reaction_haha_agg.getValue()).intValue());

				Sum reaction_love_agg = bucket.getAggregations().get(WisdomConstants.REACTION_LOVE);
				insights.setReaction_love(new Double(reaction_love_agg.getValue()).intValue());

				Sum reaction_sad_agg = bucket.getAggregations().get(WisdomConstants.REACTION_SAD);
				insights.setReaction_sad(new Double(reaction_sad_agg.getValue()).intValue());

				Sum reaction_thankful_agg = bucket.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
				insights.setReaction_thankful(new Double(reaction_thankful_agg.getValue()).intValue());

				Sum reaction_wow_agg = bucket.getAggregations().get(WisdomConstants.REACTION_WOW);
				insights.setReaction_wow(new Double(reaction_wow_agg.getValue()).intValue());

				Sum hide_all_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
				insights.setHide_all_clicks(new Double(hide_all_clicks_agg.getValue()).intValue());

				Sum hide_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_CLICKS);
				insights.setHide_clicks(new Double(hide_clicks_agg.getValue()).intValue());

				Sum report_spam_clicks_agg = bucket.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
				insights.setReport_spam_clicks(new Double(report_spam_clicks_agg.getValue()).intValue());

				Sum likes_agg = bucket.getAggregations().get(WisdomConstants.LIKES);
				insights.setLikes(new Double(likes_agg.getValue()).intValue());

				Sum comments_agg = bucket.getAggregations().get(WisdomConstants.COMMENTS);
				insights.setComments(new Double(comments_agg.getValue()).intValue());

				Sum shares_agg = bucket.getAggregations().get(WisdomConstants.SHARES);
				insights.setShares(new Double(shares_agg.getValue()).intValue());
				totalShares += new Double(shares_agg.getValue()).intValue();

				Sum ia_clicks_agg = bucket.getAggregations().get(WisdomConstants.IA_CLICKS);
				insights.setIa_clicks(new Double(ia_clicks_agg.getValue()).intValue());

				Sum video_views_agg = bucket.getAggregations().get(WisdomConstants.VIDEO_VIEWS);
				insights.setVideo_views(new Double(video_views_agg.getValue()).intValue());

				Sum unique_video_views_agg = bucket.getAggregations().get(WisdomConstants.UNIQUE_VIDEO_VIEWS);
				insights.setUnique_video_views(new Double(unique_video_views_agg.getValue()).intValue());

				if (new Double(total_reach_agg.getValue()).intValue() == 0
						|| new Double(link_clicks_agg.getValue()).intValue() == 0)
					insights.setCtr(0.0);
				else
					insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);
				if (insights.getBhaskarstoryid() == null) {
					log.warn("bhaskarstoryid missing for fb_storyid: " + hit.getId());
					insights.setBhaskarstoryid("0");
				}
				records.add(insights);
			}

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
	
	public Map<String, FacebookInsights> getFacebookInsightsByInterval(WisdomProductQuery query) {
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
			
			String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endDatetime = DateUtil.getCurrentDateTime();
			if (!StringUtils.isBlank(query.getStartDate())) {
				startDatetime = query.getStartDate();
			}
			if (!StringUtils.isBlank(query.getEndDate())) {
				endDatetime = query.getEndDate();
			}


			//Map<String, Long> popularity = getFacebookPopularity(query);
			// System.out.println(popularity);
			DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("interval").field(query.getDateField())
					.dateHistogramInterval(histInterval).format("yyyy-MM-dd'T'HH:mm:ss'Z'").order(order)
					.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL).field(WisdomConstants.REACTION_THANKFUL))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS).field(WisdomConstants.REPORT_SPAM_CLICKS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.VIDEO_VIEWS).field(WisdomConstants.VIDEO_VIEWS))
					.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_VIDEO_VIEWS).field(WisdomConstants.UNIQUE_VIDEO_VIEWS));

			if (query.getStoryid() != null) {
				/*aggBuilder
				.subAggregation(AggregationBuilders.sum(WisdomConstants.POPULAR_COUNT).field(WisdomConstants.POPULAR_COUNT))
				.subAggregation(
						AggregationBuilders.sum(WisdomConstants.NOT_POPULAR_COUNT).field(WisdomConstants.NOT_POPULAR_COUNT))
				.subAggregation(
						AggregationBuilders.sum(WisdomConstants.POPULARITY_SCORE).field(WisdomConstants.POPULARITY_SCORE));*/

			}

			else {
				if (query.getInterval().equals("1d")
						&& query.getEndDate().equals(DateUtil.getCurrentDate().replaceAll("_", "-"))
						&& query.isPrevDayRequired()) {
					records.put("prevDayData", getPrevDayFacebookInsights(query));
				}
				aggBuilder
				.subAggregation(AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID))
				.subAggregation(AggregationBuilders.cardinality("ia_story_count").field(WisdomConstants.IA_STORYID))
				.subAggregation(AggregationBuilders.filter("links",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "link"))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
								.field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY)
								.field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS)
								.field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(
								AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID)))
				.subAggregation(AggregationBuilders.filter("photos",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "photo"))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
								.field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY)
								.field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS)
								.field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(
								AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID)))
				.subAggregation(AggregationBuilders.filter("videos",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "video"))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
								.field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY)
								.field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS)
								.field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count")
								.field(WisdomConstants.STORY_ID)));

			}

			/*
			 * SearchResponse res =
			 * client.prepareSearch(Indexes.FB_INSIGHTS_HOURLY).setTypes(
			 * MappingTypes.MAPPING_REALTIME)
			 * .setQuery(getSocialDecodeQuery(query)) .addAggregation(
			 * AggregationBuilders.dateHistogram("interval").field(query.
			 * getDateField()).interval(histInterval).order(order)
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * WisdomConstants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * WisdomConstants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * WisdomConstants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(WisdomConstants.STORY_ID_FIELD))
			 * .subAggregation(AggregationBuilders.filter("links").filter(
			 * FilterBuilders.termsFilter(WisdomConstants.STATUS_TYPE, "link"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * WisdomConstants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * WisdomConstants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * WisdomConstants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(WisdomConstants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.filter("photos").filter(
			 * FilterBuilders.termsFilter(WisdomConstants.STATUS_TYPE, "photo"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * WisdomConstants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * WisdomConstants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * WisdomConstants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(WisdomConstants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.filter("videos").filter(
			 * FilterBuilders.termsFilter(WisdomConstants.STATUS_TYPE, "video"))
			 * .subAggregation(AggregationBuilders.sum("total_reach").field(
			 * WisdomConstants.TOTAL_REACH))
			 * .subAggregation(AggregationBuilders.sum("unique_reach").field(
			 * WisdomConstants.UNIQUE_REACH))
			 * .subAggregation(AggregationBuilders.sum("link_clicks").field(
			 * WisdomConstants.LINK_CLICKS))
			 * .subAggregation(AggregationBuilders.cardinality("story_count").
			 * field(WisdomConstants.STORY_ID_FIELD)))
			 * .subAggregation(AggregationBuilders.topHits("top").fetchSource
			 * (new String[]{WisdomConstants.IS_POPULAR}, new String[] {})
			 * .setSize(1).addSort(query.getDateField(), SortOrder.DESC)) )
			 * .setSize(0).execute().actionGet();
			 */

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HOURLY, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(aggBuilder).setSize(0).execute().actionGet();

			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					FacebookInsights insights = new FacebookInsights();

					Sum total_reach_agg = bucket.getAggregations().get(WisdomConstants.TOTAL_REACH);
					insights.setTotal_reach(new Double(total_reach_agg.getValue()).longValue());

					Sum unique_reach_agg = bucket.getAggregations().get(WisdomConstants.UNIQUE_REACH);
					insights.setUnique_reach(new Double(unique_reach_agg.getValue()).longValue());

					Sum link_clicks_agg = bucket.getAggregations().get(WisdomConstants.LINK_CLICKS);
					insights.setLink_clicks(new Double(link_clicks_agg.getValue()).longValue());

					Sum reaction_angry_agg = bucket.getAggregations().get(WisdomConstants.REACTION_ANGRY);
					insights.setReaction_angry(new Double(reaction_angry_agg.getValue()).intValue());

					Sum reaction_haha_agg = bucket.getAggregations().get(WisdomConstants.REACTION_HAHA);
					insights.setReaction_haha(new Double(reaction_haha_agg.getValue()).intValue());

					Sum reaction_love_agg = bucket.getAggregations().get(WisdomConstants.REACTION_LOVE);
					insights.setReaction_love(new Double(reaction_love_agg.getValue()).intValue());

					Sum reaction_sad_agg = bucket.getAggregations().get(WisdomConstants.REACTION_SAD);
					insights.setReaction_sad(new Double(reaction_sad_agg.getValue()).intValue());

					Sum reaction_thankful_agg = bucket.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
					insights.setReaction_thankful(new Double(reaction_thankful_agg.getValue()).intValue());

					Sum reaction_wow_agg = bucket.getAggregations().get(WisdomConstants.REACTION_WOW);
					insights.setReaction_wow(new Double(reaction_wow_agg.getValue()).intValue());

					Sum hide_all_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
					insights.setHide_all_clicks(new Double(hide_all_clicks_agg.getValue()).intValue());

					Sum hide_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_CLICKS);
					insights.setHide_clicks(new Double(hide_clicks_agg.getValue()).intValue());

					Sum report_spam_clicks_agg = bucket.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
					insights.setReport_spam_clicks(new Double(report_spam_clicks_agg.getValue()).intValue());

					Sum likes_agg = bucket.getAggregations().get(WisdomConstants.LIKES);
					insights.setLikes(new Double(likes_agg.getValue()).intValue());

					Sum comments_agg = bucket.getAggregations().get(WisdomConstants.COMMENTS);
					insights.setComments(new Double(comments_agg.getValue()).intValue());

					Sum shares_agg = bucket.getAggregations().get(WisdomConstants.SHARES);
					insights.setShares(new Double(shares_agg.getValue()).intValue());

					Sum ia_clicks_agg = bucket.getAggregations().get(WisdomConstants.IA_CLICKS);
					insights.setIa_clicks(new Double(ia_clicks_agg.getValue()).intValue());

					Sum video_views_agg = bucket.getAggregations().get(WisdomConstants.VIDEO_VIEWS);
					insights.setVideo_views(new Double(video_views_agg.getValue()).intValue());

					Sum unique_video_views_agg = bucket.getAggregations().get(WisdomConstants.UNIQUE_VIDEO_VIEWS);
					insights.setUnique_video_views(new Double(unique_video_views_agg.getValue()).intValue());

					if (unique_reach_agg.getValue() > 0) {
						insights.setShareability((shares_agg.getValue() / unique_reach_agg.getValue()) * 100);
					}

					if (new Double(total_reach_agg.getValue()).intValue() == 0
							|| new Double(link_clicks_agg.getValue()).intValue() == 0)
						insights.setCtr(0.0);
					else
						insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);

					if (query.getStoryid() != null) {/*
						Sum popular_agg = bucket.getAggregations().get(WisdomConstants.POPULAR_COUNT);
						insights.setPopular_count(new Double(popular_agg.getValue()).intValue());

						Sum not_popular_agg = bucket.getAggregations().get(WisdomConstants.NOT_POPULAR_COUNT);
						insights.setNot_popular_count(new Double(not_popular_agg.getValue()).intValue());

						Sum popularity_score_agg = bucket.getAggregations().get(WisdomConstants.POPULARITY_SCORE);
						insights.setPopularity_score(new Double(popularity_score_agg.getValue()).intValue());
					*/}

					else {

						Cardinality story_count = bucket.getAggregations().get("story_count");
						insights.setStory_count(new Long(story_count.getValue()).intValue());

						Cardinality ia_story_count = bucket.getAggregations().get("ia_story_count");
						insights.setIa_story_count(new Long(ia_story_count.getValue()).intValue());

						Filter link_filter = bucket.getAggregations().get("links");

						Sum link_total_reach = link_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
						insights.setLinks_total_reach(new Double(link_total_reach.getValue()).longValue());

						Sum link_unique_reach = link_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
						insights.setLinks_unique_reach(new Double(link_unique_reach.getValue()).longValue());

						Sum link_clicked = link_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
						insights.setLinks_clicked(new Double(link_clicked.getValue()).longValue());

						Sum link_reaction_angry = link_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
						insights.setLinks_reaction_angry(new Double(link_reaction_angry.getValue()).intValue());

						Sum link_reaction_haha = link_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
						insights.setLinks_reaction_haha(new Double(link_reaction_haha.getValue()).intValue());

						Sum link_reaction_love = link_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
						insights.setLinks_reaction_love(new Double(link_reaction_love.getValue()).intValue());

						Sum link_reaction_sad = link_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
						insights.setLinks_reaction_sad(new Double(link_reaction_sad.getValue()).intValue());

						Sum link_reaction_thankful = link_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
						insights.setLinks_reaction_thankful(new Double(link_reaction_thankful.getValue()).intValue());

						Sum link_reaction_wow = link_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
						insights.setLinks_reaction_wow(new Double(link_reaction_wow.getValue()).intValue());

						Sum link_hide_all_clicks = link_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
						insights.setLinks_hide_all_clicks(new Double(link_hide_all_clicks.getValue()).intValue());

						Sum link_hide_clicks = link_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
						insights.setLinks_hide_clicks(new Double(link_hide_clicks.getValue()).intValue());

						Sum link_report_spam_clicks = link_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
						insights.setLinks_report_spam_clicks(new Double(link_report_spam_clicks.getValue()).intValue());

						Sum link_likes = link_filter.getAggregations().get(WisdomConstants.LIKES);
						insights.setLinks_likes(new Double(link_likes.getValue()).intValue());

						Sum link_comments = link_filter.getAggregations().get(WisdomConstants.COMMENTS);
						insights.setLinks_comments(new Double(link_comments.getValue()).intValue());

						Sum link_shares = link_filter.getAggregations().get(WisdomConstants.SHARES);
						insights.setLinks_shares(new Double(link_shares.getValue()).intValue());

						Cardinality link_count = link_filter.getAggregations().get("story_count");
						insights.setLinks_count(new Long(link_count.getValue()).intValue());

						if (link_unique_reach.getValue() > 0) {
							insights.setLinks_shareability((link_shares.getValue() / link_unique_reach.getValue()) * 100);
						}

						if (new Double(link_total_reach.getValue()).intValue() == 0
								|| new Double(link_clicked.getValue()).intValue() == 0)
							insights.setLinks_ctr(0.0);
						else
							insights.setLinks_ctr((link_clicked.getValue() / link_total_reach.getValue()) * 100.0);

						Filter photo_filter = bucket.getAggregations().get("photos");

						Sum photo_total_reach = photo_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
						insights.setPhotos_total_reach(new Double(photo_total_reach.getValue()).longValue());

						Sum photo_unique_reach = photo_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
						insights.setPhotos_unique_reach(new Double(photo_unique_reach.getValue()).longValue());

						Sum photo_clicked = photo_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
						insights.setPhotos_clicked(new Double(photo_clicked.getValue()).longValue());

						Sum photo_reaction_angry = photo_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
						insights.setPhotos_reaction_angry(new Double(photo_reaction_angry.getValue()).intValue());

						Sum photo_reaction_haha = photo_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
						insights.setPhotos_reaction_haha(new Double(photo_reaction_haha.getValue()).intValue());

						Sum photo_reaction_love = photo_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
						insights.setPhotos_reaction_love(new Double(photo_reaction_love.getValue()).intValue());

						Sum photo_reaction_sad = photo_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
						insights.setPhotos_reaction_sad(new Double(photo_reaction_sad.getValue()).intValue());

						Sum photo_reaction_thankful = photo_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
						insights.setPhotos_reaction_thankful(new Double(photo_reaction_thankful.getValue()).intValue());

						Sum photo_reaction_wow = photo_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
						insights.setPhotos_reaction_wow(new Double(photo_reaction_wow.getValue()).intValue());

						Sum photo_hide_all_clicks = photo_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
						insights.setPhotos_hide_all_clicks(new Double(photo_hide_all_clicks.getValue()).intValue());

						Sum photo_hide_clicks = photo_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
						insights.setPhotos_hide_clicks(new Double(photo_hide_clicks.getValue()).intValue());

						Sum photo_report_spam_clicks = photo_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
						insights.setPhotos_report_spam_clicks(new Double(photo_report_spam_clicks.getValue()).intValue());

						Sum photo_likes = photo_filter.getAggregations().get(WisdomConstants.LIKES);
						insights.setPhotos_likes(new Double(photo_likes.getValue()).intValue());

						Sum photo_comments = photo_filter.getAggregations().get(WisdomConstants.COMMENTS);
						insights.setPhotos_comments(new Double(photo_comments.getValue()).intValue());

						Sum photo_shares = photo_filter.getAggregations().get(WisdomConstants.SHARES);
						insights.setPhotos_shares(new Double(photo_shares.getValue()).intValue());

						if (photo_unique_reach.getValue() > 0) {
							insights.setPhotos_shareability(
									(photo_shares.getValue() / photo_unique_reach.getValue()) * 100);
						}

						Cardinality photo_count = photo_filter.getAggregations().get("story_count");
						insights.setPhotos_count(new Long(photo_count.getValue()).intValue());

						if (new Double(photo_total_reach.getValue()).intValue() == 0
								|| new Double(photo_clicked.getValue()).intValue() == 0)
							insights.setPhotos_ctr(0.0);
						else
							insights.setPhotos_ctr((photo_clicked.getValue() / photo_total_reach.getValue()) * 100.0);

						Filter video_filter = bucket.getAggregations().get("videos");

						Sum video_total_reach = video_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
						insights.setVideos_total_reach(new Double(video_total_reach.getValue()).intValue());

						Sum video_unique_reach = video_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
						insights.setVideos_unique_reach(new Double(video_unique_reach.getValue()).intValue());

						Sum video_clicked = video_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
						insights.setVideos_clicked(new Double(video_clicked.getValue()).intValue());

						Sum video_reaction_angry = video_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
						insights.setVideos_reaction_angry(new Double(video_reaction_angry.getValue()).intValue());

						Sum video_reaction_haha = video_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
						insights.setVideos_reaction_haha(new Double(video_reaction_haha.getValue()).intValue());

						Sum video_reaction_love = video_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
						insights.setVideos_reaction_love(new Double(video_reaction_love.getValue()).intValue());

						Sum video_reaction_sad = video_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
						insights.setVideos_reaction_sad(new Double(video_reaction_sad.getValue()).intValue());

						Sum video_reaction_thankful = video_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
						insights.setVideos_reaction_thankful(new Double(video_reaction_thankful.getValue()).intValue());

						Sum video_reaction_wow = video_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
						insights.setVideos_reaction_wow(new Double(video_reaction_wow.getValue()).intValue());

						Sum video_hide_all_clicks = video_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
						insights.setVideos_hide_all_clicks(new Double(video_hide_all_clicks.getValue()).intValue());

						Sum video_hide_clicks = video_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
						insights.setVideos_hide_clicks(new Double(video_hide_clicks.getValue()).intValue());

						Sum video_report_spam_clicks = video_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
						insights.setVideos_report_spam_clicks(new Double(video_report_spam_clicks.getValue()).intValue());

						Sum video_likes = video_filter.getAggregations().get(WisdomConstants.LIKES);
						insights.setVideos_likes(new Double(video_likes.getValue()).intValue());

						Sum video_comments = video_filter.getAggregations().get(WisdomConstants.COMMENTS);
						insights.setVideos_comments(new Double(video_comments.getValue()).intValue());

						Sum video_shares = video_filter.getAggregations().get(WisdomConstants.SHARES);
						insights.setVideos_shares(new Double(video_shares.getValue()).intValue());

						if (video_unique_reach.getValue() > 0) {
							insights.setVideos_shareability(
									(video_shares.getValue() / video_unique_reach.getValue()) * 100);
						}

						Cardinality video_count = video_filter.getAggregations().get("story_count");
						insights.setVideos_count(new Long(video_count.getValue()).intValue());

						if (new Double(video_total_reach.getValue()).intValue() == 0
								|| new Double(video_clicked.getValue()).intValue() == 0)
							insights.setVideos_ctr(0.0);
						else
							insights.setVideos_ctr((video_clicked.getValue() / video_total_reach.getValue()) * 100.0);
						if (query.getInterval().equals("1d")) {
							insights.setWeekDay(DateUtil.getWeekDay(bucket.getKeyAsString()));
						}
						/*if (popularity.keySet().contains(bucket.getKeyAsString())) {
							insights.setPopular_count(popularity.get(bucket.getKeyAsString()).intValue());
						}*/
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
	
	
/*	private Map<String, Long> getFacebookPopularity(WisdomProductQuery query) {
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
				qb.must(QueryBuilders.rangeQuery(WisdomConstants.POPULARITY_SCORE).gte(0));
				if (query.getInterval().contains("M")) {
					qb.must(QueryBuilders.termQuery(WisdomConstants.INTERVAL, WisdomConstants.MONTH));
				}

				else if (query.getInterval().contains("d")) {
					qb.must(QueryBuilders.termQuery(WisdomConstants.INTERVAL, WisdomConstants.DAY));
				}

				else if (query.getInterval().contains("h")) {
					qb.must(QueryBuilders.termQuery(WisdomConstants.INTERVAL, WisdomConstants.HOUR));
				}
				aggBuilder
				.subAggregation(AggregationBuilders.cardinality("storyCount").field(WisdomConstants.STORY_ID_FIELD));
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
	}*/
	
	private BoolQueryBuilder getSocialDecodeQuery(WisdomProductQuery query) {
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery();

		/*if (query.getGa_cat_name() != null && !query.getGa_cat_name().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.GA_CAT_NAME, query.getGa_cat_name()));
		}

		else if (query.getPp_cat_name() != null && !query.getPp_cat_name().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(WisdomConstants.WisdomNextConstant.PP_CAT_NAME, query.getPp_cat_name()));
		}*/

		if (query.getStoryid() != null && !query.getStoryid().isEmpty()) {
			mainQuery.must(QueryBuilders.termQuery(WisdomConstants.STORY_ID, query.getStoryid()));
		} 
		else {
			mainQuery.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()));
			BoolQueryBuilder dateQuery = QueryBuilders.boolQuery();
			dateQuery.should(
					QueryBuilders.rangeQuery(query.getDateField()).gte(query.getStartDate()).lte(query.getEndDate()));
			if (!query.isPrevDayRequired() && query.getCompareEndDate() != null
					&& query.getCompareStartDate() != null) {
				dateQuery.should(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getCompareStartDate())
						.lte(query.getCompareEndDate()));
			}
			mainQuery.must(dateQuery);
		}

		/*if (query.getUrl_domain() != null) {
			mainQuery.must(QueryBuilders.termQuery(WisdomConstants.URL_DOMAIN, query.getUrl_domain()));
		}*/

		if (query.getDateField().equals(WisdomConstants.CREATED_DATETIME) && query.getProfile_id() != null) {
			mainQuery.must(QueryBuilders.termQuery(WisdomConstants.PROFILE_ID, query.getProfile_id()));
		}
		/*if (query.getSuper_cat_id() != null && !query.getSuper_cat_id().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(WisdomConstants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}*/
		if (query.getAuthorId() != null && !query.getAuthorId().isEmpty()) {
			mainQuery.must(QueryBuilders.termsQuery(WisdomConstants.AUTHOR_ID, query.getAuthorId()));
		}	
		return mainQuery;
	}
	
	private FacebookInsights getPrevDayFacebookInsights(WisdomProductQuery query) {
		FacebookInsights insights = new FacebookInsights();
		
		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();
		if (!StringUtils.isBlank(query.getStartDate())) {
			startDatetime = query.getStartDate();
		}
		if (!StringUtils.isBlank(query.getEndDate())) {
			endDatetime = query.getEndDate();
		}

		BoolQueryBuilder qb = QueryBuilders.boolQuery()
				.must(QueryBuilders.termsQuery(WisdomConstants.DOMAIN_ID, query.getDomainId()))
				.must(QueryBuilders.rangeQuery(query.getDateField()).gte(query.getCompareStartDate())
						.lte(query.getCompareEndDate()));
		/*if (query.getGa_cat_name() != null && !query.getGa_cat_name().isEmpty()) {
			qb.must(QueryBuilders.termsQuery(WisdomConstants.GA_CAT_NAME, query.getGa_cat_name()));
		}
		if (query.getSuper_cat_id() != null) {
			qb.must(QueryBuilders.termsQuery(WisdomConstants.SUPER_CAT_ID, query.getSuper_cat_id()));
		}
		else if (query.getPp_cat_name() != null && !query.getPp_cat_name().isEmpty()) {
			qb.must(QueryBuilders.termsQuery(WisdomConstants.PP_CAT_NAME, query.getPp_cat_name()));
		}*/

		if (query.getDateField().equals(WisdomConstants.CREATED_DATETIME) && query.getProfile_id() != null) {
			qb.must(QueryBuilders.termQuery(WisdomConstants.PROFILE_ID, query.getProfile_id()));
		}

		SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HOURLY, startDatetime, endDatetime)).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(qb)
				.addAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL).field(WisdomConstants.REACTION_THANKFUL))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
				.addAggregation(
						AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS).field(WisdomConstants.REPORT_SPAM_CLICKS))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))
				.addAggregation(AggregationBuilders.sum(WisdomConstants.VIDEO_VIEWS).field(WisdomConstants.VIDEO_VIEWS))
				.addAggregation(
						AggregationBuilders.sum(WisdomConstants.UNIQUE_VIDEO_VIEWS).field(WisdomConstants.UNIQUE_VIDEO_VIEWS))
				.addAggregation(AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID))
				.addAggregation(AggregationBuilders.cardinality("ia_story_count").field(WisdomConstants.IA_STORYID))
				.addAggregation(AggregationBuilders.filter("links",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "link"))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL).field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID)))
				.addAggregation(AggregationBuilders.filter("photos",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "photo"))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL).field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID)))
				.addAggregation(AggregationBuilders.filter("videos",QueryBuilders.termsQuery(WisdomConstants.STATUS_TYPE, "video"))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL).field(WisdomConstants.REACTION_THANKFUL))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
						.subAggregation(
								AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
								.field(WisdomConstants.REPORT_SPAM_CLICKS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
						.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
						.subAggregation(AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID)))
				.setSize(0).execute().actionGet();
		Sum total_reach_agg = res.getAggregations().get(WisdomConstants.TOTAL_REACH);
		insights.setTotal_reach(new Double(total_reach_agg.getValue()).longValue());

		Sum unique_reach_agg = res.getAggregations().get(WisdomConstants.UNIQUE_REACH);
		insights.setUnique_reach(new Double(unique_reach_agg.getValue()).longValue());

		Sum link_clicks_agg = res.getAggregations().get(WisdomConstants.LINK_CLICKS);
		insights.setLink_clicks(new Double(link_clicks_agg.getValue()).longValue());

		Sum reaction_angry_agg = res.getAggregations().get(WisdomConstants.REACTION_ANGRY);
		insights.setReaction_angry(new Double(reaction_angry_agg.getValue()).intValue());

		Sum reaction_haha_agg = res.getAggregations().get(WisdomConstants.REACTION_HAHA);
		insights.setReaction_haha(new Double(reaction_haha_agg.getValue()).intValue());

		Sum reaction_love_agg = res.getAggregations().get(WisdomConstants.REACTION_LOVE);
		insights.setReaction_love(new Double(reaction_love_agg.getValue()).intValue());

		Sum reaction_sad_agg = res.getAggregations().get(WisdomConstants.REACTION_SAD);
		insights.setReaction_sad(new Double(reaction_sad_agg.getValue()).intValue());

		Sum reaction_thankful_agg = res.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
		insights.setReaction_thankful(new Double(reaction_thankful_agg.getValue()).intValue());

		Sum reaction_wow_agg = res.getAggregations().get(WisdomConstants.REACTION_WOW);
		insights.setReaction_wow(new Double(reaction_wow_agg.getValue()).intValue());

		Sum hide_all_clicks_agg = res.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
		insights.setHide_all_clicks(new Double(hide_all_clicks_agg.getValue()).intValue());

		Sum hide_clicks_agg = res.getAggregations().get(WisdomConstants.HIDE_CLICKS);
		insights.setHide_clicks(new Double(hide_clicks_agg.getValue()).intValue());

		Sum report_spam_clicks_agg = res.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
		insights.setReport_spam_clicks(new Double(report_spam_clicks_agg.getValue()).intValue());

		Sum likes_agg = res.getAggregations().get(WisdomConstants.LIKES);
		insights.setLikes(new Double(likes_agg.getValue()).intValue());

		Sum comments_agg = res.getAggregations().get(WisdomConstants.COMMENTS);
		insights.setComments(new Double(comments_agg.getValue()).intValue());

		Sum shares_agg = res.getAggregations().get(WisdomConstants.SHARES);
		insights.setShares(new Double(shares_agg.getValue()).intValue());

		Sum ia_clicks_agg = res.getAggregations().get(WisdomConstants.IA_CLICKS);
		insights.setIa_clicks(new Double(ia_clicks_agg.getValue()).intValue());

		Cardinality story_count = res.getAggregations().get("story_count");
		insights.setStory_count(new Long(story_count.getValue()).intValue());

		Cardinality ia_story_count = res.getAggregations().get("ia_story_count");
		insights.setIa_story_count(new Long(ia_story_count.getValue()).intValue());

		Sum video_views_agg = res.getAggregations().get(WisdomConstants.VIDEO_VIEWS);
		insights.setVideo_views(new Double(video_views_agg.getValue()).intValue());

		Sum unique_video_views_agg = res.getAggregations().get(WisdomConstants.UNIQUE_VIDEO_VIEWS);
		insights.setUnique_video_views(new Double(unique_video_views_agg.getValue()).intValue());

		if (unique_reach_agg.getValue() > 0) {
			insights.setShareability((shares_agg.getValue() / unique_reach_agg.getValue()) * 100);
		}

		if (new Double(total_reach_agg.getValue()).intValue() == 0
				|| new Double(link_clicks_agg.getValue()).intValue() == 0)
			insights.setCtr(0.0);
		else
			insights.setCtr((link_clicks_agg.getValue() / total_reach_agg.getValue()) * 100.0);

		Filter link_filter = res.getAggregations().get("links");

		Sum link_total_reach = link_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
		insights.setLinks_total_reach(new Double(link_total_reach.getValue()).longValue());

		Sum link_unique_reach = link_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
		insights.setLinks_unique_reach(new Double(link_unique_reach.getValue()).longValue());

		Sum link_clicked = link_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
		insights.setLinks_clicked(new Double(link_clicked.getValue()).longValue());

		Sum link_reaction_angry = link_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
		insights.setLinks_reaction_angry(new Double(link_reaction_angry.getValue()).intValue());

		Sum link_reaction_haha = link_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
		insights.setLinks_reaction_haha(new Double(link_reaction_haha.getValue()).intValue());

		Sum link_reaction_love = link_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
		insights.setLinks_reaction_love(new Double(link_reaction_love.getValue()).intValue());

		Sum link_reaction_sad = link_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
		insights.setLinks_reaction_sad(new Double(link_reaction_sad.getValue()).intValue());

		Sum link_reaction_thankful = link_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
		insights.setLinks_reaction_thankful(new Double(link_reaction_thankful.getValue()).intValue());

		Sum link_reaction_wow = link_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
		insights.setLinks_reaction_wow(new Double(link_reaction_wow.getValue()).intValue());

		Sum link_hide_all_clicks = link_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
		insights.setLinks_hide_all_clicks(new Double(link_hide_all_clicks.getValue()).intValue());

		Sum link_hide_clicks = link_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
		insights.setLinks_hide_clicks(new Double(link_hide_clicks.getValue()).intValue());

		Sum link_report_spam_clicks = link_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
		insights.setLinks_report_spam_clicks(new Double(link_report_spam_clicks.getValue()).intValue());

		Sum link_likes = link_filter.getAggregations().get(WisdomConstants.LIKES);
		insights.setLinks_likes(new Double(link_likes.getValue()).intValue());

		Sum link_comments = link_filter.getAggregations().get(WisdomConstants.COMMENTS);
		insights.setLinks_comments(new Double(link_comments.getValue()).intValue());

		Sum link_shares = link_filter.getAggregations().get(WisdomConstants.SHARES);
		insights.setLinks_shares(new Double(link_shares.getValue()).intValue());

		if (link_unique_reach.getValue() > 0) {
			insights.setLinks_shareability((link_shares.getValue() / link_unique_reach.getValue()) * 100);
		}

		Cardinality link_count = link_filter.getAggregations().get("story_count");
		insights.setLinks_count(new Long(link_count.getValue()).intValue());

		if (new Double(link_total_reach.getValue()).intValue() == 0
				|| new Double(link_clicked.getValue()).intValue() == 0)
			insights.setLinks_ctr(0.0);
		else
			insights.setLinks_ctr((link_clicked.getValue() / link_total_reach.getValue()) * 100.0);

		Filter photo_filter = res.getAggregations().get("photos");

		Sum photo_total_reach = photo_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
		insights.setPhotos_total_reach(new Double(photo_total_reach.getValue()).longValue());

		Sum photo_unique_reach = photo_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
		insights.setPhotos_unique_reach(new Double(photo_unique_reach.getValue()).longValue());

		Sum photo_clicked = photo_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
		insights.setPhotos_clicked(new Double(photo_clicked.getValue()).longValue());

		Sum photo_reaction_angry = photo_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
		insights.setPhotos_reaction_angry(new Double(photo_reaction_angry.getValue()).intValue());

		Sum photo_reaction_haha = photo_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
		insights.setPhotos_reaction_haha(new Double(photo_reaction_haha.getValue()).intValue());

		Sum photo_reaction_love = photo_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
		insights.setPhotos_reaction_love(new Double(photo_reaction_love.getValue()).intValue());

		Sum photo_reaction_sad = photo_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
		insights.setPhotos_reaction_sad(new Double(photo_reaction_sad.getValue()).intValue());

		Sum photo_reaction_thankful = photo_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
		insights.setPhotos_reaction_thankful(new Double(photo_reaction_thankful.getValue()).intValue());

		Sum photo_reaction_wow = photo_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
		insights.setPhotos_reaction_wow(new Double(photo_reaction_wow.getValue()).intValue());

		Sum photo_hide_all_clicks = photo_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
		insights.setPhotos_hide_all_clicks(new Double(photo_hide_all_clicks.getValue()).intValue());

		Sum photo_hide_clicks = photo_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
		insights.setPhotos_hide_clicks(new Double(photo_hide_clicks.getValue()).intValue());

		Sum photo_report_spam_clicks = photo_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
		insights.setPhotos_report_spam_clicks(new Double(photo_report_spam_clicks.getValue()).intValue());

		Sum photo_likes = photo_filter.getAggregations().get(WisdomConstants.LIKES);
		insights.setPhotos_likes(new Double(photo_likes.getValue()).intValue());

		Sum photo_comments = photo_filter.getAggregations().get(WisdomConstants.COMMENTS);
		insights.setPhotos_comments(new Double(photo_comments.getValue()).intValue());

		Sum photo_shares = photo_filter.getAggregations().get(WisdomConstants.SHARES);
		insights.setPhotos_shares(new Double(photo_shares.getValue()).intValue());

		if (photo_unique_reach.getValue() > 0) {
			insights.setPhotos_shareability((photo_shares.getValue() / photo_unique_reach.getValue()) * 100);
		}

		Cardinality photo_count = photo_filter.getAggregations().get("story_count");
		insights.setPhotos_count(new Long(photo_count.getValue()).intValue());

		if (new Double(photo_total_reach.getValue()).intValue() == 0
				|| new Double(photo_clicked.getValue()).intValue() == 0)
			insights.setPhotos_ctr(0.0);
		else
			insights.setPhotos_ctr((photo_clicked.getValue() / photo_total_reach.getValue()) * 100.0);

		Filter video_filter = res.getAggregations().get("videos");

		Sum video_total_reach = video_filter.getAggregations().get(WisdomConstants.TOTAL_REACH);
		insights.setVideos_total_reach(new Double(video_total_reach.getValue()).intValue());

		Sum video_unique_reach = video_filter.getAggregations().get(WisdomConstants.UNIQUE_REACH);
		insights.setVideos_unique_reach(new Double(video_unique_reach.getValue()).intValue());

		Sum video_clicked = video_filter.getAggregations().get(WisdomConstants.LINK_CLICKS);
		insights.setVideos_clicked(new Double(video_clicked.getValue()).intValue());

		Sum video_reaction_angry = video_filter.getAggregations().get(WisdomConstants.REACTION_ANGRY);
		insights.setVideos_reaction_angry(new Double(video_reaction_angry.getValue()).intValue());

		Sum video_reaction_haha = video_filter.getAggregations().get(WisdomConstants.REACTION_HAHA);
		insights.setVideos_reaction_haha(new Double(video_reaction_haha.getValue()).intValue());

		Sum video_reaction_love = video_filter.getAggregations().get(WisdomConstants.REACTION_LOVE);
		insights.setVideos_reaction_love(new Double(video_reaction_love.getValue()).intValue());

		Sum video_reaction_sad = video_filter.getAggregations().get(WisdomConstants.REACTION_SAD);
		insights.setVideos_reaction_sad(new Double(video_reaction_sad.getValue()).intValue());

		Sum video_reaction_thankful = video_filter.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
		insights.setVideos_reaction_thankful(new Double(video_reaction_thankful.getValue()).intValue());

		Sum video_reaction_wow = video_filter.getAggregations().get(WisdomConstants.REACTION_WOW);
		insights.setVideos_reaction_wow(new Double(video_reaction_wow.getValue()).intValue());

		Sum video_hide_all_clicks = video_filter.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
		insights.setVideos_hide_all_clicks(new Double(video_hide_all_clicks.getValue()).intValue());

		Sum video_hide_clicks = video_filter.getAggregations().get(WisdomConstants.HIDE_CLICKS);
		insights.setVideos_hide_clicks(new Double(video_hide_clicks.getValue()).intValue());

		Sum video_report_spam_clicks = video_filter.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
		insights.setVideos_report_spam_clicks(new Double(video_report_spam_clicks.getValue()).intValue());

		Sum video_likes = video_filter.getAggregations().get(WisdomConstants.LIKES);
		insights.setVideos_likes(new Double(video_likes.getValue()).intValue());

		Sum video_comments = video_filter.getAggregations().get(WisdomConstants.COMMENTS);
		insights.setVideos_comments(new Double(video_comments.getValue()).intValue());

		Sum video_shares = video_filter.getAggregations().get(WisdomConstants.SHARES);
		insights.setVideos_shares(new Double(video_shares.getValue()).intValue());

		if (video_unique_reach.getValue() > 0) {
			insights.setVideos_shareability((video_shares.getValue() / video_unique_reach.getValue()) * 100);
		}

		Cardinality video_count = video_filter.getAggregations().get("story_count");
		insights.setVideos_count(new Long(video_count.getValue()).intValue());

		if (new Double(video_total_reach.getValue()).intValue() == 0
				|| new Double(video_clicked.getValue()).intValue() == 0)
			insights.setVideos_ctr(0.0);
		else
			insights.setVideos_ctr((video_clicked.getValue() / video_total_reach.getValue()) * 100.0);

		/*qb.must(QueryBuilders.termQuery(WisdomConstants.INTERVAL, WisdomConstants.DAY))
		.must(QueryBuilders.rangeQuery(WisdomConstants.POPULARITY_SCORE).gte(0));*/

		SearchResponse popRes = client.prepareSearch(Indexes.FB_INSIGHTS_POPULAR)
				.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(qb)
				.addAggregation(AggregationBuilders.cardinality("storyCount").field(WisdomConstants.STORY_ID))
				.setSize(0).execute().actionGet();

		Cardinality popStories = popRes.getAggregations().get("storyCount");
		insights.setPopular_count(((Long) popStories.getValue()).intValue());
		return insights;
	}
	
	public Map<String, FacebookInsights> getFacebookInsightsByIntervalAndCategory(WisdomProductQuery query) {
		Map<String, FacebookInsights> records = new TreeMap<>();
		long startTime = System.currentTimeMillis();

		try {
			String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
			String endDatetime = DateUtil.getCurrentDateTime();
			if (!StringUtils.isBlank(query.getStartDate())) {
				startDatetime = query.getStartDate();
			}
			if (!StringUtils.isBlank(query.getEndDate())) {
				endDatetime = query.getEndDate();
			}

			String interval = query.getInterval();
			String parameter = query.getParameter();
			DateHistogramInterval histInterval = new DateHistogramInterval(interval);

			TermsAggregationBuilder catAgg = null; //new TermsBuilder("category");

		
		    catAgg = AggregationBuilders.terms("category").field(WisdomConstants.CATEGORY_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(parameter, false));
		    
			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HOURLY, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(AggregationBuilders.dateHistogram("interval").field(query.getDateField())
							.format("yyyy-MM-dd'T'HH:mm:ss'Z'").dateHistogramInterval(histInterval)
							.subAggregation(
									AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID))
							.subAggregation(
									AggregationBuilders.cardinality("ia_story_count").field(WisdomConstants.IA_STORYID))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
									.field(WisdomConstants.REACTION_THANKFUL))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
									.field(WisdomConstants.REPORT_SPAM_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))
							.subAggregation(catAgg
									.subAggregation(AggregationBuilders.cardinality("story_count").field(
											WisdomConstants.STORY_ID))
									.subAggregation(AggregationBuilders.cardinality("ia_story_count")
											.field(WisdomConstants.IA_STORYID))
									.subAggregation(
											AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH)
											.field(WisdomConstants.UNIQUE_REACH))
									.subAggregation(
											AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
											.field(WisdomConstants.REACTION_THANKFUL))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_SAD)
											.field(WisdomConstants.REACTION_SAD))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY)
											.field(WisdomConstants.REACTION_ANGRY))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_WOW)
											.field(WisdomConstants.REACTION_WOW))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_HAHA)
											.field(WisdomConstants.REACTION_HAHA))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_LOVE)
											.field(WisdomConstants.REACTION_LOVE))
									.subAggregation(
											AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS)
											.field(WisdomConstants.HIDE_ALL_CLICKS))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
											.field(WisdomConstants.REPORT_SPAM_CLICKS))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
									.subAggregation(
											AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
									.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
									.subAggregation(
											AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))))
					.setSize(0).execute().actionGet();
			Histogram histogram = res.getAggregations().get("interval");
			for (Histogram.Bucket bucket : histogram.getBuckets()) {
				if(bucket.getDocCount()>0){
					FacebookInsights insights = new FacebookInsights();
					List<Object> categoryInsights = new ArrayList<>();

					Cardinality story_count = bucket.getAggregations().get("story_count");
					insights.setStory_count(new Long(story_count.getValue()).intValue());

					Cardinality ia_story_count = bucket.getAggregations().get("ia_story_count");
					insights.setIa_story_count(new Long(ia_story_count.getValue()).intValue());

					Sum total_reach_agg = bucket.getAggregations().get(WisdomConstants.TOTAL_REACH);
					insights.setTotal_reach(new Double(total_reach_agg.getValue()).longValue());

					Sum unique_reach_agg = bucket.getAggregations().get(WisdomConstants.UNIQUE_REACH);
					insights.setUnique_reach(new Double(unique_reach_agg.getValue()).longValue());

					Sum link_clicks_agg = bucket.getAggregations().get(WisdomConstants.LINK_CLICKS);
					insights.setLink_clicks(new Double(link_clicks_agg.getValue()).longValue());

					Sum reaction_angry_agg = bucket.getAggregations().get(WisdomConstants.REACTION_ANGRY);
					insights.setReaction_angry(new Double(reaction_angry_agg.getValue()).intValue());

					Sum reaction_haha_agg = bucket.getAggregations().get(WisdomConstants.REACTION_HAHA);
					insights.setReaction_haha(new Double(reaction_haha_agg.getValue()).intValue());

					Sum reaction_love_agg = bucket.getAggregations().get(WisdomConstants.REACTION_LOVE);
					insights.setReaction_love(new Double(reaction_love_agg.getValue()).intValue());

					Sum reaction_sad_agg = bucket.getAggregations().get(WisdomConstants.REACTION_SAD);
					insights.setReaction_sad(new Double(reaction_sad_agg.getValue()).intValue());

					Sum reaction_thankful_agg = bucket.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
					insights.setReaction_thankful(new Double(reaction_thankful_agg.getValue()).intValue());

					Sum reaction_wow_agg = bucket.getAggregations().get(WisdomConstants.REACTION_WOW);
					insights.setReaction_wow(new Double(reaction_wow_agg.getValue()).intValue());

					Sum hide_all_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
					insights.setHide_all_clicks(new Double(hide_all_clicks_agg.getValue()).intValue());

					Sum hide_clicks_agg = bucket.getAggregations().get(WisdomConstants.HIDE_CLICKS);
					insights.setHide_clicks(new Double(hide_clicks_agg.getValue()).intValue());

					Sum report_spam_clicks_agg = bucket.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
					insights.setReport_spam_clicks(new Double(report_spam_clicks_agg.getValue()).intValue());

					Sum likes_agg = bucket.getAggregations().get(WisdomConstants.LIKES);
					insights.setLikes(new Double(likes_agg.getValue()).intValue());

					Sum comments_agg = bucket.getAggregations().get(WisdomConstants.COMMENTS);
					insights.setComments(new Double(comments_agg.getValue()).intValue());

					Sum shares_agg = bucket.getAggregations().get(WisdomConstants.SHARES);
					insights.setShares(new Double(shares_agg.getValue()).intValue());

					Sum ia_clicks_agg = bucket.getAggregations().get(WisdomConstants.IA_CLICKS);
					insights.setIa_clicks(new Double(ia_clicks_agg.getValue()).intValue());

					Terms categories = bucket.getAggregations().get("category");
					for (Terms.Bucket catBucket : categories.getBuckets()) {
						if (!StringUtils.isBlank(catBucket.getKeyAsString())) {
							Map<String, Object> catMap = new HashMap<>();
							catMap.put(WisdomConstants.CATEGORY_NAME, catBucket.getKey());

							Sum total_reach = catBucket.getAggregations().get(WisdomConstants.TOTAL_REACH);
							Sum unique_reach = catBucket.getAggregations().get(WisdomConstants.UNIQUE_REACH);
							Sum link_clicks = catBucket.getAggregations().get(WisdomConstants.LINK_CLICKS);
							Sum reaction_angry = catBucket.getAggregations().get(WisdomConstants.REACTION_ANGRY);
							Sum reaction_haha = catBucket.getAggregations().get(WisdomConstants.REACTION_HAHA);
							Sum reaction_love = catBucket.getAggregations().get(WisdomConstants.REACTION_LOVE);
							Sum reaction_sad = catBucket.getAggregations().get(WisdomConstants.REACTION_SAD);
							Sum reaction_thankful = catBucket.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
							Sum reaction_wow = catBucket.getAggregations().get(WisdomConstants.REACTION_WOW);
							Sum hide_all_clicks = catBucket.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
							Sum hide_clicks = catBucket.getAggregations().get(WisdomConstants.HIDE_CLICKS);
							Sum report_spam_clicks = catBucket.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
							Sum likes = catBucket.getAggregations().get(WisdomConstants.LIKES);
							Sum comments = catBucket.getAggregations().get(WisdomConstants.COMMENTS);
							Sum shares = catBucket.getAggregations().get(WisdomConstants.SHARES);
							Sum ia_clicks = catBucket.getAggregations().get(WisdomConstants.IA_CLICKS);
							Cardinality cat_story_count = catBucket.getAggregations().get("story_count");
							Cardinality cat_ia_story_count = catBucket.getAggregations().get("ia_story_count");
							//catMap.put(WisdomConstants.STORY_COUNT, cat_story_count.getValue());
							//catMap.put(WisdomConstants.IA_STORY_COUNT, cat_ia_story_count.getValue());
							catMap.put(WisdomConstants.TOTAL_REACH, total_reach.getValue());
							catMap.put(WisdomConstants.UNIQUE_REACH, unique_reach.getValue());
							catMap.put(WisdomConstants.LINK_CLICKS, link_clicks.getValue());
							catMap.put(WisdomConstants.REACTION_ANGRY, reaction_angry.getValue());
							catMap.put(WisdomConstants.REACTION_HAHA, reaction_haha.getValue());
							catMap.put(WisdomConstants.REACTION_LOVE, reaction_love.getValue());
							catMap.put(WisdomConstants.REACTION_SAD, reaction_sad.getValue());
							catMap.put(WisdomConstants.REACTION_THANKFUL, reaction_thankful.getValue());
							catMap.put(WisdomConstants.REACTION_WOW, reaction_wow.getValue());
							catMap.put(WisdomConstants.HIDE_ALL_CLICKS, hide_all_clicks.getValue());
							catMap.put(WisdomConstants.HIDE_CLICKS, hide_clicks.getValue());
							catMap.put(WisdomConstants.REPORT_SPAM_CLICKS, report_spam_clicks.getValue());
							catMap.put(WisdomConstants.LIKES, likes.getValue());
							catMap.put(WisdomConstants.COMMENTS, comments.getValue());
							catMap.put(WisdomConstants.SHARES, shares.getValue());
							catMap.put(WisdomConstants.IA_CLICKS, ia_clicks.getValue());

							categoryInsights.add(catMap);
						}
					}

					if (categoryInsights.size() > 0) {
						insights.setCategoryInsights(categoryInsights);
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
	
	public List<FacebookInsights> getFacebookInsightsByCategory(WisdomProductQuery query) {
		String startDatetime = DateUtil.getCurrentDate().replaceAll("_", "-");
		String endDatetime = DateUtil.getCurrentDateTime();
		if (!StringUtils.isBlank(query.getStartDate())) {
			startDatetime = query.getStartDate();
		}
		if (!StringUtils.isBlank(query.getEndDate())) {
			endDatetime = query.getEndDate();
		}
		List<FacebookInsights> records = new ArrayList<>();
		long startTime = System.currentTimeMillis();

		try {
			TermsAggregationBuilder catAgg =null;// new TermsBuilder("category");
			
			catAgg = AggregationBuilders.terms("category").field(WisdomConstants.CATEGORY_NAME)
						.size(query.getCategorySize()).order(Order.aggregation(WisdomConstants.TOTAL_REACH, false));

			SearchResponse res = client.prepareSearch(IndexUtils.getMonthlyIndexes(Indexes.WisdomIndexes.WISDOM_FB_INSIGHTS_HOURLY, startDatetime, endDatetime))
					.setTypes(MappingTypes.MAPPING_REALTIME).setQuery(getSocialDecodeQuery(query))
					.addAggregation(catAgg
							.subAggregation(
									AggregationBuilders.cardinality("story_count").field(WisdomConstants.STORY_ID))
							.subAggregation(
									AggregationBuilders.cardinality("ia_story_count").field(WisdomConstants.IA_STORYID))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.TOTAL_REACH).field(WisdomConstants.TOTAL_REACH))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.UNIQUE_REACH).field(WisdomConstants.UNIQUE_REACH))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LINK_CLICKS).field(WisdomConstants.LINK_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REACTION_THANKFUL)
									.field(WisdomConstants.REACTION_THANKFUL))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_SAD).field(WisdomConstants.REACTION_SAD))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_ANGRY).field(WisdomConstants.REACTION_ANGRY))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_WOW).field(WisdomConstants.REACTION_WOW))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_HAHA).field(WisdomConstants.REACTION_HAHA))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.REACTION_LOVE).field(WisdomConstants.REACTION_LOVE))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.HIDE_CLICKS).field(WisdomConstants.HIDE_CLICKS))
							.subAggregation(
									AggregationBuilders.sum(WisdomConstants.HIDE_ALL_CLICKS).field(WisdomConstants.HIDE_ALL_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.REPORT_SPAM_CLICKS)
									.field(WisdomConstants.REPORT_SPAM_CLICKS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.LIKES).field(WisdomConstants.LIKES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.COMMENTS).field(WisdomConstants.COMMENTS))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.SHARES).field(WisdomConstants.SHARES))
							.subAggregation(AggregationBuilders.sum(WisdomConstants.IA_CLICKS).field(WisdomConstants.IA_CLICKS))
							.order(Order.aggregation("total_reach", query.isOrderAsc())))
					.setSize(0).execute().actionGet();

			Terms categories = res.getAggregations().get("category");
			for (Terms.Bucket catBucket : categories.getBuckets()) {
				if (!StringUtils.isBlank(catBucket.getKeyAsString())) {
					FacebookInsights insights = new FacebookInsights();
					Cardinality cat_story_count = catBucket.getAggregations().get("story_count");
					Cardinality cat_ia_story_count = catBucket.getAggregations().get("ia_story_count");
					insights.setGa_cat_name(catBucket.getKeyAsString());
					insights.setStory_count(((Long) cat_story_count.getValue()).intValue());
					insights.setIa_story_count(((Long) cat_ia_story_count.getValue()).intValue());

					Sum total_reach_agg = catBucket.getAggregations().get(WisdomConstants.TOTAL_REACH);
					insights.setTotal_reach(new Double(total_reach_agg.getValue()).longValue());

					Sum unique_reach_agg = catBucket.getAggregations().get(WisdomConstants.UNIQUE_REACH);
					insights.setUnique_reach(new Double(unique_reach_agg.getValue()).longValue());

					Sum link_clicks_agg = catBucket.getAggregations().get(WisdomConstants.LINK_CLICKS);
					insights.setLink_clicks(new Double(link_clicks_agg.getValue()).longValue());

					Sum reaction_angry_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_ANGRY);
					insights.setReaction_angry(new Double(reaction_angry_agg.getValue()).intValue());

					Sum reaction_haha_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_HAHA);
					insights.setReaction_haha(new Double(reaction_haha_agg.getValue()).intValue());

					Sum reaction_love_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_LOVE);
					insights.setReaction_love(new Double(reaction_love_agg.getValue()).intValue());

					Sum reaction_sad_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_SAD);
					insights.setReaction_sad(new Double(reaction_sad_agg.getValue()).intValue());

					Sum reaction_thankful_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_THANKFUL);
					insights.setReaction_thankful(new Double(reaction_thankful_agg.getValue()).intValue());

					Sum reaction_wow_agg = catBucket.getAggregations().get(WisdomConstants.REACTION_WOW);
					insights.setReaction_wow(new Double(reaction_wow_agg.getValue()).intValue());

					Sum hide_all_clicks_agg = catBucket.getAggregations().get(WisdomConstants.HIDE_ALL_CLICKS);
					insights.setHide_all_clicks(new Double(hide_all_clicks_agg.getValue()).intValue());

					Sum hide_clicks_agg = catBucket.getAggregations().get(WisdomConstants.HIDE_CLICKS);
					insights.setHide_clicks(new Double(hide_clicks_agg.getValue()).intValue());

					Sum report_spam_clicks_agg = catBucket.getAggregations().get(WisdomConstants.REPORT_SPAM_CLICKS);
					insights.setReport_spam_clicks(new Double(report_spam_clicks_agg.getValue()).intValue());

					Sum likes_agg = catBucket.getAggregations().get(WisdomConstants.LIKES);
					insights.setLikes(new Double(likes_agg.getValue()).intValue());

					Sum comments_agg = catBucket.getAggregations().get(WisdomConstants.COMMENTS);
					insights.setComments(new Double(comments_agg.getValue()).intValue());

					Sum shares_agg = catBucket.getAggregations().get(WisdomConstants.SHARES);
					insights.setShares(new Double(shares_agg.getValue()).intValue());

					Sum ia_clicks_agg = catBucket.getAggregations().get(WisdomConstants.IA_CLICKS);
					insights.setIa_clicks(new Double(ia_clicks_agg.getValue()).intValue());

					records.add(insights);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error while retrieving facebookInsightsByCategory.", e);
		}
		log.info("Retrieving facebookInsightsByCategory; Execution Time:(Seconds) "
				+ (System.currentTimeMillis() - startTime) / 1000.0);
		return records;
	}
	
	public static void main(String[] args) throws Exception {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		WisdomProductQueryExecutorService wqes = new WisdomProductQueryExecutorService();
		WisdomProductQuery query = new WisdomProductQuery();
		ArrayList<String> channel_slno = new ArrayList<>();
		ArrayList<String> tracker = new ArrayList<>();
		ArrayList<String> cat_name = new ArrayList<>();
		cat_name.add("");
		tracker.add("news");
		ArrayList<String> platform = new ArrayList<>();
		platform.add("web");
		platform.add("mobile");
		List<String> excludeTracker = new ArrayList<>();
		ArrayList<String> author = new ArrayList<>();
		author.add("1222");
		excludeTracker = Arrays.asList("news-m-ucb","news-ucb","news-m-ucb_1","news-ucb_1","news-fpaid","news-bgp","news-vpaid","news-opd","news-gpd","news-njp","news-fpamn","news-aff","facebook","news-fbo","news-cht","news-mmax");
		channel_slno.add("www.bhaskar.com");
		//channel_slno.add("3849");
		query.setStartDate("2018-01-09");
		query.setEndDate("2018-01-10");
		//query.setStartPubDate("2017-11-01");
		//query.setEndPubDate("2017-12-28");
		query.setCount(10);
		//query.setTracker(tracker);
		//query.setWeekDay("Tue");
		query.setDomainId(channel_slno);
		//query.setAuthorId(author);
		//query.setPlatform(platform);
		query.setProfile_id("75a7da5805fcc9094f80e1ca1dbcb478");
		//query.setInterval("3h");
		query.setDateField("datetime");
		//query.setCategoryName(cat_name);
		//query.setStatus_type("video");
		//query.setStoryid("27ea3e175f6aac9df3fa40788eca5a42");
		//query.setField("pgno,apvs,ipvs");
		//query.setCategorySize(5);
		//query.setParameter("total_engagement");
		//query.setType(1);
		//query.setMinutes("15");
		//query.setExcludeTracker(excludeTracker);getsli
		//query.setInterval("1M");
		//query.setWidget_name("HP_NEW");
		//query.setTitle("kohli hits century");
		System.out.println(" " + gson.toJson(wqes.getFacebookInsightsByCategory(query)));
	}
}
