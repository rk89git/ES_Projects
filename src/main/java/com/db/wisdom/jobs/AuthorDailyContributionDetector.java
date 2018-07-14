package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.HostType;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class AuthorDailyContributionDetector {

	private Client client = null;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(AuthorDailyContributionDetector.class);

	public AuthorDailyContributionDetector() {
		try {
			initializeClient();
			log.info("Connection initialized with ElasticSearch.");
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	private void initializeClient() {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	public void derivePreviousDayData() {
		long startTime = System.currentTimeMillis();
		try {
			String rowIDSuffix=DateUtil.getPreviousDate();
			String startPubDate = rowIDSuffix.replaceAll("_", "-");
			String includes[] = { Constants.AUTHOR_NAME };
			SearchResponse res = client.prepareSearch(Indexes.STORY_DETAIL).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(QueryBuilders.boolQuery()
							.must(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte(startPubDate)))
					.addAggregation(AggregationBuilders.terms("uid").field(Constants.UID)
							.size(Constants.AUTHOR_COUNT_FOR_DAILY_CONTRIBUTION).order(Order.aggregation("tpvs", false))
							.subAggregation(AggregationBuilders.sum("tpvs").field(Constants.PVS))
							.subAggregation(
									AggregationBuilders.cardinality("storyCount").field(Constants.STORY_ID_FIELD))
							.subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, new String[] {})
									.size(1))
							.subAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE).size(10)
									.subAggregation(AggregationBuilders.sum("pvs").field(Constants.PVS))))
					.setSize(0).execute().actionGet();

			List<Map<String, Object>> authorDailyContributionList = new ArrayList<Map<String, Object>>();
			Terms authorBuckets = res.getAggregations().get("uid");
			System.out.println("Author Count for date "+startPubDate+": "+authorBuckets.getBuckets().size());
			for (Terms.Bucket author : authorBuckets.getBuckets()) {
				String uid = author.getKeyAsString();
				Map<String, Object> recordMap = new HashMap<String, Object>();
				recordMap.put(Constants.UID, uid);

				Sum tpvs = author.getAggregations().get("tpvs");
				recordMap.put("totalpvs", (new Double(tpvs.getValue())).longValue());
				Cardinality storyCount = author.getAggregations().get("storyCount");
				recordMap.put(Constants.STORY_COUNT, new Long(storyCount.getValue()).intValue());
				TopHits topHits = author.getAggregations().get("top");
				String authorName = topHits.getHits().getHits()[0].getSource().get(Constants.AUTHOR_NAME).toString();
				recordMap.put(Constants.ROWID, uid + "_" + rowIDSuffix);
				recordMap.put(Constants.STORY_PUBLISH_TIME, startPubDate);
				recordMap.put(Constants.AUTHOR_NAME, authorName);
				recordMap.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				Terms hostType = author.getAggregations().get("host_type");
				for (Terms.Bucket host : hostType.getBuckets()) {
					Sum pvs = host.getAggregations().get("pvs");

					if (host.getKey().equals(HostType.WEB)) {
						recordMap.put(Constants.WPVS, new Double(pvs.getValue()).longValue());
					} else if (host.getKey().equals(HostType.MOBILE)) {
						recordMap.put(Constants.MPVS, new Double(pvs.getValue()).longValue());

					} else if (host.getKey().equals(HostType.ANDROID)) {
						recordMap.put(Constants.APVS, new Double(pvs.getValue()).longValue());

					} else if (host.getKey().equals(HostType.IPHONE)) {
						recordMap.put(Constants.IPVS, new Double(pvs.getValue()).longValue());
					} else {
						if (recordMap.containsKey(Constants.OTHERPVS)) {
							recordMap.put(Constants.OTHERPVS,
									(Long) recordMap.get(Constants.OTHERPVS) + new Double(pvs.getValue()).longValue());
						} else {
							recordMap.put(Constants.OTHERPVS, new Double(pvs.getValue()).longValue());
						}
					}
				}
				authorDailyContributionList.add(recordMap);
			}
			if (authorDailyContributionList.size() > 0) {
				elasticSearchIndexService.indexOrUpdate(Indexes.AUTHOR_CONTRIBUTION, MappingTypes.MAPPING_REALTIME,
						authorDailyContributionList);
				System.out.println("Author Contribution updated in index "+Indexes.AUTHOR_CONTRIBUTION+", Size: "+authorDailyContributionList.size());
				authorDailyContributionList.clear();
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Total Execution time(Seconds) in fetching and inserting of author data : "
					+ (endTime - startTime) / (1000));
		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error in fetching data of author." + e.getMessage());
		}

	}

	public static void main(String[] args) {
		AuthorDailyContributionDetector duvi = new AuthorDailyContributionDetector();
		duvi.derivePreviousDayData();
		System.exit(0);
	}

}