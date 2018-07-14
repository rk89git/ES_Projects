package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.Constants.CricketConstants.SessionTypeConstants;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;
import com.db.wisdom.model.StoryPerformance;

public class AuthorsSessionCountIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> sessionsData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(AuthorsSessionCountIndexer.class);

	List<String> bhaskarChannels = Arrays.asList("521", "3849", "3322", "10400", "10402", "10401");
	List<String> divyaChannels = Arrays.asList("960", "3850", "3776");
	List<String> remainingChannels = Arrays.asList("1463", "4371", "5483", "4444", "9254", "9069");

	public void insertData(String date) {
		try {

			String indexName = "realtime_upvs_" + date.replaceAll("-", "_");
			// System.out.println(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT));
			SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(QueryBuilders.rangeQuery(Constants.STORY_PUBLISH_TIME).gte("2017-03-01"))
					.addAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE)
							.subAggregation(AggregationBuilders.filter("521", QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, bhaskarChannels))
									.subAggregation(AggregationBuilders.dateHistogram("days").field(Constants.STORY_PUBLISH_TIME)
											.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
											.subAggregation(AggregationBuilders.terms("uid").field(Constants.UID)
													.subAggregation(AggregationBuilders.cardinality("sess_id").field(Constants.SESS_ID)).size(100))))
							.subAggregation(AggregationBuilders.filter("960", QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, divyaChannels))
									.subAggregation(AggregationBuilders.dateHistogram("days").field(Constants.STORY_PUBLISH_TIME)
											.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
											.subAggregation(AggregationBuilders.terms("uid").field(Constants.UID)
													.subAggregation(AggregationBuilders.cardinality("sess_id").field(Constants.SESS_ID)).size(100))))
							.subAggregation(AggregationBuilders.filter("remaining",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, remainingChannels))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.CHANNEL).field(Constants.CHANNEL_SLNO).size(50)
											.subAggregation(AggregationBuilders.dateHistogram("days").field(Constants.STORY_PUBLISH_TIME)
													.dateHistogramInterval(DateHistogramInterval.days(1)).format("yyyy-MM-dd")
													.subAggregation(AggregationBuilders.terms("uid").field(Constants.UID)
															.subAggregation(AggregationBuilders.cardinality("sess_id").field(Constants.SESS_ID)).size(100))))))
					.setSize(0).execute().actionGet();

			Terms hostTerms = res.getAggregations().get("host_type");
			for (Terms.Bucket hostBucket : hostTerms.getBuckets()) {
				String hostType = hostBucket.getKeyAsString();
				Filter remainingChannel = hostBucket.getAggregations().get("remaining");
				Terms channelTerms = remainingChannel.getAggregations().get(SessionTypeConstants.CHANNEL);

				for (Terms.Bucket channelBucket : channelTerms.getBuckets()) {
					int channel = channelBucket.getKeyAsNumber().intValue();					
					Histogram interval = channelBucket.getAggregations().get("days");
					for (Histogram.Bucket bucket : interval.getBuckets()) {
						if (bucket.getDocCount() > 0) {
							String pub_date = bucket.getKeyAsString();
							Terms author = bucket.getAggregations().get("uid");

							for (Terms.Bucket uid : author.getBuckets()) {
								String authorUid = uid.getKeyAsString();
								Cardinality channelSessionsAgg = uid.getAggregations().get("sess_id");
								long sess_id = channelSessionsAgg.getValue();

								Map<String, Object> authorData = new HashMap<>();
								authorData.put(Constants.UID, authorUid);
								authorData.put(Constants.PUB_DATE, pub_date);
								authorData.put(Constants.DATE, date);
								authorData.put(Constants.SESSION_COUNT, sess_id);
								authorData.put(Constants.CHANNEL_SLNO, channel);								
								authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
								authorData.put(Constants.HOST_TYPE, hostType);
								authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
								authorData.put(Constants.ROWID, pub_date+"_"+channel+ "_" + authorUid+"_"+hostType+"_"+date);
								sessionsData.add(authorData);

							}

						}
					}
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(
								IndexUtils.getMonthlyIndex(Indexes.AUTHORS_SESSION_COUNT),
								MappingTypes.MAPPING_REALTIME, sessionsData);
						log.info("Records inserted in authors_session_count index, size: " + sessionsData.size());
						System.out.println(
								"Records inserted in authors_session_count index, size: " + sessionsData.size());
						sessionsData.clear();
					}

				}

				Filter bhaskarChannel = hostBucket.getAggregations().get("521");
				int channel = 521;
				Histogram interval = bhaskarChannel.getAggregations().get("days");
				for (Histogram.Bucket bucket : interval.getBuckets()) {
					if (bucket.getDocCount() > 0) {
						String pub_date = bucket.getKeyAsString();
						Terms author = bucket.getAggregations().get("uid");
						for (Terms.Bucket uid : author.getBuckets()) {
							String authorUid = uid.getKeyAsString();
							Cardinality sess_id_count = uid.getAggregations().get("sess_id");
							long sess_id = sess_id_count.getValue();

							Map<String, Object> authorData = new HashMap<>();
							authorData.put(Constants.UID, authorUid);
							authorData.put(Constants.SESSION_COUNT, sess_id);
							authorData.put(Constants.CHANNEL_SLNO, channel);
							authorData.put(Constants.PUB_DATE, pub_date);
							authorData.put(Constants.DATE, date);
							authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
							authorData.put(Constants.HOST_TYPE, hostType);
							authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
							authorData.put(Constants.ROWID,   pub_date + "_"+channel + "_" + authorUid+"_"+hostType+"_"+date);
							sessionsData.add(authorData);

						}

					}
				}
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(
							IndexUtils.getMonthlyIndex(Indexes.AUTHORS_SESSION_COUNT),
							MappingTypes.MAPPING_REALTIME, sessionsData);
					log.info("Records inserted in authors_session_count index, size: " + sessionsData.size());
					System.out.println(
							"Records inserted in authors_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}

				Filter divyaChannel = hostBucket.getAggregations().get("960");
				int dChannel = 960;
				Histogram divyaInterval = divyaChannel.getAggregations().get("days");
				for (Histogram.Bucket bucket : divyaInterval.getBuckets()) {
					if (bucket.getDocCount() > 0) {
						String pub_date = bucket.getKeyAsString();
						Terms author = bucket.getAggregations().get("uid");
						for (Terms.Bucket uid : author.getBuckets()) {
							String authorUid = uid.getKeyAsString();
							Cardinality sess_id_count = uid.getAggregations().get("sess_id");
							long sess_id = sess_id_count.getValue();

							Map<String, Object> authorData = new HashMap<>();
							authorData.put(Constants.UID, authorUid);
							authorData.put(Constants.SESSION_COUNT, sess_id);
							authorData.put(Constants.CHANNEL_SLNO, dChannel);
							authorData.put(Constants.PUB_DATE, pub_date);
							authorData.put(Constants.DATE, date);
							authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
							authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
							authorData.put(Constants.HOST_TYPE, hostType);
							authorData.put(Constants.ROWID,  pub_date + "_"+dChannel + "_" + authorUid+"_"+hostType+"_"+date);
							sessionsData.add(authorData);

						}

					}
				}
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(
							IndexUtils.getMonthlyIndex(Indexes.AUTHORS_SESSION_COUNT),
							MappingTypes.MAPPING_REALTIME, sessionsData);
					log.info("Records inserted in authors_session_count index, size: " + sessionsData.size());
					System.out.println(
							"Records inserted in authors_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		String date = DateUtil.getCurrentDate().replaceAll("_","-");
		//String date ="2018-04-03";
		AuthorsSessionCountIndexer sci = new AuthorsSessionCountIndexer();		
		
		sci.insertData(date);
		log.info("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		System.out.println("Total time taken (Seconds):  " + ((System.currentTimeMillis() - start) / 1000));
		}
}
