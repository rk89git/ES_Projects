package com.db.wisdom.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.Constants.CricketConstants.SessionTypeConstants;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.db.common.utils.IndexUtils;

public class PrevDaySessionCountIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> sessionsData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(PrevDaySessionCountIndexer.class);

	List<String> bhaskarChannels = Arrays.asList("521","3849","3322","10400","10402","10401");
	List<String> divyaChannels = Arrays.asList("960","3850","3776");
	List<String> remainingChannels = Arrays.asList("1463","4371","5483","4444","9254","9069");

	public void insertData(String date) {
		String indexName = "realtime_upvs_"+date.replaceAll("-", "_");
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.addAggregation(AggregationBuilders.filter("521",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, bhaskarChannels))
						.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
						.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
								.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))))
						.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
								.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))))
				.addAggregation(AggregationBuilders.filter("960",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, divyaChannels))
						.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
						.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
								.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))))
						.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
								.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))))
				.addAggregation(AggregationBuilders.filter("remaining",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, remainingChannels))
						.subAggregation(AggregationBuilders.terms(SessionTypeConstants.CHANNEL).field(Constants.CHANNEL_SLNO).size(50)
								.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD))))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))))
				.setSize(0).execute().actionGet(); 


		Filter remainingChannel = res.getAggregations().get("remaining");
		Terms channelTerms = remainingChannel.getAggregations().get(SessionTypeConstants.CHANNEL);
		for(Terms.Bucket channelBucket:channelTerms.getBuckets()){

			int channel = channelBucket.getKeyAsNumber().intValue();
			Cardinality channelSessionsAgg = channelBucket.getAggregations().get("sessions");
			long channelSessions = channelSessionsAgg.getValue();
			Map<String,Object> channelData = new HashMap<>();
			channelData.put(Constants.CHANNEL_SLNO, channel);
			channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
			channelData.put(Constants.DATE, date);
			channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			channelData.put(Constants.SESSION_COUNT, channelSessions);
			channelData.put(Constants.ROWID, channel+"_"+date);
			sessionsData.add(channelData);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				sessionsData.clear();
			}

			Terms authorTerms = channelBucket.getAggregations().get(SessionTypeConstants.AUTHOR);
			for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

				int author = authorBucket.getKeyAsNumber().intValue();
				Cardinality authorSessionsAgg = authorBucket.getAggregations().get("sessions");
				long authorSessions = authorSessionsAgg.getValue();
				Map<String,Object> authorData = new HashMap<>();
				authorData.put(Constants.CHANNEL_SLNO, channel);
				authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
				authorData.put(Constants.DATE, date);
				authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				authorData.put(Constants.SESSION_COUNT, authorSessions);
				authorData.put(Constants.UID, author);
				authorData.put(Constants.ROWID, channel+"_"+author+"_"+date);
				sessionsData.add(authorData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}

				Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
				for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){

					int cat = authorCatBucket.getKeyAsNumber().intValue();
					Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get("sessions");
					long authorCatSessions = authorCatSessionsAgg.getValue();
					Map<String,Object> authorCatData = new HashMap<>();
					authorCatData.put(Constants.CHANNEL_SLNO, channel);
					authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
					authorCatData.put(Constants.DATE, date);
					authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
					authorCatData.put(Constants.UID, author);
					authorCatData.put(Constants.SUPER_CAT_NAME, cat);
					authorCatData.put(Constants.ROWID, channel+"_"+author+"_"+cat+"_"+date);
					sessionsData.add(authorCatData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
						sessionsData.clear();
					}
				}
			}	

			Terms catTerms = channelBucket.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
			for(Terms.Bucket catBucket:catTerms.getBuckets()){

				int cat = catBucket.getKeyAsNumber().intValue();
				Cardinality catSessionsAgg = catBucket.getAggregations().get("sessions");
				long catSessions = catSessionsAgg.getValue();
				Map<String,Object> catData = new HashMap<>();
				catData.put(Constants.CHANNEL_SLNO, channel);
				catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
				catData.put(Constants.DATE, date);
				catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				catData.put(Constants.SESSION_COUNT, catSessions);
				catData.put(Constants.SUPER_CAT_NAME, cat);
				catData.put(Constants.ROWID, channel+"_"+cat+"_"+date);
				sessionsData.add(catData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}
			}
		}

		Filter bhaskarChannel = res.getAggregations().get("521");
		int channel = 521;
		Cardinality channelSessionsAgg = bhaskarChannel.getAggregations().get("sessions");
		long channelSessions = channelSessionsAgg.getValue();
		Map<String,Object> channelData = new HashMap<>();
		channelData.put(Constants.CHANNEL_SLNO, channel);
		channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
		channelData.put(Constants.DATE, date);
		channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		channelData.put(Constants.SESSION_COUNT, channelSessions);
		channelData.put(Constants.ROWID, channel+"_"+date);
		sessionsData.add(channelData);
		if (sessionsData.size() > batchSize) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
					sessionsData);
			log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
			sessionsData.clear();
		}

		Terms authorTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.AUTHOR);
		for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

			int author = authorBucket.getKeyAsNumber().intValue();
			Cardinality authorSessionsAgg = authorBucket.getAggregations().get("sessions");
			long authorSessions = authorSessionsAgg.getValue();
			Map<String,Object> authorData = new HashMap<>();
			authorData.put(Constants.CHANNEL_SLNO, channel);
			authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
			authorData.put(Constants.DATE, date);
			authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			authorData.put(Constants.SESSION_COUNT, authorSessions);
			authorData.put(Constants.UID, author);
			authorData.put(Constants.ROWID, channel+"_"+author+"_"+date);
			sessionsData.add(authorData);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				sessionsData.clear();
			}

			Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
			for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){

				int cat = authorCatBucket.getKeyAsNumber().intValue();
				Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get("sessions");
				long authorCatSessions = authorCatSessionsAgg.getValue();
				Map<String,Object> authorCatData = new HashMap<>();
				authorCatData.put(Constants.CHANNEL_SLNO, channel);
				authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
				authorCatData.put(Constants.DATE, date);
				authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
				authorCatData.put(Constants.UID, author);
				authorCatData.put(Constants.SUPER_CAT_NAME, cat);
				authorCatData.put(Constants.ROWID, channel+"_"+author+"_"+cat+"_"+date);
				sessionsData.add(authorCatData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}
			}
		}	

		Terms catTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
		for(Terms.Bucket catBucket:catTerms.getBuckets()){

			int cat = catBucket.getKeyAsNumber().intValue();
			Cardinality catSessionsAgg = catBucket.getAggregations().get("sessions");
			long catSessions = catSessionsAgg.getValue();
			Map<String,Object> catData = new HashMap<>();
			catData.put(Constants.CHANNEL_SLNO, channel);
			catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
			catData.put(Constants.DATE, date);
			catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			catData.put(Constants.SESSION_COUNT, catSessions);
			catData.put(Constants.SUPER_CAT_NAME, cat);
			catData.put(Constants.ROWID, channel+"_"+cat+"_"+date);
			sessionsData.add(catData);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				sessionsData.clear();
			}
		}


		Filter divyaChannel = res.getAggregations().get("960");
		channel = 960;
		channelSessionsAgg = divyaChannel.getAggregations().get("sessions");
		channelSessions = channelSessionsAgg.getValue();
		channelData = new HashMap<>();
		channelData.put(Constants.CHANNEL_SLNO, channel);
		channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
		channelData.put(Constants.DATE, date);
		channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
		channelData.put(Constants.SESSION_COUNT, channelSessions);
		channelData.put(Constants.ROWID, channel+"_"+date);
		sessionsData.add(channelData);
		if (sessionsData.size() > batchSize) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
					sessionsData);
			log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
			sessionsData.clear();
		}

		authorTerms = divyaChannel.getAggregations().get(SessionTypeConstants.AUTHOR);
		for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

			int author = authorBucket.getKeyAsNumber().intValue();
			Cardinality authorSessionsAgg = authorBucket.getAggregations().get("sessions");
			long authorSessions = authorSessionsAgg.getValue();
			Map<String,Object> authorData = new HashMap<>();
			authorData.put(Constants.CHANNEL_SLNO, channel);
			authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
			authorData.put(Constants.DATE, date);
			authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			authorData.put(Constants.SESSION_COUNT, authorSessions);
			authorData.put(Constants.UID, author);
			authorData.put(Constants.ROWID, channel+"_"+author+"_"+date);
			sessionsData.add(authorData);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				sessionsData.clear();
			}

			Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
			for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){
				int cat = authorCatBucket.getKeyAsNumber().intValue();
				Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get("sessions");
				long authorCatSessions = authorCatSessionsAgg.getValue();
				Map<String,Object> authorCatData = new HashMap<>();
				authorCatData.put(Constants.CHANNEL_SLNO, channel);
				authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
				authorCatData.put(Constants.DATE, date);
				authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
				authorCatData.put(Constants.UID, author);
				authorCatData.put(Constants.SUPER_CAT_NAME, cat);
				authorCatData.put(Constants.ROWID, channel+"_"+author+"_"+cat+"_"+date);
				sessionsData.add(authorCatData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
					sessionsData.clear();
				}
			}
		}	

		catTerms = divyaChannel.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
		for(Terms.Bucket catBucket:catTerms.getBuckets()){
			int cat = catBucket.getKeyAsNumber().intValue();
			Cardinality catSessionsAgg = catBucket.getAggregations().get("sessions");
			long catSessions = catSessionsAgg.getValue();
			Map<String,Object> catData = new HashMap<>();
			catData.put(Constants.CHANNEL_SLNO, channel);
			catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
			catData.put(Constants.DATE, date);
			catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			catData.put(Constants.SESSION_COUNT, catSessions);
			catData.put(Constants.SUPER_CAT_NAME, cat);
			catData.put(Constants.ROWID, channel+"_"+cat+"_"+date);
			sessionsData.add(catData);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				sessionsData.clear();
			}
		}

		if (sessionsData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getYearlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
					sessionsData);
			log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
			sessionsData.clear();
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		PrevDaySessionCountIndexer sci = new PrevDaySessionCountIndexer();

		sci.insertData(DateUtil.getPreviousDate().replaceAll("_", "-"));
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
