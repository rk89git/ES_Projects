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

public class SessionCountIndexer {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> sessionsData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(SessionCountIndexer.class);

	List<String> bhaskarChannels = Arrays.asList("521","3849","3322","10400","10402","10401");
	List<String> divyaChannels = Arrays.asList("960","3850","3776");
	List<String> remainingChannels = Arrays.asList("1463","4371","5483","4444","9254","9069","3322","3776");

	public void insertData(String date) {
		try {
			String indexName = "realtime_upvs_"+date.replaceAll("-", "_");
			SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
					.addAggregation(AggregationBuilders.terms("host_type").field(Constants.HOST_TYPE)

							.subAggregation(AggregationBuilders.filter("521",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, bhaskarChannels))
									.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.REF_PLATFORM).field(Constants.REF_PLATFORM).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(Constants.SPL_TRACKER).field(Constants.SPL_TRACKER).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									)
							.subAggregation(AggregationBuilders.filter("960",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, divyaChannels))
									.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.REF_PLATFORM).field(Constants.REF_PLATFORM).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									.subAggregation(AggregationBuilders.terms(Constants.SPL_TRACKER).field(Constants.SPL_TRACKER).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
									)
							.subAggregation(AggregationBuilders.filter("remaining",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, remainingChannels))
									.subAggregation(AggregationBuilders.terms(SessionTypeConstants.CHANNEL).field(Constants.CHANNEL_SLNO).size(50)
											.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR).field(Constants.UID).size(500)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))
													.subAggregation(AggregationBuilders.terms(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
															.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD))))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.SUPER_CAT_NAME).field(Constants.SUPER_CAT_NAME).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
											.subAggregation(AggregationBuilders.terms(SessionTypeConstants.REF_PLATFORM).field(Constants.REF_PLATFORM).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
											.subAggregation(AggregationBuilders.terms(Constants.SPL_TRACKER).field(Constants.SPL_TRACKER).size(50)
													.subAggregation(AggregationBuilders.cardinality(Constants.SESSION).field(Constants.SESS_ID_FIELD)))
											)))
					.setSize(0).execute().actionGet(); 

			Terms hostTerms = res.getAggregations().get("host_type");
			for(Terms.Bucket hostBucket:hostTerms.getBuckets()){
				String hostType = hostBucket.getKeyAsString();
				Filter remainingChannel = hostBucket.getAggregations().get("remaining");
				Terms channelTerms = remainingChannel.getAggregations().get(SessionTypeConstants.CHANNEL);
				for(Terms.Bucket channelBucket:channelTerms.getBuckets()){

					int channel = channelBucket.getKeyAsNumber().intValue();
					Cardinality channelSessionsAgg = channelBucket.getAggregations().get(Constants.SESSION);
					long channelSessions = channelSessionsAgg.getValue();
					Map<String,Object> channelData = new HashMap<>();
					channelData.put(Constants.CHANNEL_SLNO, channel);
					channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
					channelData.put(Constants.DATE, date);
					channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					channelData.put(Constants.SESSION_COUNT, channelSessions);
					channelData.put(Constants.ROWID, channel+"_"+hostType+"_"+date);
					channelData.put(Constants.HOST_TYPE, hostType);
					sessionsData.add(channelData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}

					Terms authorTerms = channelBucket.getAggregations().get(SessionTypeConstants.AUTHOR);
					for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

						int author = authorBucket.getKeyAsNumber().intValue();
						Cardinality authorSessionsAgg = authorBucket.getAggregations().get(Constants.SESSION);
						long authorSessions = authorSessionsAgg.getValue();
						Map<String,Object> authorData = new HashMap<>();
						authorData.put(Constants.CHANNEL_SLNO, channel);
						authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
						authorData.put(Constants.DATE, date);
						authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						authorData.put(Constants.SESSION_COUNT, authorSessions);
						authorData.put(Constants.UID, author);
						authorData.put(Constants.HOST_TYPE, hostType);
						authorData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+date);
						sessionsData.add(authorData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							
							sessionsData.clear();
						}

						Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
						for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){

							String cat = authorCatBucket.getKeyAsString();
							Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get(Constants.SESSION);
							long authorCatSessions = authorCatSessionsAgg.getValue();
							Map<String,Object> authorCatData = new HashMap<>();
							authorCatData.put(Constants.CHANNEL_SLNO, channel);
							authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
							authorCatData.put(Constants.DATE, date);
							authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
							authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
							authorCatData.put(Constants.UID, author);
							authorCatData.put(Constants.SUPER_CAT_NAME, cat);
							authorCatData.put(Constants.HOST_TYPE, hostType);
							authorCatData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+cat+"_"+date);
							sessionsData.add(authorCatData);
							if (sessionsData.size() > batchSize) {
								elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
										sessionsData);
								sessionsData.clear();
							}
						}
					}	

					Terms catTerms = channelBucket.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
					for(Terms.Bucket catBucket:catTerms.getBuckets()){

						String cat = catBucket.getKeyAsString();
						Cardinality catSessionsAgg = catBucket.getAggregations().get(Constants.SESSION);
						long catSessions = catSessionsAgg.getValue();
						Map<String,Object> catData = new HashMap<>();
						catData.put(Constants.CHANNEL_SLNO, channel);
						catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
						catData.put(Constants.DATE, date);
						catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						catData.put(Constants.SESSION_COUNT, catSessions);
						catData.put(Constants.SUPER_CAT_NAME, cat);
						catData.put(Constants.HOST_TYPE, hostType);
						catData.put(Constants.ROWID, channel+"_"+hostType+"_"+cat+"_"+date);
						sessionsData.add(catData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							sessionsData.clear();
						}
					}

					// ## preparing for session counts of Trackers ##

					Terms trackersTerms = channelBucket.getAggregations().get(SessionTypeConstants.TRACKER);
					for(Terms.Bucket trackBucket:trackersTerms.getBuckets()){

						String tracker = trackBucket.getKeyAsString();						
						Cardinality trackerSessionsAgg = trackBucket.getAggregations().get(Constants.SESSION);

						long trackerSessions = trackerSessionsAgg.getValue();
						Map<String,Object> trackersData = new HashMap<>();

						trackersData.put(Constants.CHANNEL_SLNO, channel);
						trackersData.put(Constants.SESSION_TYPE, SessionTypeConstants.TRACKER);
						trackersData.put(Constants.DATE, date);
						trackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						trackersData.put(Constants.SESSION_COUNT, trackerSessions);						
						trackersData.put(Constants.TRACKER, tracker);
						trackersData.put(Constants.HOST_TYPE, hostType);
						trackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+tracker+"_"+date);
						sessionsData.add(trackersData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							sessionsData.clear();
						}
					}
					
					// ## preparing for session counts of ref_platform ##

					Terms refPlatformTerms = channelBucket.getAggregations().get(SessionTypeConstants.REF_PLATFORM);
					for(Terms.Bucket refPlatformBucket:refPlatformTerms.getBuckets()){

						String refPlatform = refPlatformBucket.getKeyAsString();						
						Cardinality refPlatformSessionsAgg = refPlatformBucket.getAggregations().get(Constants.SESSION);

						long refPlatformSessions = refPlatformSessionsAgg.getValue();
						Map<String,Object> refPlatformData = new HashMap<>();

						refPlatformData.put(Constants.CHANNEL_SLNO, channel);
						refPlatformData.put(Constants.SESSION_TYPE, SessionTypeConstants.REF_PLATFORM);
						refPlatformData.put(Constants.DATE, date);
						refPlatformData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						refPlatformData.put(Constants.SESSION_COUNT, refPlatformSessions);						
						refPlatformData.put(Constants.REF_PLATFORM, refPlatform);
						refPlatformData.put(Constants.HOST_TYPE, hostType);
						refPlatformData.put(Constants.ROWID, channel+"_"+hostType+"_"+refPlatform+"_"+date);
						sessionsData.add(refPlatformData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							sessionsData.clear();
						}
					}


					// ## preparing for session counts of Trackers ##

					Terms splTrackersTerms = channelBucket.getAggregations().get(Constants.SPL_TRACKER);
					for(Terms.Bucket splTrackBucket:splTrackersTerms.getBuckets()){

						int spl_tracker = splTrackBucket.getKeyAsNumber().intValue();						
						Cardinality spl_trackerSessionsAgg = splTrackBucket.getAggregations().get(Constants.SESSION);

						long trackerSessions = spl_trackerSessionsAgg.getValue();
						Map<String,Object> splTrackersData = new HashMap<>();

						splTrackersData.put(Constants.CHANNEL_SLNO, channel);
						splTrackersData.put(Constants.SESSION_TYPE, Constants.SPL_TRACKER);
						splTrackersData.put(Constants.DATE, date);
						splTrackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						splTrackersData.put(Constants.SESSION_COUNT, trackerSessions);						
						splTrackersData.put(Constants.SPL_TRACKER, spl_tracker);
						splTrackersData.put(Constants.HOST_TYPE, hostType);
						splTrackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+spl_tracker+"_"+date);
						sessionsData.add(splTrackersData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							sessionsData.clear();
						}
					}

				}

				Filter bhaskarChannel = hostBucket.getAggregations().get("521");
				int channel = 521;
				Cardinality channelSessionsAgg = bhaskarChannel.getAggregations().get(Constants.SESSION);
				long channelSessions = channelSessionsAgg.getValue();
				Map<String,Object> channelData = new HashMap<>();
				channelData.put(Constants.CHANNEL_SLNO, channel);
				channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
				channelData.put(Constants.DATE, date);
				channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				channelData.put(Constants.SESSION_COUNT, channelSessions);
				channelData.put(Constants.HOST_TYPE, hostType);
				channelData.put(Constants.ROWID, channel+"_"+hostType+"_"+date);
				sessionsData.add(channelData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					sessionsData.clear();
				}

				Terms authorTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.AUTHOR);
				for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

					int author = authorBucket.getKeyAsNumber().intValue();
					Cardinality authorSessionsAgg = authorBucket.getAggregations().get(Constants.SESSION);
					long authorSessions = authorSessionsAgg.getValue();
					Map<String,Object> authorData = new HashMap<>();
					authorData.put(Constants.CHANNEL_SLNO, channel);
					authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
					authorData.put(Constants.DATE, date);
					authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					authorData.put(Constants.SESSION_COUNT, authorSessions);
					authorData.put(Constants.UID, author);
					authorData.put(Constants.HOST_TYPE, hostType);
					authorData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+date);
					sessionsData.add(authorData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}

					Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
					for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){

						String cat = authorCatBucket.getKeyAsString();
						Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get(Constants.SESSION);
						long authorCatSessions = authorCatSessionsAgg.getValue();
						Map<String,Object> authorCatData = new HashMap<>();
						authorCatData.put(Constants.CHANNEL_SLNO, channel);
						authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
						authorCatData.put(Constants.DATE, date);
						authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
						authorCatData.put(Constants.UID, author);
						authorCatData.put(Constants.SUPER_CAT_NAME, cat);
						authorCatData.put(Constants.HOST_TYPE, hostType);
						authorCatData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+cat+"_"+date);
						sessionsData.add(authorCatData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							sessionsData.clear();
						}
					}
				}	

				Terms catTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
				for(Terms.Bucket catBucket:catTerms.getBuckets()){

					String cat = catBucket.getKeyAsString();
					Cardinality catSessionsAgg = catBucket.getAggregations().get(Constants.SESSION);
					long catSessions = catSessionsAgg.getValue();
					Map<String,Object> catData = new HashMap<>();
					catData.put(Constants.CHANNEL_SLNO, channel);
					catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
					catData.put(Constants.DATE, date);
					catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					catData.put(Constants.SESSION_COUNT, catSessions);
					catData.put(Constants.SUPER_CAT_NAME, cat);
					catData.put(Constants.HOST_TYPE, hostType);
					catData.put(Constants.ROWID, channel+"_"+hostType+"_"+cat+"_"+date);
					sessionsData.add(catData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}
				}

				// ## preparing for session counts of Trackers ##

				Terms trackersTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.TRACKER);
				for(Terms.Bucket trackBucket:trackersTerms.getBuckets()){

					String tracker = trackBucket.getKeyAsString();						
					Cardinality trackerSessionsAgg = trackBucket.getAggregations().get(Constants.SESSION);

					long trackerSessions = trackerSessionsAgg.getValue();
					Map<String,Object> trackersData = new HashMap<>();

					trackersData.put(Constants.CHANNEL_SLNO, channel);
					trackersData.put(Constants.SESSION_TYPE, SessionTypeConstants.TRACKER);
					trackersData.put(Constants.DATE, date);
					trackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					trackersData.put(Constants.SESSION_COUNT, trackerSessions);						
					trackersData.put(Constants.TRACKER, tracker);
					trackersData.put(Constants.HOST_TYPE, hostType);
					trackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+tracker+"_"+date);
					sessionsData.add(trackersData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}
				}
				
				// ## preparing for session counts of ref_platform ##

				Terms refPlatformTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.REF_PLATFORM);
				for(Terms.Bucket refPlatformBucket:refPlatformTerms.getBuckets()){

					String refPlatform = refPlatformBucket.getKeyAsString();						
					Cardinality refPlatformSessionsAgg = refPlatformBucket.getAggregations().get(Constants.SESSION);

					long refPlatformSessions = refPlatformSessionsAgg.getValue();
					Map<String,Object> refPlatformData = new HashMap<>();

					refPlatformData.put(Constants.CHANNEL_SLNO, channel);
					refPlatformData.put(Constants.SESSION_TYPE, SessionTypeConstants.REF_PLATFORM);
					refPlatformData.put(Constants.DATE, date);
					refPlatformData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					refPlatformData.put(Constants.SESSION_COUNT, refPlatformSessions);						
					refPlatformData.put(Constants.REF_PLATFORM, refPlatform);
					refPlatformData.put(Constants.HOST_TYPE, hostType);
					refPlatformData.put(Constants.ROWID, channel+"_"+hostType+"_"+refPlatform+"_"+date);
					sessionsData.add(refPlatformData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}
				}

				// ## preparing for session counts of Trackers ##

				Terms splTrackersTerms = bhaskarChannel.getAggregations().get(Constants.SPL_TRACKER);
				for(Terms.Bucket splTrackBucket:splTrackersTerms.getBuckets()){

					int spl_tracker = splTrackBucket.getKeyAsNumber().intValue();						
					Cardinality spl_trackerSessionsAgg = splTrackBucket.getAggregations().get(Constants.SESSION);

					long trackerSessions = spl_trackerSessionsAgg.getValue();
					Map<String,Object> splTrackersData = new HashMap<>();

					splTrackersData.put(Constants.CHANNEL_SLNO, channel);
					splTrackersData.put(Constants.SESSION_TYPE, Constants.SPL_TRACKER);
					splTrackersData.put(Constants.DATE, date);
					splTrackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					splTrackersData.put(Constants.SESSION_COUNT, trackerSessions);						
					splTrackersData.put(Constants.SPL_TRACKER, spl_tracker);
					splTrackersData.put(Constants.HOST_TYPE, hostType);
					splTrackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+spl_tracker+"_"+date);
					sessionsData.add(splTrackersData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}
				}



				Filter divyaChannel = hostBucket.getAggregations().get("960");
				channel = 960;
				channelSessionsAgg = divyaChannel.getAggregations().get(Constants.SESSION);
				channelSessions = channelSessionsAgg.getValue();
				channelData = new HashMap<>();
				channelData.put(Constants.CHANNEL_SLNO, channel);
				channelData.put(Constants.SESSION_TYPE, SessionTypeConstants.CHANNEL);
				channelData.put(Constants.DATE, date);
				channelData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
				channelData.put(Constants.SESSION_COUNT, channelSessions);
				channelData.put(Constants.HOST_TYPE, hostType);
				channelData.put(Constants.ROWID, channel+"_"+hostType+"_"+date);
				sessionsData.add(channelData);
				if (sessionsData.size() > batchSize) {
					elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
							sessionsData);
					sessionsData.clear();
				}

				authorTerms = divyaChannel.getAggregations().get(SessionTypeConstants.AUTHOR);
				for(Terms.Bucket authorBucket:authorTerms.getBuckets()){

					int author = authorBucket.getKeyAsNumber().intValue();
					Cardinality authorSessionsAgg = authorBucket.getAggregations().get(Constants.SESSION);
					long authorSessions = authorSessionsAgg.getValue();
					Map<String,Object> authorData = new HashMap<>();
					authorData.put(Constants.CHANNEL_SLNO, channel);
					authorData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR);
					authorData.put(Constants.DATE, date);
					authorData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					authorData.put(Constants.SESSION_COUNT, authorSessions);
					authorData.put(Constants.UID, author);
					authorData.put(Constants.HOST_TYPE, hostType);
					authorData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+date);
					sessionsData.add(authorData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);


						sessionsData.clear();
					}

					Terms authorCatTerms = authorBucket.getAggregations().get(SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
					for(Terms.Bucket authorCatBucket:authorCatTerms.getBuckets()){
						String cat = authorCatBucket.getKeyAsString();
						Cardinality authorCatSessionsAgg = authorCatBucket.getAggregations().get(Constants.SESSION);
						long authorCatSessions = authorCatSessionsAgg.getValue();
						Map<String,Object> authorCatData = new HashMap<>();
						authorCatData.put(Constants.CHANNEL_SLNO, channel);
						authorCatData.put(Constants.SESSION_TYPE, SessionTypeConstants.AUTHOR_SUPER_CAT_NAME);
						authorCatData.put(Constants.DATE, date);
						authorCatData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
						authorCatData.put(Constants.SESSION_COUNT, authorCatSessions);
						authorCatData.put(Constants.UID, author);
						authorCatData.put(Constants.SUPER_CAT_NAME, cat);
						authorCatData.put(Constants.HOST_TYPE, hostType);
						authorCatData.put(Constants.ROWID, channel+"_"+hostType+"_"+author+"_"+cat+"_"+date);
						sessionsData.add(authorCatData);
						if (sessionsData.size() > batchSize) {
							elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
									sessionsData);
							log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());

							sessionsData.clear();
						}
					}
				}	

				catTerms = divyaChannel.getAggregations().get(SessionTypeConstants.SUPER_CAT_NAME);
				for(Terms.Bucket catBucket:catTerms.getBuckets()){
					String cat = catBucket.getKeyAsString();
					Cardinality catSessionsAgg = catBucket.getAggregations().get(Constants.SESSION);
					long catSessions = catSessionsAgg.getValue();
					Map<String,Object> catData = new HashMap<>();
					catData.put(Constants.CHANNEL_SLNO, channel);
					catData.put(Constants.SESSION_TYPE, SessionTypeConstants.SUPER_CAT_NAME);
					catData.put(Constants.DATE, date);
					catData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					catData.put(Constants.SESSION_COUNT, catSessions);
					catData.put(Constants.SUPER_CAT_NAME, cat);
					catData.put(Constants.HOST_TYPE, hostType);
					catData.put(Constants.ROWID, channel+"_"+hostType+"_"+cat+"_"+date);
					sessionsData.add(catData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());

						sessionsData.clear();
					}
				}

				// ## preparing for session counts of Trackers ##

				Terms divyaTrackersTerms = divyaChannel.getAggregations().get(SessionTypeConstants.TRACKER);
				for(Terms.Bucket trackBucket:divyaTrackersTerms.getBuckets()){

					String tracker = trackBucket.getKeyAsString();						
					Cardinality trackerSessionsAgg = trackBucket.getAggregations().get(Constants.SESSION);

					long trackerSessions = trackerSessionsAgg.getValue();
					Map<String,Object> trackersData = new HashMap<>();

					trackersData.put(Constants.CHANNEL_SLNO, channel);
					trackersData.put(Constants.SESSION_TYPE, SessionTypeConstants.TRACKER);
					trackersData.put(Constants.DATE, date);
					trackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					trackersData.put(Constants.SESSION_COUNT, trackerSessions);						
					trackersData.put(Constants.TRACKER, tracker);
					trackersData.put(Constants.HOST_TYPE, hostType);
					trackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+tracker+"_"+date);
					sessionsData.add(trackersData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());

						sessionsData.clear();
					}
				}
				
				// ## preparing for session counts of ref_platform ##

				Terms divyaRefPlatformTerms = divyaChannel.getAggregations().get(SessionTypeConstants.REF_PLATFORM);
				for(Terms.Bucket refPlatformBucket:refPlatformTerms.getBuckets()){

					String refPlatform = refPlatformBucket.getKeyAsString();						
					Cardinality refPlatformSessionsAgg = refPlatformBucket.getAggregations().get(Constants.SESSION);

					long refPlatformSessions = refPlatformSessionsAgg.getValue();
					Map<String,Object> refPlatformData = new HashMap<>();

					refPlatformData.put(Constants.CHANNEL_SLNO, channel);
					refPlatformData.put(Constants.SESSION_TYPE, SessionTypeConstants.REF_PLATFORM);
					refPlatformData.put(Constants.DATE, date);
					refPlatformData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					refPlatformData.put(Constants.SESSION_COUNT, refPlatformSessions);						
					refPlatformData.put(Constants.REF_PLATFORM, refPlatform);
					refPlatformData.put(Constants.HOST_TYPE, hostType);
					refPlatformData.put(Constants.ROWID, channel+"_"+hostType+"_"+refPlatform+"_"+date);
					sessionsData.add(refPlatformData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						sessionsData.clear();
					}
				}

				// ## preparing for session counts of Trackers ##

				Terms divyaSplTrackersTerms = bhaskarChannel.getAggregations().get(Constants.SPL_TRACKER);
				for(Terms.Bucket splTrackBucket:divyaSplTrackersTerms.getBuckets()){

					int spl_tracker = splTrackBucket.getKeyAsNumber().intValue();						
					Cardinality spl_trackerSessionsAgg = splTrackBucket.getAggregations().get(Constants.SESSION);

					long trackerSessions = spl_trackerSessionsAgg.getValue();
					Map<String,Object> splTrackersData = new HashMap<>();

					splTrackersData.put(Constants.CHANNEL_SLNO, channel);
					splTrackersData.put(Constants.SESSION_TYPE, Constants.SPL_TRACKER);
					splTrackersData.put(Constants.DATE, date);
					splTrackersData.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					splTrackersData.put(Constants.SESSION_COUNT, trackerSessions);						
					splTrackersData.put(Constants.SPL_TRACKER, spl_tracker);
					splTrackersData.put(Constants.HOST_TYPE, hostType);
					splTrackersData.put(Constants.ROWID, channel+"_"+hostType+"_"+spl_tracker+"_"+date);
					sessionsData.add(splTrackersData);
					if (sessionsData.size() > batchSize) {
						elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
								sessionsData);
						log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());

						sessionsData.clear();
					}
				}
			}

			if (!sessionsData.isEmpty()) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndex(Indexes.CATEGORISED_SESSION_COUNT), MappingTypes.MAPPING_REALTIME,
						sessionsData);


				sessionsData.clear();
			}

		} catch (Exception e) {
			log.error(e);
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		SessionCountIndexer sci = new SessionCountIndexer();

		String date = DateUtil.getCurrentDate().replaceAll("_", "-");
		sci.insertData(date);
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));

	}

}
