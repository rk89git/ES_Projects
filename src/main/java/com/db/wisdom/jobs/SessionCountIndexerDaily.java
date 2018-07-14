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

public class SessionCountIndexerDaily {

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private Client client = elasticSearchIndexService.getClient();

	private List<Map<String, Object>> sessionsData = new ArrayList<Map<String, Object>>();

	int batchSize = 1000;

	private static Logger log = LogManager.getLogger(SessionCountIndexerDaily.class);

	List<String> bhaskarChannels = Arrays.asList("521","3849","3322","10400","10402","10401");
	List<String> divyaChannels = Arrays.asList("960","3850","3776");
	List<String> remainingChannels = Arrays.asList("1463","4371","5483","4444","9254","9069","3322","3776");

	public void insertData(String date) {
		String indexName = "realtime_upvs_"+date.replaceAll("-", "_");
		SearchResponse res = client.prepareSearch(indexName).setTypes(MappingTypes.MAPPING_REALTIME)
				.setQuery(QueryBuilders.termsQuery(Constants.HOST_TYPE, "m","w","a","i"))
				.addAggregation(AggregationBuilders.terms("host").field(Constants.HOST)
						.subAggregation(AggregationBuilders.filter("521",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, bhaskarChannels))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.BROWSER).field(Constants.BROWSER).size(500)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.DEVICE).field(Constants.DEVICE_NAME).size(15000)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.NETWORK).field(Constants.NETWORK).size(100)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.OS).field(Constants.OS).size(50)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(5000)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								)
						.subAggregation(AggregationBuilders.filter("960",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, divyaChannels))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.BROWSER).field(Constants.BROWSER).size(500)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.DEVICE).field(Constants.DEVICE_NAME).size(15000)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.NETWORK).field(Constants.NETWORK).size(100)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.OS).field(Constants.OS).size(50)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(5000)
										.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
								)
						.subAggregation(AggregationBuilders.filter("remaining",QueryBuilders.termsQuery(Constants.CHANNEL_SLNO, remainingChannels))
								.subAggregation(AggregationBuilders.terms(SessionTypeConstants.CHANNEL).field(Constants.CHANNEL_SLNO).size(50)
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.BROWSER).field(Constants.BROWSER).size(500)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.DEVICE).field(Constants.DEVICE_NAME).size(15000)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.NETWORK).field(Constants.NETWORK).size(100)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.OS).field(Constants.OS).size(50)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
										.subAggregation(AggregationBuilders.terms(SessionTypeConstants.TRACKER).field(Constants.TRACKER).size(5000)
												.subAggregation(AggregationBuilders.cardinality("sessions").field(Constants.SESS_ID_FIELD)))
										)))
				.setSize(0).execute().actionGet(); 

		Terms hostTerms = res.getAggregations().get("host");
		for(Terms.Bucket hostBucket:hostTerms.getBuckets()){
			String host = hostBucket.getKeyAsString();
			Filter remainingChannel = hostBucket.getAggregations().get("remaining");
			Terms channelTerms = remainingChannel.getAggregations().get(SessionTypeConstants.CHANNEL);
			for(Terms.Bucket channelBucket:channelTerms.getBuckets()){
				int channel = channelBucket.getKeyAsNumber().intValue();
				Terms browserTerms = channelBucket.getAggregations().get(SessionTypeConstants.BROWSER);
				addData(browserTerms, SessionTypeConstants.BROWSER, channel, host, date);
				Terms deviceTerms = channelBucket.getAggregations().get(SessionTypeConstants.DEVICE);
				addData(deviceTerms, SessionTypeConstants.DEVICE, channel, host, date);
				Terms networkTerms = channelBucket.getAggregations().get(SessionTypeConstants.NETWORK);
				addData(networkTerms, SessionTypeConstants.NETWORK, channel, host, date);
				Terms osTerms = channelBucket.getAggregations().get(SessionTypeConstants.OS);
				addData(osTerms, SessionTypeConstants.OS, channel, host, date);
				Terms trackerTerms = channelBucket.getAggregations().get(SessionTypeConstants.TRACKER);
				addData(trackerTerms, SessionTypeConstants.TRACKER, channel, host, date);				

			}

			Filter bhaskarChannel = hostBucket.getAggregations().get("521");
			int channel = 521;
			Terms browserTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.BROWSER);
			addData(browserTerms, SessionTypeConstants.BROWSER, channel, host, date);
			Terms deviceTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.DEVICE);
			addData(deviceTerms, SessionTypeConstants.DEVICE, channel, host, date);
			Terms networkTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.NETWORK);
			addData(networkTerms, SessionTypeConstants.NETWORK, channel, host, date);
			Terms osTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.OS);
			addData(osTerms, SessionTypeConstants.OS, channel, host, date);
			Terms trackerTerms = bhaskarChannel.getAggregations().get(SessionTypeConstants.TRACKER);
			addData(trackerTerms, SessionTypeConstants.TRACKER, channel, host, date);				


			Filter divyaChannel = hostBucket.getAggregations().get("960");
			channel = 960;
			browserTerms = divyaChannel.getAggregations().get(SessionTypeConstants.BROWSER);
			addData(browserTerms, SessionTypeConstants.BROWSER, channel, host, date);
			deviceTerms = divyaChannel.getAggregations().get(SessionTypeConstants.DEVICE);
			addData(deviceTerms, SessionTypeConstants.DEVICE, channel, host, date);
			networkTerms = divyaChannel.getAggregations().get(SessionTypeConstants.NETWORK);
			addData(networkTerms, SessionTypeConstants.NETWORK, channel, host, date);
			osTerms = divyaChannel.getAggregations().get(SessionTypeConstants.OS);
			addData(osTerms, SessionTypeConstants.OS, channel, host, date);
			trackerTerms = divyaChannel.getAggregations().get(SessionTypeConstants.TRACKER);
			addData(trackerTerms, SessionTypeConstants.TRACKER, channel, host, date);				

		}
	
		if (sessionsData.size() > 0) {
			elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndexes(Indexes.CATEGORISED_SESSION_COUNT,date,date)[0], MappingTypes.MAPPING_REALTIME,
					sessionsData);
			log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
			//	System.out.println("Records inserted in categorised_session_count index, size: " + sessionsData.size());

			sessionsData.clear();
		}

	}

	private void addData(Terms termsAgg, String parameter,int channel, String host, String date) {
		for(Terms.Bucket bucket:termsAgg.getBuckets()){
			String parameterValue = bucket.getKeyAsString();
			Cardinality sessionsAgg = bucket.getAggregations().get("sessions");
			long sessions = sessionsAgg.getValue();
			Map<String,Object> data = new HashMap<>();
			data.put(Constants.CHANNEL_SLNO, channel);
			data.put(Constants.HOST, host);
			data.put(Constants.SESSION_TYPE, parameter);
			data.put(Constants.DATE, date);
			data.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
			data.put(Constants.SESSION_COUNT, sessions);
			data.put(parameter, parameterValue);
			data.put(Constants.ROWID, channel+"_"+host+"_"+parameterValue+"_"+date);
			sessionsData.add(data);
			if (sessionsData.size() > batchSize) {
				elasticSearchIndexService.indexOrUpdate(IndexUtils.getMonthlyIndexes(Indexes.CATEGORISED_SESSION_COUNT,date,date)[0], MappingTypes.MAPPING_REALTIME,
						sessionsData);
				log.info("Records inserted in categorised_session_count index, size: " + sessionsData.size());
				//System.out.println("Records inserted in categorised_session_count index, size: " + sessionsData.size());

				sessionsData.clear();
			}
		}		
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();		
		SessionCountIndexerDaily sci = new SessionCountIndexerDaily();

		sci.insertData(DateUtil.getPreviousDate().replaceAll("_", "-"));
		log.info("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
		//System.out.println("Total time taken (Seconds):  "+((System.currentTimeMillis()-start)/1000));
	}

}
