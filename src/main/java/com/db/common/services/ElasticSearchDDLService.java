package com.db.common.services;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.IndexNotFoundException;

import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.EsTransportClient;
import com.db.common.utils.IndexUtils;

public class ElasticSearchDDLService {

	private static Logger log = LogManager.getLogger(ElasticSearchDDLService.class);

	/**
	 * Attribute for using ElasticSeach client
	 */
	private TransportClient client;

	private static ElasticSearchDDLService elasticSearchDDLService = null;

	public static ElasticSearchDDLService getInstance() {
		if (elasticSearchDDLService == null) {
			elasticSearchDDLService = new ElasticSearchDDLService();
		}
		return elasticSearchDDLService;
	}

	private EsTransportClient esTransportClient = EsTransportClient.getInstance();

	/**
	 * Constructs a ElasticSearchIndexService Object on the basis of the
	 * configuration. See <code>config.properties</code>
	 */
	public ElasticSearchDDLService() {
		// pass the name of elasticsearch cluster
		try {
			if (this.client != null) {
				client.close();
			}
			this.client = esTransportClient.getClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public void deleteIndex(String index) {
		try {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
			DeleteIndexResponse deleteIndexResponse = client.admin().indices().delete(deleteIndexRequest).actionGet();
			if (deleteIndexResponse.isAcknowledged()) {
				System.out.println("Deleted index: " + index);
			}
		} catch (IndexNotFoundException indexNotFoundException) {
			log.error("Index not found with name " + index + "; " + indexNotFoundException.getMessage());
		}

	}

	/**
	 * This method close the elasticsearch client.
	 */
	public void close() {
		client.close();
	}

	public static void main(String[] args) throws ParseException {
		List<String> indexNames = Arrays.asList(IndexUtils.getDailyIndexes("identification_","2017-09-05", "2017-09-20"));
		ElasticSearchDDLService elasticSearchDDLService = ElasticSearchDDLService.getInstance();
		 for (String index : indexNames) {
		elasticSearchDDLService.deleteIndex(index);
		 }
		elasticSearchDDLService.close();
	}

}
