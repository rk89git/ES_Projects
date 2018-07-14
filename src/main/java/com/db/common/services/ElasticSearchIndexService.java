package com.db.common.services;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.db.common.constants.Indexes;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.EsTransportClient;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

//@Service
public class ElasticSearchIndexService {

	private static final String SUCCESS_COUNT_KEY = "successCount";

	private static final String FAILURE_COUNT_KEY = "failureCount";

	private static final String RETRY_LIST_KEY = "retryList";

	private static final String INDEX_RESPONSES = "indexResponses";

	private static Logger log = LogManager.getLogger(ElasticSearchIndexService.class);

	private static ElasticSearchIndexService elasticSearchIndexService = null;

	private EsTransportClient esTransportClient = EsTransportClient.getInstance();

	/**
	 * Attribute for using ElasticSeach client
	 */
	private Client client;

	/**
	 * Constructs a ElasticSearchIndexService Object on the basis of the
	 * configuration. See <code>elasticsearch.properties</code> section of the
	 * configuration.
	 */
	private ElasticSearchIndexService() {
		try {
			initializeClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public static ElasticSearchIndexService getInstance() {
		if (elasticSearchIndexService == null) {
			elasticSearchIndexService = new ElasticSearchIndexService();
		}
		return elasticSearchIndexService;
	}

	/**
	 * This method initialize the Elasticsearch client.
	 *
	 * @throws NoNodeAvailableException
	 *             if all the node of es is down.
	 * @throws ClusterBlockException
	 *             if maximum shards of cluster is unassigned state
	 */
	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = esTransportClient.getClient();
	}

	public Client getClient() {
		return this.client;
	}

	/**
	 * This method true if at least one node in a ElasticSearch cluster is running
	 * else return false
	 *
	 * @return
	 */
	public boolean isServiceRunning() {
		try {
			List<NodeInfo> nodeInfos = this.client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet()
					.getNodes();
			if (nodeInfos.size() >= 1) {
				return true;
			}
			return false;
		} catch (NoNodeAvailableException nodeAvailableException) {
			// log.error("All the nodes of ElasticSearch are down : "
			// + nodeAvailableException.getMessage());
			// return false;
		} catch (Exception exception) {
			// log.error("ElasticSearch node status are not available :"
			// + exception);
			return false;
		}
		return false;
	}

	/**
	 * This method close the elasticsearch client.
	 */
	public void close() {
		log.info("Closing connection to ES.");
		client.close();
	}

	/**
	 * This method index the batch of records into the ElasticSearch. All the index
	 * methods internally calls indexRecord(...) method for indexing and storing
	 * records into ElasticSearch.
	 *
	 * @param bulkRequestBuilder
	 *            instance of ElasticSearch BulkRequestBuilder
	 * @param indexName
	 *            Name of input index name
	 */
	@SuppressWarnings("unchecked")
	private int indexRecord(BulkRequestBuilder bulkRequestBuilder, String indexName) {
		// Create bulk request.
		BulkRequest bulkRequest = bulkRequestBuilder.request();
		int successCount = 0;
		try {
			// index bulk records.
			Map<String, Object> resultMap = bulkIndex(bulkRequest);
			successCount = (Integer) resultMap.get(SUCCESS_COUNT_KEY);
			log.info("Total Records: " + bulkRequest.numberOfActions() + "; Success: " + successCount
					+ "; Failure Count: " + (Integer) resultMap.get(FAILURE_COUNT_KEY));
			// Retry to insert failure records
			if (((List<IndexRequest>) resultMap.get(RETRY_LIST_KEY)).size() > 0) {
				BulkRequest retryBulkRequest = prepareRetryBulkRequest(
						(List<IndexRequest>) resultMap.get(RETRY_LIST_KEY));
				// index bulk records.
				log.info("Records count for retry index: " + retryBulkRequest.requests().size());
				resultMap = bulkIndex(retryBulkRequest);
				successCount = successCount + (Integer) resultMap.get(SUCCESS_COUNT_KEY);
				log.info("After retry: Total Records: " + bulkRequest.numberOfActions() + "; Success: " + successCount);
				return successCount;
			} else {
				return successCount;
			}
		} catch (Exception e) {
			log.error("Records could not be indexed", e);
			// Not throwing exception to avoid breaking of entire flow of this
			// method call
			// throw new DBAnalyticsException("Record could not be indexed", e);
		}
		return successCount;
	}

	@SuppressWarnings("unchecked")
	private int updateRecord(BulkRequestBuilder bulkRequestBuilder, String indexName) {
		// Create bulk request.
		BulkRequest bulkRequest = bulkRequestBuilder.request();
		int successCount = 0;
		try {
			// index bulk records.
			Map<String, Object> resultMap = bulkUpdate(bulkRequest);
			successCount = (Integer) resultMap.get(SUCCESS_COUNT_KEY);
			log.info("Total Records: " + bulkRequest.numberOfActions() + "; Success: " + successCount
					+ "; Failure Count: " + (Integer) resultMap.get(FAILURE_COUNT_KEY));

			// Retry to insert failure records
			if (((List<UpdateRequest>) resultMap.get(RETRY_LIST_KEY)).size() > 0) {
				BulkRequest retryBulkRequest = prepareRetryBulkRequest(
						(List<UpdateRequest>) resultMap.get(RETRY_LIST_KEY));
				// index bulk records.
				log.info("Records count for retry update: " + retryBulkRequest.requests().size());
				resultMap = bulkUpdate(retryBulkRequest);
				successCount = successCount + (Integer) resultMap.get(SUCCESS_COUNT_KEY);
				log.info("After retry: Total Records: " + bulkRequest.numberOfActions() + "; Success: " + successCount);
				return successCount;
			} else {
				return successCount;
			}
		} catch (Exception e) {
			log.error("Records could not be updated", e);
			// Not throwing exception to avoid breaking of entire flow of this
			// method call
			// throw new DBAnalyticsException("Record could not be indexed", e);
		}
		return successCount;
	}

	/**
	 * Prepares the retry bulk index request.
	 *
	 * @param retryIndexRequestList
	 *            List of {@link IndexRequest}s to be indexed
	 * @return {@link BulkRequest} object
	 */
	private BulkRequest prepareRetryBulkRequest(List retryIndexRequestList) {
		// Create bulk request builder.

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Object request : retryIndexRequestList) {
			if (request instanceof IndexRequest) {
				bulkRequestBuilder.add((IndexRequest) request);
			} else if (request instanceof UpdateRequest) {
				bulkRequestBuilder.add((UpdateRequest) request);
			} else if (request instanceof DeleteRequest) {
				bulkRequestBuilder.add((DeleteRequest) request);
			}

		}
		// Create bulk request and return.
		return bulkRequestBuilder.request();
	}

	/**
	 * Bulk index.
	 * 
	 * @param bulkRequest
	 *            the bulk request
	 */
	private Map<String, Object> bulkIndex(BulkRequest bulkRequest) {
		try {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			List<IndexRequest> retryList = new ArrayList<>();
			if (bulkRequest.numberOfActions() > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

				if (bulkResponse.hasFailures()) {
					BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
					List<DocWriteRequest> actionRequests = bulkRequest.requests();
					int successCount = 0;
					int failureCount = 0;
					for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
						if (!bulkItemResponse.isFailed()) {
							successCount++;
						} else {
							failureCount++;
							log.error("Record for index [" + bulkItemResponse.getIndex() + "] with id ["
									+ bulkItemResponse.getId()
									+ "] can't persist. We'll retry to index these records. Caused by :  "
									+ bulkItemResponse.getFailureMessage());
							retryList.add((IndexRequest) actionRequests.get(bulkItemResponse.getItemId()));
						}
					}
					resultMap.put(SUCCESS_COUNT_KEY, successCount);
					resultMap.put(FAILURE_COUNT_KEY, failureCount);
					resultMap.put(RETRY_LIST_KEY, retryList);
					return resultMap;
				} else {
					resultMap.put(SUCCESS_COUNT_KEY, bulkRequest.numberOfActions());
					resultMap.put(FAILURE_COUNT_KEY, 0);
					resultMap.put(RETRY_LIST_KEY, retryList); // blank list
					return resultMap;
				}
			} else {
				// returns empty map
				return resultMap;
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	private Map<String, Object> bulkUpdate(BulkRequest bulkRequest) {
		try {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			List<UpdateRequest> retryList = new ArrayList<>();
			if (bulkRequest.numberOfActions() > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

				if (bulkResponse.hasFailures()) {
					BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
					List<DocWriteRequest> actionRequests = bulkRequest.requests();
					int successCount = 0;
					int failureCount = 0;
					for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
						if (!bulkItemResponse.isFailed()) {
							successCount++;
						} else {
							failureCount++;
							log.error("Record for index [" + bulkItemResponse.getIndex() + "] with id ["
									+ bulkItemResponse.getId()
									+ "] can't persist. We'll retry to index these records. Caused by :  "
									+ bulkItemResponse.getFailureMessage() + bulkItemResponse.getFailure());
							retryList.add((UpdateRequest) actionRequests.get(bulkItemResponse.getItemId()));
						}
					}
					resultMap.put(SUCCESS_COUNT_KEY, successCount);
					resultMap.put(FAILURE_COUNT_KEY, failureCount);
					resultMap.put(RETRY_LIST_KEY, retryList);
					return resultMap;
				} else {
					resultMap.put(SUCCESS_COUNT_KEY, bulkRequest.numberOfActions());
					resultMap.put(FAILURE_COUNT_KEY, 0);
					resultMap.put(RETRY_LIST_KEY, retryList); // blank list
					return resultMap;
				}
			} else {
				// returns empty map
				return resultMap;
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
	}

	private Map<String, Object> bulkIndexWithResponseDetails(BulkRequest bulkRequest) {
		List<IndexResponse> indexResponses = new ArrayList<>();
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<IndexRequest> retryList = new ArrayList<IndexRequest>();
		try {
			if (bulkRequest.numberOfActions() > 0) {
				int successCount = 0;
				int failureCount = 0;
				BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

				List<DocWriteRequest> actionRequests = bulkRequest.requests();
				BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();

				for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
					IndexResponse indexResponse = bulkItemResponse.getResponse();
					indexResponses.add(indexResponse);

					if (!bulkItemResponse.isFailed()) {
						successCount++;
					} else {
						failureCount++;
						log.error("Record for index [" + bulkItemResponse.getIndex() + "] with id ["
								+ bulkItemResponse.getId()
								+ "] can't persist. We'll retry to index these records. Caused by :  "
								+ bulkItemResponse.getFailureMessage());
						retryList.add((IndexRequest) actionRequests.get(bulkItemResponse.getItemId()));
					}
				}
				resultMap.put(SUCCESS_COUNT_KEY, successCount);
				resultMap.put(FAILURE_COUNT_KEY, failureCount);
				resultMap.put(RETRY_LIST_KEY, retryList);
				resultMap.put(INDEX_RESPONSES, indexResponses);
				return resultMap;
			} else {
				resultMap.put(SUCCESS_COUNT_KEY, bulkRequest.numberOfActions());
				resultMap.put(FAILURE_COUNT_KEY, 0);
				resultMap.put(RETRY_LIST_KEY, retryList); // blank list
				resultMap.put(INDEX_RESPONSES, indexResponses);
				// returns empty map
				return resultMap;
			}
		} catch (Exception e) {
			log.error("Records could not be indexed", e);
			resultMap.put(SUCCESS_COUNT_KEY, 0);
			resultMap.put(FAILURE_COUNT_KEY, 0);
			resultMap.put(RETRY_LIST_KEY, retryList); // blank list
			resultMap.put(INDEX_RESPONSES, indexResponses);
			return resultMap;
			// throw new DBAnalyticsException(e);
		}
	}

	/**
	 * Index the batch of records into the indexer.
	 * 
	 * The batch of records passed to index method. The input can have a field
	 * <code>rowId</code> if key is already generated for that records.
	 * 
	 * @param indexName
	 * @param mappingType
	 * @param recordList
	 * @return
	 */
	public int index(String indexName, String mappingType, List<Map<String, Object>> recordList) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;
		for (Map<String, Object> record : recordList) {
			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);

			String bulkRequestKey = mappingType + "#bulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			// index request
			IndexRequest indexRequest = new IndexRequest(indexName, mappingType);

			indexRequest.source(record);
			if (StringUtils.isNotBlank(rowId)) {
				indexRequest.id(rowId);
			}
			indexBulkMap.get(bulkRequestKey).add(indexRequest);
		}

		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + indexRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("Number of records indexed : " + persisted);

		return persisted;
	}

	public List<IndexResponse> indexWithResponse(String indexName, String mappingType,
			List<Map<String, Object>> recordList) {

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		List<IndexResponse> indexResponses = null;
		for (Map<String, Object> record : recordList) {
			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);

			// index request
			IndexRequest indexRequest = new IndexRequest(indexName, mappingType);

			indexRequest.source(record);
			if (StringUtils.isNotBlank(rowId)) {
				indexRequest.id(rowId);
			}
			bulkRequestBuilder.add(indexRequest);
		}

		BulkRequest bulkRequest = bulkRequestBuilder.request();
		Map<String, Object> resultMap = bulkIndexWithResponseDetails(bulkRequest);
		// If there is any failure then reindex failure records
		if (((List) resultMap.get(RETRY_LIST_KEY)).size() > 0) {
			BulkRequest retryBulkRequest = prepareRetryBulkRequest((List<IndexRequest>) resultMap.get(RETRY_LIST_KEY));
			// index bulk records.
			bulkIndexWithResponseDetails(retryBulkRequest);
		}

		indexResponses = (List<IndexResponse>) resultMap.get(INDEX_RESPONSES);
		log.info("Number of records indexed : " + indexResponses.size());

		return indexResponses;
	}

	/**
	 * Index the batch of records into the indexer.
	 * <p>
	 * The batch of records passed to index method must have <code>indexName</code>
	 * and <code>timestamp</code> keys. The <code>indexName</code> is the name of
	 * index and <code>timestamp</code> is used for time based partitioning of the
	 * data.
	 * <p>
	 * The input can have a field <code>rowId</code> if key is already generated for
	 * that records or specify the keyGenerator class in indexing schema file.
	 *
	 * @param recordList
	 *            List of messages to be indexed.
	 */
	public int indexOrUpdate(String indexName, String mappingType, List<Map<String, Object>> recordList) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;
		for (Map<String, Object> record : recordList) {

			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);

			// Getting bulk request key from the index Map
			String bulkRequestKey = mappingType + "#bulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexName);
			updateRequest.type(mappingType);
			updateRequest.id(rowId);
			updateRequest.docAsUpsert(true);
			updateRequest.doc(record);
			updateRequest.retryOnConflict(5);

			indexBulkMap.get(bulkRequestKey).add(updateRequest);
		}
		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("Number of records indexed : " + persisted);
		return persisted;
	}

	/**
	 * @param indexName
	 * @param mappingType
	 * @param recordList
	 * @param keyToAppend
	 * @return
	 */
	public int indexOrUpdateWithScriptAndAppend(String indexName, String mappingType,
			List<Map<String, Object>> recordList, String keyToAppend) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;
		for (Map<String, Object> record : recordList) {

			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);
			Collection<?> valueToAppend = (Collection<?>) record.remove(keyToAppend);

			// Getting bulk request key from the index Map
			String bulkRequestKey = mappingType + "#bulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexName);
			updateRequest.type(mappingType);
			updateRequest.id(rowId);
			updateRequest.scriptedUpsert(true);
			updateRequest.upsert(record);
			updateRequest.retryOnConflict(5);
			Map<String, Object> params = new HashMap<>();
			params.put("record", record);
			params.put("c", valueToAppend);
			String scriptCode=null;
			if(valueToAppend!=null)
			 scriptCode = docTOPainlessScript() + painlessScriptToAppendArray(keyToAppend, valueToAppend);
			else
				scriptCode = docTOPainlessScript();
			updateRequest.script(new Script(ScriptType.INLINE, "painless", scriptCode, params));
			indexBulkMap.get(bulkRequestKey).add(updateRequest);
		}
		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("Number of records indexed : " + persisted);
		return persisted;
	}

	String docTOPainlessScript() {
		StringBuilder result = new StringBuilder();

		String entryScript = "ctx._source.putAll(params.record);";

		result.append(entryScript);

		return result.toString();

	}

	private String painlessScriptToAppendArray(String key, Collection c) {
		return "if (ctx._source." + key + "== null ) {ctx._source." + key + " =params.c } else if (!ctx._source." + key
				+ ".containsAll(params.c)) {ctx._source." + key + ".addAll(params.c);}";
	}

	public int removeFromArrayElements(String indexName, String mappingType, List<Map<String, Object>> recordList,
			List<String> keysToRemove) {

		/** The index bulk map. */
		int persisted = 0;
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> record : recordList) {
			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);
			if (StringUtils.isBlank(rowId)) {
				log.error("Invalid record, _id missing. Record: " + record);
				continue;
			}
			StringBuilder scriptToUpdateRecord = new StringBuilder();
			int i = 0;
			Map<String, Object> params = new HashMap<>();
			for (String key : keysToRemove) {

				String value = "value" + i;
				scriptToUpdateRecord
						.append("if (ctx._source." + key + " != null && ctx._source." + key + ".containsAll(params."
								+ value + ")) {ctx._source." + key + ".removeAll(params." + value + ")} ");
				params.put(value, record.get(key));
				i++;
			}

			UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);
			updateRequest.script(new Script(ScriptType.INLINE, "painless", scriptToUpdateRecord.toString(), params));
			updateRequest.retryOnConflict(5);

			bulkRequestBuilder.add(updateRequest);
		}

		// Bulk indexing
		persisted = persisted + updateRecord(bulkRequestBuilder, indexName);

		log.info(persisted + " records updated as array element.");
		return persisted;
	}

	public List<UpdateResponse> indexOrUpdateWithResponse(String indexName, String mappingType,
			List<Map<String, Object>> recordList) {
		List<UpdateResponse> updateResponses = null;
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		for (Map<String, Object> record : recordList) {

			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);

			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexName);
			updateRequest.type(mappingType);
			updateRequest.id(rowId);
			updateRequest.docAsUpsert(true);
			updateRequest.doc(record);
			updateRequest.retryOnConflict(5);
			updateRequest.fetchSource(true);
			bulkRequestBuilder.add(updateRequest);
		}
		BulkRequest bulkRequest = bulkRequestBuilder.request();
		updateResponses = bulkUpdateWithResponseDetails(bulkRequest);

		log.info("Number of records indexed : " + updateResponses.size());
		return updateResponses;
	}

	private List<UpdateResponse> bulkUpdateWithResponseDetails(BulkRequest bulkRequest) {
		List<UpdateResponse> updateResponses = new ArrayList<>();
		try {
			if (bulkRequest.numberOfActions() > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

				BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
				for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
					UpdateResponse updateResponse = bulkItemResponse.getResponse();
					updateResponses.add(updateResponse);
				}

			} else {
				// returns empty map
				return updateResponses;
			}
		} catch (Exception e) {
			throw new DBAnalyticsException(e);
		}
		return updateResponses;
	}

	public Map<String, Object> indexWithResponseId(String indexName, String mappingType, Map<String, Object> record) {
		Map<String, Object> responseMap = new HashMap<>();
		IndexResponse indexResponse = client.prepareIndex(indexName, mappingType).setSource(record).execute()
				.actionGet();
		String responseId = indexResponse.getId();
		responseMap.put(Constants.ROWID, responseId);
		log.info("Record indexed successfully in ElasticSearch with id: " + responseId);
		return responseMap;
	}

	public int indexOrUpdate(String indexName, String mappingType, Map<String, Object> record) {
		return this.indexOrUpdate(indexName, mappingType, ImmutableList.of(record));
	}

	public int setValue(String indexName, String mappingType, List<String> rowIds, String field, Object value) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;

		for (String rowId : rowIds) {
			// Getting bulk request key from the index Map
			String bulkRequestKey = mappingType + "#bulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			Map<String, Object> pvrecord = new HashMap<String, Object>();
			pvrecord.put(field, value);
			UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);
			Map<String, Object> params = new HashMap<>();
			params.put("value", value);

			updateRequest.script(
					new Script(ScriptType.INLINE, "painless", "ctx._source." + field + "=params.value;", params));
			updateRequest.upsert(pvrecord);
			updateRequest.retryOnConflict(5);
			indexBulkMap.get(bulkRequestKey).add(updateRequest);
		}
		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("value updated  in " + persisted + " records");
		return persisted;
	}

	public void incrementFieldParallel(final String index, final String type, final List<String> _ids,
			final String field, final int value) {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.submit(new Runnable() {
			@Override
			public void run() {
				elasticSearchIndexService.incrementCounter(index, type, _ids, field, value);
			}
		});
		executorService.shutdown();
	}

	public int incrementCounter(String indexName, String mappingType, List<String> rowIds, String field, int value) {
		Map<String, Integer> fieldCounterMap = new HashMap<>();
		fieldCounterMap.put(field, value);

		Map<String, Map<String, Integer>> rowIdTofieldCounterMap = new HashMap<>();
		for (String rowId : rowIds) {
			rowIdTofieldCounterMap.put(rowId, fieldCounterMap);
		}

		return incrementCounter(indexName, mappingType, rowIdTofieldCounterMap);
	}

	public int incrementCounter(String indexName, String mappingType, Map<String, Integer> rowIdCounterMap,
			String field) {

		Map<String, Map<String, Integer>> rowIdTofieldCounterMap = new HashMap<>();
		for (String rowId : rowIdCounterMap.keySet()) {
			Map<String, Integer> fieldCounterMap = new HashMap<>();
			fieldCounterMap.put(field, rowIdCounterMap.get(rowId));
			rowIdTofieldCounterMap.put(rowId, fieldCounterMap);
		}

		return incrementCounter(indexName, mappingType, rowIdTofieldCounterMap);
	}

	public int incrementCounter(String indexName, String mappingType,
			Map<String, Map<String, Integer>> rowIdTofieldCounterMap) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;

		for (String rowId : rowIdTofieldCounterMap.keySet()) {
			// Getting bulk request key from the index Map
			String bulkRequestKey = mappingType + "#bulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			Map<String, Integer> fieldCounterMap = rowIdTofieldCounterMap.get(rowId);
			UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);
			StringBuilder scriptToUpdateRecord = new StringBuilder();
			Map<String, Object> params = new HashMap<String, Object>();
			int i = 0;
			for (String fieldName : fieldCounterMap.keySet()) {
				String value = "value" + i++;
				params.put(value, (int) fieldCounterMap.get(fieldName));

				scriptToUpdateRecord.append("if (ctx._source." + fieldName + " == null ) {ctx._source." + fieldName
						+ "=params." + value + " } else {ctx._source." + fieldName + "+=params." + value + "} ");

			}
			updateRequest.script(new Script(ScriptType.INLINE, "painless", scriptToUpdateRecord.toString(), params));
			updateRequest.upsert(fieldCounterMap);
			updateRequest.retryOnConflict(5);
			indexBulkMap.get(bulkRequestKey).add(updateRequest);
		}
		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("Counter updated  in " + persisted + " records");
		return persisted;
	}

	public int indexOrUpdateAsArrayElements(String indexName, String mappingType,
			List<Map<String, Object>> recordList) {

		/** The index bulk map. */
		int persisted = 0;
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> record : recordList) {
			// Getting id from the index Map
			String rowId = (String) record.remove(Constants.ROWID);
			if (StringUtils.isBlank(rowId)) {
				log.error("Invalid record, _id missing. Record: " + record);
				continue;
			}
			Map<String, Object> upsertRecord = new HashMap<String, Object>();
			StringBuilder scriptToUpdateRecord = new StringBuilder();
			Map<String, Object> params = new HashMap<String, Object>();
			int i = 0;
			for (String key : record.keySet()) {
				upsertRecord.put(key, Arrays.asList(record.get(key)));
				// scriptToUpdateRecord.append("ctx._source." + key + " == null
				// ? (ctx._source." + key
				// + "="+Arrays.asList(record.get(key))+") : (ctx._source." +
				// key + ".add("+record.get(key)+"));");
				String value = "value" + i++;
				params.put(value, record.get(key));
				scriptToUpdateRecord.append("if (ctx._source." + key + " == null ) {ctx._source." + key + "=[params."
						+ value + "] } else if (!ctx._source." + key + ".contains(params." + value + ")) {ctx._source."
						+ key + ".add(params." + value + ")} ");
			}
			// System.err.println(scriptToUpdateRecord.toString());

			UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);

			updateRequest.script(new Script(ScriptType.INLINE, "painless", scriptToUpdateRecord.toString(), params));
			// updateRequest.addScriptParam("values",
			// storyId).addScriptParam("sectionsValue", section);

			updateRequest.upsert(upsertRecord);
			updateRequest.retryOnConflict(5);
			bulkRequestBuilder.add(updateRequest);
		}

		// Bulk indexing
		persisted = persisted + updateRecord(bulkRequestBuilder, indexName);

		log.info(persisted + " records updated as array element.");
		return persisted;
	}

	public int index(String indexName, List<Map<String, Object>> recordList) {
		return index(indexName, MappingTypes.MAPPING_REALTIME, recordList);
	}

	public synchronized int index(String indexName, Map<String, Object> record) {
		return this.index(indexName, ImmutableList.of(record));
	}

	public synchronized int delete(String indexName, String mappingType, List<String> rowIds) {

		/** The index bulk map. */
		Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
		int persisted = 0;

		for (String rowId : rowIds) {
			// Getting bulk request key from the index Map
			String bulkRequestKey = mappingType + "#deletebulrequest";
			// rowId is used as primary key and store into ES.
			if (!indexBulkMap.containsKey(bulkRequestKey)) {
				indexBulkMap.put(bulkRequestKey, client.prepareBulk());
			}

			DeleteRequest deleteRequest = new DeleteRequest(indexName, mappingType, rowId);
			indexBulkMap.get(bulkRequestKey).add(deleteRequest);
		}
		for (String bulkRequestKey : indexBulkMap.keySet()) {
			// Bulk indexing
			persisted = persisted + indexRecord(indexBulkMap.get(bulkRequestKey), indexName);
		}
		log.info("Records count deleted from [" + indexName + "]: " + persisted);
		return persisted;
	}

	public synchronized long deleteByToken(String indexName, String mappingType, List<String> rowIds) {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client).source(indexName)
				.filter(QueryBuilders.termsQuery("device_token", rowIds)).get();

		// DeleteByQueryResponse response =
		// client.prepareDeleteByQuery(indexName).setTypes(mappingType)
		// .setQuery(QueryBuilders.termsQuery("device_token",
		// rowIds)).execute().actionGet();

		return response.getDeleted();
	}

	public int copy(String sourceIndexName, String destIndexName, String sourceMappingType, String destMappingType,
			List<String> source, List<String> destination) {
		int indexOfList = 0;
		List<Map<String, Object>> updatedRecords = new ArrayList<Map<String, Object>>();
		MultiGetRequest multiGetRequest = new MultiGetRequest();

		for (String oldId : source) {
			multiGetRequest.add(sourceIndexName, sourceMappingType, oldId);
		}

		MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();
		for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
			Map<String, Object> summaryObj = multiGetItemResponse.getResponse().getSourceAsMap();
			if (summaryObj != null) {
				summaryObj.put(Constants.ROWID, multiGetItemResponse.getResponse().getId());
				updatedRecords.add(summaryObj);
			}
			indexOfList++;
		}

		// Index all records
		int persisted = index(destIndexName, destMappingType, updatedRecords);
		log.info("Records count copied in [" + destIndexName + "]: " + persisted);
		return persisted;
	}

	public void deleteIndex(String index, String type) {

		DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchAllQuery()).source(index)
				.get();

		System.out.println("Deleted data from index: " + index + " type: " + type);
	}

	public List<Map<String, Object>> getSourceList(String index, String type, Collection<String> _ids) {
		List<Map<String, Object>> sourceList = new ArrayList<>();
		MultiGetRequest multiGetRequest = new MultiGetRequest();
		_ids.forEach(_id -> multiGetRequest.add(index, type, _id));
		MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest).actionGet();
		sourceList = Arrays.stream(multiGetResponse.getResponses())
				.map(multiGetItemResponse -> multiGetItemResponse.getResponse().getSourceAsMap())
				.collect(Collectors.toList());
		return sourceList;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ElasticSearchIndexService elasticSearchIndexService = new ElasticSearchIndexService();
		// Map<String,Object> record=new HashMap<String, Object>();
		// record.put("name", "hanish");
		// record.put("age", "21");
		/*
		 * List<String> rowIds = new ArrayList<String>();
		 * rowIds.add("AVmDNSHP6rbjlFkL2EJj"); Map<String, Object> records = new
		 * HashMap<String, Object>(); records.put("success", 100L);
		 * records.put("failure", 5L); elasticSearchIndexService.index("testhb",
		 * records);
		 */
		// elasticSearchIndexService.deleteIndex("story_detail_hourly",
		// "realtime");
		// System.exit(0);

		Map<String, Object> pp = new HashMap<String, Object>();
		pp.put("_id", "1");
		pp.put("storyid", "a1");

		Map<String, Object> pp2 = new HashMap<String, Object>();
		pp2.put("_id", "4");
		pp2.put("storyid", "a2");
		pp2.put("section", "Bollywood");

		Map<String, Object> pp3 = new HashMap<String, Object>();
		pp3.put("_id", "3");
		pp3.put("storyid", "a3");
		pp3.put("section", "Bollywood");

		List<Map<String, Object>> recordList = new ArrayList<Map<String, Object>>();
		// recordList.add(pp);
		recordList.add(pp2);
		// recordList.add(pp3);

		elasticSearchIndexService.indexOrUpdateAsArrayElements("testhb12", "realtime", recordList);

	}

	public SearchResponse getSearchResponse(String indexName, String typeName, QueryBuilder queryBuilder, int from,
			int size, Map<String, String> sortOrder, String[] includeFields, String[] excludeFields,
			AggregationBuilder aggregationBuilder, TimeValue keepAlive) {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(typeName)
				.setQuery(queryBuilder).setFrom(from).setSize(size);
		if (sortOrder != null)
			sortOrder.forEach((s, s2) -> searchRequestBuilder.addSort(s, SortOrder.fromString(s2)));
		searchRequestBuilder.setFetchSource(includeFields, excludeFields);
		if (aggregationBuilder != null)
			searchRequestBuilder.addAggregation(aggregationBuilder);
		if (keepAlive != null)
			searchRequestBuilder.setScroll(keepAlive);
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		return searchResponse;
	}

	public SearchResponse getSearchResponse1(String[] indexName, String typeName, QueryBuilder queryBuilder, int from,
			int size, Map<String, String> sortOrder, String[] includeFields, String[] excludeFields,
			List<AggregationBuilder> aggregationBuilders, TimeValue keepAlive) {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(typeName)
				.setQuery(queryBuilder).setFrom(from).setSize(size);
		if (sortOrder != null)
			sortOrder.forEach((s, s2) -> searchRequestBuilder.addSort(s, SortOrder.fromString(s2)));
		searchRequestBuilder.setFetchSource(includeFields, excludeFields);
		if (aggregationBuilders != null)
			aggregationBuilders.forEach(agg -> searchRequestBuilder.addAggregation(agg));
		if (keepAlive != null)
			searchRequestBuilder.setScroll(keepAlive);
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		return searchResponse;
	}

	public SearchResponse getScrollResponse(String scrollId, TimeValue keepAlive) {
		return client.prepareSearchScroll(scrollId).setScroll(keepAlive).execute().actionGet();
	}

	@SuppressWarnings("unchecked")
	public Map<String, Long> getTopUniqueValues(String indexes, String types, String field, int size) {
		Map<String, Long> topValues = new HashMap<>();
		SearchResponse response = client.prepareSearch().setIndices(indexes.split(String.valueOf(",")))
				.setTypes(types.split(String.valueOf(","))).setSize(0)
				.addAggregation(AggregationBuilders.terms("topValues").field(field).size(size)).execute().actionGet();
		Terms topValuesAggr = response.getAggregations().get("topValues");
		List<Terms.Bucket> topValuesBuckets = (List<Terms.Bucket>) topValuesAggr.getBuckets();
		topValues = topValuesBuckets.stream().collect(Collectors.toMap(Terms.Bucket::getKeyAsString,
				Terms.Bucket::getDocCount, (oldValue, newValue) -> newValue, LinkedHashMap::new));
		return topValues;
	}

	public int updateDocWithIncrementCounter(String indexName, String mappingType,
			Map<String, Map<String, Integer>> rowIdTofieldCounterMap,
			Map<String, Map<String, Object>> rowIdToUpdateFieldMap) throws Exception {
		try {
			Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
			int persisted = 0;

			for (String rowId : rowIdTofieldCounterMap.keySet()) {
				// Getting bulk request key from the index Map
				String bulkRequestKey = mappingType + "#bulrequest";
				// rowId is used as primary key and store into ES.
				if (!indexBulkMap.containsKey(bulkRequestKey)) {
					indexBulkMap.put(bulkRequestKey, client.prepareBulk());
				}

				Map<String, Integer> fieldCounterMap = rowIdTofieldCounterMap.get(rowId);
				UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);

				Map<String, Object> fieldUpdateMap = null;
				if (rowIdToUpdateFieldMap != null && rowIdToUpdateFieldMap.containsKey(rowId)) {
					fieldUpdateMap = rowIdToUpdateFieldMap.get(rowId);
				}

				StringBuilder scriptToUpdateRecord = new StringBuilder();
				Map<String, Object> params = new HashMap<String, Object>();

				int i = 0;
				for (String fieldName : fieldCounterMap.keySet()) {
					params.put(fieldName, (int) fieldCounterMap.get(fieldName));

					scriptToUpdateRecord
							.append("if (ctx._source." + fieldName + " == null ) {ctx._source." + fieldName + "=params."
									+ fieldName + " } else {ctx._source." + fieldName + "+=params." + fieldName + "}");

				}

				if (fieldUpdateMap != null && !fieldUpdateMap.isEmpty()) {
					for (String fieldName : fieldUpdateMap.keySet()) {
						params.put(fieldName, fieldUpdateMap.get(fieldName));

						scriptToUpdateRecord.append("ctx._source." + fieldName + "= params." + fieldName + ";");
					}
				}

				updateRequest
						.script(new Script(ScriptType.INLINE, "painless", scriptToUpdateRecord.toString(), params));
				updateRequest.upsert(params);
				updateRequest.retryOnConflict(5);
				indexBulkMap.get(bulkRequestKey).add(updateRequest);
			}
			for (String bulkRequestKey : indexBulkMap.keySet()) {
				// Bulk indexing
				persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
			}
			log.info("Counter updated  in " + persisted + " records");
			return persisted;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public TermsAggregationBuilder getVersionSpecificOrderedAggregation(TermsAggregationBuilder termsAggregationBuilder,
			String path, boolean asc) {
		return termsAggregationBuilder.order(Terms.Order.aggregation(path, asc));
	}

	public int updateAndAppendDocWithIncrementCounter(String indexName, String mappingType,
			Map<String, Map<String, Integer>> rowIdTofieldCounterMap,
			Map<String, Map<String, Object>> rowIdToUpdateFieldMap,
			Map<String, Map<String, Collection<?>>> rowIdToAppendFieldMap) throws Exception {
		try {
			Map<String, BulkRequestBuilder> indexBulkMap = new HashMap<String, BulkRequestBuilder>();
			int persisted = 0;

			for (String rowId : rowIdTofieldCounterMap.keySet()) {
				// Getting bulk request key from the index Map
				String bulkRequestKey = mappingType + "#bulrequest";
				// rowId is used as primary key and store into ES.
				if (!indexBulkMap.containsKey(bulkRequestKey)) {
					indexBulkMap.put(bulkRequestKey, client.prepareBulk());
				}

				Map<String, Integer> fieldCounterMap = rowIdTofieldCounterMap.get(rowId);
				UpdateRequest updateRequest = new UpdateRequest(indexName, mappingType, rowId);

				Map<String, Object> fieldUpdateMap = null;
				if (rowIdToUpdateFieldMap != null && rowIdToUpdateFieldMap.containsKey(rowId)) {
					fieldUpdateMap = rowIdToUpdateFieldMap.get(rowId);
				}

				StringBuilder scriptToUpdateRecord = new StringBuilder();
				Map<String, Object> params = new HashMap<String, Object>();

				int i = 0;
				for (String fieldName : fieldCounterMap.keySet()) {
					params.put(fieldName, (int) fieldCounterMap.get(fieldName));

					scriptToUpdateRecord
							.append("if (ctx._source." + fieldName + " == null ) {ctx._source." + fieldName + "=params."
									+ fieldName + " } else {ctx._source." + fieldName + "+=params." + fieldName + "}");

				}

				if (fieldUpdateMap != null && !fieldUpdateMap.isEmpty()) {
					for (String fieldName : fieldUpdateMap.keySet()) {
						params.put(fieldName, fieldUpdateMap.get(fieldName));

						scriptToUpdateRecord.append("ctx._source." + fieldName + "= params." + fieldName + ";");
					}
				}

				if (rowIdToAppendFieldMap != null) {
					Map<String, Collection<?>> appendFieldMap = rowIdToAppendFieldMap.get(rowId);
					if (appendFieldMap != null)
						appendFieldMap.forEach((s, o) -> {
							scriptToUpdateRecord.append(painlessScriptToAppendArray(s, o));
							params.put("c", o);
						});
				}

				//log.info("scriptToUpdateRecord" + scriptToUpdateRecord);

				updateRequest
						.script(new Script(ScriptType.INLINE, "painless", scriptToUpdateRecord.toString(), params));
				updateRequest.upsert(params);
				updateRequest.retryOnConflict(5);
				indexBulkMap.get(bulkRequestKey).add(updateRequest);
			}
			for (String bulkRequestKey : indexBulkMap.keySet()) {
				// Bulk indexing
				persisted = persisted + updateRecord(indexBulkMap.get(bulkRequestKey), indexName);
			}
			log.info("Counter updated  in " + persisted + " records");
			return persisted;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * returns response for a multi get query
	 * 
	 * @param request
	 * @return
	 */

	public Map<String, Map<String, Object>> getMultiRequestResponse(MultiGetRequest request) {
		Map<String, Object> response = null;
		Map<String, Map<String, Object>> responseObMap = new HashMap<>();
		MultiGetResponse multiGetResponse = client.multiGet(request).actionGet();

		for (MultiGetItemResponse multiGetItemResponse : multiGetResponse.getResponses()) {
			String id = multiGetItemResponse.getId();

			if (multiGetItemResponse.getResponse()!=null) {
				response = multiGetItemResponse.getResponse().getSourceAsMap();
			}

			if (response != null) {
				responseObMap.put(id, response);
			}
		}

		return responseObMap;
	}

}
