package com.db.elastic.services;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.DBConfig;
import com.db.elastic.model.Response;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * The Class ESExecutorService.
 */
@Service
public class ESExecutorService {

	/** The config. */
	private DBConfig config = DBConfig.getInstance();

	/** The log. */
	private static Logger log = LogManager.getLogger(ESExecutorService.class);

	/** The request config. */
	private RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(60 * 1000).build();

	/** The http client. */
	private HttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

	/** The gson. */
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	/** The host list. */
	private List<String> hostList = new ArrayList<>();

	/** The port. */
	private String port = config.getProperty("index.elasticsearch.http.port");

	{
		String eSNodes = config.getProperty("index.elasticsearch.connect");
		for (Object node : eSNodes.split(",")) {
			if ("".equals(node)) {
				throw new DBAnalyticsException("Invalid index.elasticsearch.connect property in config file");
			}
			String hostName = ((String) node).split(":")[0];
			hostList.add(hostName);
		}

	}

	/**
	 * Post search request.
	 *
	 * @param indexName
	 *            the index name
	 * @param type
	 *            the type
	 * @param query
	 *            the query
	 * @return the string
	 */
	public String postSearchRequest(String indexName, String type, String query) {
		String response = "";
		Long executionTime = 0L;
		try {
			Long start = System.currentTimeMillis();
			Random random = new Random();
			int n = random.nextInt(hostList.size());
			String url = "http://" + hostList.get(n) + ":" + port + "/" + indexName + "/" + type + "/_search";

			response = getPOSTResponse(url, query);
			Long end = System.currentTimeMillis();
			executionTime = end - start;
			Response res = gson.fromJson(response, Response.class);
			log.info("POST _search request [URL=" + url + "] query response time (ms): " + res.getTook()
					+ "; Execution time (ms): " + executionTime);

		} catch (Exception e) {
			throw new DBAnalyticsException("Error occured executing POST _search request." + e.getMessage(), e);
		}

		return response;
	}

	/**
	 * Post search without type request.
	 *
	 * @param indexName
	 *            the index name
	 * @param query
	 *            the query
	 * @return the string
	 */
	public String postSearchWithoutTypeRequest(String indexName, String query) {
		String response = "";
		Long executionTime = 0L;
		try {
			Long start = System.currentTimeMillis();
			Random random = new Random();
			int n = random.nextInt(hostList.size());
			String url = "http://" + hostList.get(n) + ":" + port + "/" + indexName + "/_search";

			response = getPOSTResponse(url, query);
			Long end = System.currentTimeMillis();
			executionTime = end - start;
			Response res = gson.fromJson(response, Response.class);
			log.info("POST _search without type request [URL=" + url + "] query response time (ms): " + res.getTook()
					+ ";Execution time (ms): " + executionTime);

		} catch (Exception e) {
			throw new DBAnalyticsException(
					"Error occured executing POST _search without type request." + e.getMessage(), e);
		}

		return response;
	}

	/**
	 * Gets the document by id.
	 *
	 * @param indexName
	 *            the index name
	 * @param type
	 *            the type
	 * @param request
	 *            the doc id
	 * @return the document by id
	 */
	public String getRequest(String indexName, String type, String request) {
		String response = "";
		Long executionTime = 0L;
		try {
			Long start = System.currentTimeMillis();
			Random random = new Random();
			int n = random.nextInt(hostList.size());
			String url = "http://" + hostList.get(n) + ":" + port + "/" + indexName + "/" + type + "/" + request;
			response = getGETResponse(url);
			Long end = System.currentTimeMillis();
			executionTime = end - start;
			log.info("GET request [URL=" + url + "] Execution time (ms): " + executionTime);
		} catch (Exception e) {
			throw new DBAnalyticsException("Error occured executing GET request." + e.getMessage(), e);
		}
		return response;
	}

	/**
	 * Gets the request without type .
	 *
	 * @param indexName
	 *            the index name
	 * @return the search without type request
	 */
	public String getRequestWithoutType(String indexName, String request) {
		String response = "";
		Long executionTime = 0L;
		try {
			Long start = System.currentTimeMillis();
			Random random = new Random();
			int n = random.nextInt(hostList.size());
			String url = "http://" + hostList.get(n) + ":" + port + "/" + indexName + "/" + request;
			response = getGETResponse(url);
			Long end = System.currentTimeMillis();
			executionTime = end - start;
			Response res = gson.fromJson(response, Response.class);
			log.info("GET request [URL=" + url + "] without type  query response time (ms): " + res.getTook()
					+ "; Execution time (ms): " + executionTime);
		} catch (Exception e) {
			throw new DBAnalyticsException("Error occured executing GET request without type." + e.getMessage(), e);
		}
		return response;
	}

	/**
	 * Gets the gets the response.
	 *
	 * @param url
	 *            the url
	 * @return the gets the response
	 * @throws ClientProtocolException
	 *             the client protocol exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private String getGETResponse(String url) throws ClientProtocolException, IOException {
		HttpGet request = new HttpGet(url);
		HttpResponse httpResponse = httpClient.execute(request);
		BufferedReader br = new BufferedReader(new InputStreamReader((httpResponse.getEntity().getContent())));
		String response = br.readLine();
		return response;
	}
	
	private String getDeleteResponse(String url) throws ClientProtocolException, IOException {
		HttpDelete request = new HttpDelete(url);
		HttpResponse httpResponse = httpClient.execute(request);
		BufferedReader br = new BufferedReader(new InputStreamReader((httpResponse.getEntity().getContent())));
		String response = br.readLine();
		return response;
	}

	/**
	 * Gets the POST response.
	 *
	 * @param url
	 *            the url
	 * @param query
	 *            the query
	 * @return the POST response
	 * @throws ClientProtocolException
	 *             the client protocol exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private String getPOSTResponse(String url, String query) throws ClientProtocolException, IOException {
		HttpPost request = new HttpPost(url);
		StringEntity params = new StringEntity(query);
		params.setContentType("application/json");
		request.setEntity(params);
		HttpResponse httpResponse = httpClient.execute(request);
		BufferedReader br = new BufferedReader(new InputStreamReader((httpResponse.getEntity().getContent())));
		String response = br.readLine();
		return response;
	}
	
	public String deleteRequest(String indexName, String type, String id) {
		String response = "";
		Long executionTime = 0L;
		try {
			Long start = System.currentTimeMillis();
			Random random = new Random();
			int n = random.nextInt(hostList.size());
			String url = "http://" + hostList.get(n) + ":" + port + "/" + indexName + "/" + type + "/" + id;
			response = getDeleteResponse(url);
			Long end = System.currentTimeMillis();
			executionTime = end - start;
			log.info("DELETE request [URL=" + url + "] Execution time (ms): " + executionTime);
		} catch (Exception e) {
			throw new DBAnalyticsException("Error occured executing GET request." + e.getMessage(), e);
		}
		return response;
	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		ESExecutorService esExecutorService = new ESExecutorService();
		String query = "{\"query\":{\"match\":{\"host\":\"15\"}}}";
		System.out.println(esExecutorService.getRequest("user_profile_daywise", "realtime",
				"54e0a20b-5865-8679-9e95-a3e32b7bca7c_2017_02_20"));
	}

	

}
