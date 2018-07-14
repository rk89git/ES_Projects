package com.db.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.db.common.exception.DBAnalyticsException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * The Class EsTransportClient.
 */
public class EsTransportClient {

	/** The Constant CLUSTER_NAME. */
	private static final String CLUSTER_NAME = "cluster.name";
	
	/** The config. */
	private DBConfig config;
	
	/** The client. */
	private TransportClient client;
	
	/** The settings. */
	private Settings settings;

	/** The host map. */
	private Multimap<String, Integer> hostMap;
	
	private static EsTransportClient esTransportClient=null;
	
	/**
	 * Gets the comma separated hosts.
	 *
	 * @return the comma separated hosts
	 */
	private String getCommaSeparatedHosts() {
		return StringUtils.join(this.hostMap.keySet(), ",");
	}
	
	public static EsTransportClient getInstance() {
		if (esTransportClient == null) {
			esTransportClient = new EsTransportClient();
		}
		return esTransportClient;
	}
	
	/**
	 * Inits the host map.
	 */
	private void initHostMap() {
		String eSNodes = config.getProperty("index.elasticsearch.connect");
		hostMap = HashMultimap.create();
		for (Object node : eSNodes.split(",")) {
			if ("".equals(node)) {
				throw new DBAnalyticsException("Invalid index.elasticsearch.connect property in config file");
			}
			String hostName = ((String) node).split(":")[0];
			int port = Integer.parseInt(((String) node).split(":")[1]);
			hostMap.put(hostName, port);
		}
	}
	
	/**
	 * Inits the settings.
	 */
	private void initSettings() {
		String hosts = this.getCommaSeparatedHosts();
		this.settings = Settings.builder()
				.put(CLUSTER_NAME, config.getString("index.elasticsearch.cluster.name"))
				.put("http.enabled", "false").put("transport.tcp.port", "9300-9400")
				.put("discovery.zen.ping.unicast.hosts", hosts).build();
	}
	
	/**
	 * Instantiates a new es transport client.
	 */
	private EsTransportClient() {
		try {
			this.config = DBConfig.getInstance();
			this.initHostMap();
			this.initSettings();
			this.initClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		} catch (UnknownHostException e) {
			throw new DBAnalyticsException(e);
		}
	}
	
	/**
	 * Inits the client.
	 *
	 * @throws NoNodeAvailableException the no node available exception
	 * @throws ClusterBlockException the cluster block exception
	 * @throws UnknownHostException the unknown host exception
	 */
	private void initClient() throws NoNodeAvailableException, ClusterBlockException, UnknownHostException {
		if (this.client != null) {
			client.close();
		}
		this.client = new PreBuiltTransportClient(settings);
		for (String hostName : hostMap.keySet()) {
			for (Integer port : hostMap.get(hostName)) {
				((TransportClient) this.client).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
			}
		}
	}

	/**
	 * Gets the config.
	 *
	 * @return the config
	 */
	public DBConfig getConfig() {
		return config;
	}

	/**
	 * Sets the config.
	 *
	 * @param config the config to set
	 */
	public void setConfig(DBConfig config) {
		this.config = config;
	}

	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public TransportClient getClient() {
		return client;
	}

	/**
	 * Sets the client.
	 *
	 * @param client the client to set
	 */
	public void setClient(TransportClient client) {
		this.client = client;
	}

	/**
	 * Gets the settings.
	 *
	 * @return the settings
	 */
	public Settings getSettings() {
		return settings;
	}

	/**
	 * Sets the settings.
	 *
	 * @param settings the settings to set
	 */
	public void setSettings(Settings settings) {
		this.settings = settings;
	}

	/**
	 * Gets the host map.
	 *
	 * @return the hostMap
	 */
	public Multimap<String, Integer> getHostMap() {
		return hostMap;
	}

	/**
	 * Sets the host map.
	 *
	 * @param hostMap the hostMap to set
	 */
	public void setHostMap(Multimap<String, Integer> hostMap) {
		this.hostMap = hostMap;
	}
}
