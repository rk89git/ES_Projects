package com.db.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.db.common.constants.Constants;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;

public class CricketGlobalVariableUtils {

	private static Client client = ElasticSearchIndexService.getInstance().getClient();
	private static Map<String, Map<String, Object>> CRICKET_TEAMS_NAMES = new HashMap<String, Map<String, Object>>();
	private static List<Integer> APP_NOTIFICATION_VENDOR_LIST = new ArrayList<>();
	private static List<Integer> WEB_NOTIFICATION_VENDOR_LIST = new ArrayList<>();
	private static Logger LOG = LogManager.getLogger(CricketGlobalVariableUtils.class);
	private static boolean isLoaded = false;
	
	static{
		load();
		isLoaded = true;
	}
	
	public static void load(){
		if(!isLoaded){
			loadAppNotificationVendorsList();
			loadCricketTeamNames();
		}
	}
	
	private static void loadCricketTeamNames(){
		try{
			QueryBuilder qb = QueryBuilders.matchAllQuery();

			SearchResponse ser = client.prepareSearch(Indexes.CRICKET_TEAM_NAMES).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb).execute()
					.actionGet();

			SearchHit[] searchHits = ser.getHits().getHits();

			for (SearchHit hit : searchHits) {
				CRICKET_TEAMS_NAMES.put(hit.getId(), hit.getSource());
			}
		}catch(Exception e){
			LOG.error("Error while loading CricketTeamNames", e);
		}
	}
	
	private static void loadAppNotificationVendorsList(){
		try{
			APP_NOTIFICATION_VENDOR_LIST = Arrays.asList(Constants.Cricket.PredictWinConstants.APP_VENDORS_LIST);
		}catch(Exception e){
			LOG.error("Error while loading AppNotificationVendorsList", e);
		}
	}
	
	public static Map<String, Map<String, Object>> getCricketTeamsNames(){
		return CRICKET_TEAMS_NAMES;
	}
	
	public static List<Integer> getAppNotificationVendorsList(){
		return APP_NOTIFICATION_VENDOR_LIST;
	}
	
	public static List<Integer> getWebNotificationVendorsList(){
		return WEB_NOTIFICATION_VENDOR_LIST;
	}
}
