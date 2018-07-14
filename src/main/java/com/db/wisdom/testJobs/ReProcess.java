package com.db.wisdom.testJobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class ReProcess {

	private Client client = null;

	private Connection connection;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ReProcess.class);

	int batchSize = 5000;
	//static String ip = "104.199.156.183";   //global
	static String ip="10.140.0.7";          //private

	public ReProcess() {
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

	public void reindex(String sourceIndex, String targetIndex,String channel_slno) {
		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;

			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc", SortOrder.ASC)
					.setQuery(QueryBuilders.termQuery(Constants.CHANNEL_SLNO,channel_slno))
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						Map<String, Object> record = hit.getSource();
						if (hit.getSource().get("cat_id") != null) {
							if (hit.getSource().get("cat_name") == null|| hit.getSource().get("cat_name").toString().isEmpty()) {

								String cat_name = getCatIdName(hit.getSource().get("cat_id").toString());
								record.put("cat_name", cat_name);
							}
							Map<String, String> idNameMap = getPcatIdName(hit.getSource().get("cat_id").toString());
							if (idNameMap.get("parent_name") == null) {
								System.out.println(hit.getId()+" no parent found" + hit.getSource().get("cat_id").toString());
							}
							if (idNameMap.get("ga_cat_name") == null) {
								System.out.println(hit.getId()+" no ga_cat_name found" + hit.getSource().get("cat_id").toString());
							}

							record.put("pp_cat_id", idNameMap.get("parent_id"));
							record.put("pp_cat_name", idNameMap.get("parent_name"));
							record.put("ga_cat_name", idNameMap.get("ga_cat_name"));

						}

						if (record.get("pp_cat_id") == null) {
							record.put("pp_cat_id", record.get("cat_id"));
						}

						if (record.get("pp_cat_name") == null) {
							record.put("pp_cat_name", record.get("cat_name"));
						}							

						if (record.get("ga_cat_name") == null) {
                            record.put("ga_cat_name", String.valueOf("Others"));
						}
						
						if(record.get(Constants.URL)!=null&&record.get(Constants.URL).toString().contains("dbvideos")){
							record.put("ga_cat_name","DB Videos");
						}
						
						record.put(Constants.ROWID, hit.getId());

						listWeightage.add(record);

						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().getTotalHits() + "; Records Processed: ");
							listWeightage.clear();

						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						e.printStackTrace();
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			// Index remaining data if there after completing loop
			if (listWeightage.size() > 0) {
				elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: ");
				listWeightage.clear();

			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public void reindexDbVideos(String sourceIndex, String targetIndex) {
		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;

			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setQuery(QueryBuilders.prefixQuery(Constants.TITLE, "Video"))
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						Map<String, Object> record = hit.getSource();

						record.put("ga_cat_name", "DB Videos");
						record.put(Constants.ROWID, hit.getId());

						listWeightage.add(record);

						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().getTotalHits() + "; Records Processed: ");
							listWeightage.clear();

						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						e.printStackTrace();
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			// Index remaining data if there after completing loop
			if (listWeightage.size() > 0) {
				elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: ");
				listWeightage.clear();

			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public void reindexDayHyper(String sourceIndex, String targetIndex,String channel_slno) {
		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;
			List<String> dayHyperPrefixList = new ArrayList<>();
			dayHyperPrefixList.add("c-m-10-");
			dayHyperPrefixList.add("c-m-16-");
			dayHyperPrefixList.add("c-m-17-");
			dayHyperPrefixList.add("c-m-181-");
			dayHyperPrefixList.add("c-m-20-");
			dayHyperPrefixList.add("c-m-268-");
			dayHyperPrefixList.add("c-m-6-");
			dayHyperPrefixList.add("c-m-271-");
			dayHyperPrefixList.add("c-m-3-");
			dayHyperPrefixList.add("c-m-58-");
			dayHyperPrefixList.add("c-m-8-");
			dayHyperPrefixList.add("c-m-85-");
			dayHyperPrefixList.add("c-m-99-");
			dayHyperPrefixList.add("c-m-299-");
			dayHyperPrefixList.add("C-m-110-");
			dayHyperPrefixList.add("c-10-");
			dayHyperPrefixList.add("c-16-");
			dayHyperPrefixList.add("c-17-");
			dayHyperPrefixList.add("c-181-");
			dayHyperPrefixList.add("c-20-");
			dayHyperPrefixList.add("c-268-");
			dayHyperPrefixList.add("c-6-");
			dayHyperPrefixList.add("c-271-");
			dayHyperPrefixList.add("c-3-");
			dayHyperPrefixList.add("c-58-");
			dayHyperPrefixList.add("c-8-");
			dayHyperPrefixList.add("c-85-");
			dayHyperPrefixList.add("c-99-");
			dayHyperPrefixList.add("c-299-");
			dayHyperPrefixList.add("C-110-");
			BoolQueryBuilder prefixBoolQuery = new BoolQueryBuilder();
			for (String prefix : dayHyperPrefixList) {
				prefixBoolQuery.should(QueryBuilders.prefixQuery(Constants.FILENAME, prefix));
			}
			prefixBoolQuery.must(QueryBuilders.termQuery(Constants.CHANNEL_SLNO, channel_slno));
			SearchResponse scrollResp = client.prepareSearch(sourceIndex)
					//.setSearchType(SearchType.SCAN)
					.addSort("_doc",SortOrder.ASC)
					.setQuery(prefixBoolQuery)
					.setScroll(new TimeValue(60000)).setSize(numRec).execute().actionGet();
			
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			AtomicInteger dayWiseThreadAtomicInt = new AtomicInteger(1);
			while (true) {
				System.out.println("Hit size to process : " + scrollResp.getHits().getHits().length);
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					try {
						Map<String, Object> record = hit.getSource();
						record.put("ga_cat_name", "Day Hyper");
						record.put(Constants.ROWID, hit.getId());

						listWeightage.add(record);

						if (listWeightage.size() == batchSize) {
							elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
							System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
									+ scrollResp.getHits().getTotalHits() + "; Records Processed: ");
							listWeightage.clear();

						}
					} catch (Exception e) {
						log.error("Error getting record from source index " + sourceIndex);
						e.printStackTrace();
						continue;
					}
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
						.execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			// Index remaining data if there after completing loop
			if (listWeightage.size() > 0) {
				elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
				System.out.println(DateUtil.getCurrentDateTime() + "Records Processed: ");
				listWeightage.clear();

			}
			// End of unique users calculation
			System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) / (1000 * 60));
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public static void main(String[] args) {
		ReProcess duvi = new ReProcess();
		duvi.getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");		
		//duvi.reindex(args[0], args[1]);
		if(args[2].equals("521")){
			System.out.println("===============================Starting day hyper========================================");		
			duvi.reindexDayHyper(args[0], args[1],args[2]);
			}
		System.out.println("===============================Starting reindex========================================");
		duvi.reindex(args[0], args[1],args[2]);
		
		
	}

	void getMySSQLCOnnection(String ip, String port, String user, String pass, String database) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			log.error(e);
		}
		Connection con = null;
		try {
			con = DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + database, user, pass);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("connection created");
		connection = con;
	}

	Map<String, String> getPcatIdName(String catId) {
		
		if(connection==null)
			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");

		Map<String, String> idNameMap = new HashMap<>();

		String query = "select parent_id,parent_name,ga_cat_name from parent_cat_details where find_in_set(?,child_ids)";
		PreparedStatement st = null;
		try {
			st = connection.prepareStatement(query);
			st.setString(1, catId);
			ResultSet resultSet = st.executeQuery();

			while (resultSet.next()) {
				idNameMap.put("parent_id", resultSet.getString("parent_id"));
				idNameMap.put("parent_name", resultSet.getString("parent_name"));
				idNameMap.put("ga_cat_name", resultSet.getString("ga_cat_name"));

			}

            if (idNameMap.get("parent_name") == null) {


                String query1 = "select parent_id,parent_name,ga_cat_name from parent_cat_details where parent_id=?";

                st = connection.prepareStatement(query1);
                st.setString(1, catId);
                resultSet = st.executeQuery();

                while (resultSet.next()) {
                    idNameMap.put("parent_id", resultSet.getString("parent_id"));
                    idNameMap.put("parent_name", resultSet.getString("parent_name"));
                    idNameMap.put("ga_cat_name", resultSet.getString("ga_cat_name"));

                }
            }

		} catch (SQLException e) {

			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");

			idNameMap = getPcatIdName(catId);
			e.printStackTrace();
		}
		return idNameMap;

	}


	String getCatIdName(String catId) {

		String res = null;
		if(connection==null)
			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");
		
		String query = "SELECT cat_name FROM DNA_Websites where cat_id=?";
		PreparedStatement st = null;
		try {
			st = connection.prepareStatement(query);
			st.setString(1, catId);
			ResultSet resultSet = st.executeQuery();

			while (resultSet.next()) {

				res = resultSet.getString("cat_name");
			}

		} catch (SQLException e) {

			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");

			res = getCatIdName(catId);
			e.printStackTrace();
		}
		return res;

	}

}