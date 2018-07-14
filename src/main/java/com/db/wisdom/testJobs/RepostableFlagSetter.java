package com.db.wisdom.testJobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;

public class RepostableFlagSetter {


	private Client client = null;

	private Connection connection;

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private static Logger log = LogManager.getLogger(ReProcess.class);

	int batchSize = 5000;
	static String ip = "104.199.156.183";   //global
	//static String ip="10.140.0.7";          //private

	public RepostableFlagSetter() {		
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
			BoolQueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termsQuery(Constants.CHANNEL_SLNO,Arrays.asList(channel_slno.split(","))))
					.mustNot(QueryBuilders.termQuery(Constants.BHASKARSTORYID, 0));
			
			BoolQueryBuilder fqb = qb.filter(QueryBuilders.existsQuery(Constants.BHASKARSTORYID));

			SearchResponse resp = client.prepareSearch(sourceIndex).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(fqb)
					.setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			System.out.println("Hit size to process : " + resp.getHits().getTotalHits());
			for (SearchHit hit : resp.getHits().getHits()) {
				try {
					Map<String, Object> record = hit.getSource();
					String bhaskarStoryid = hit.getSource().get(Constants.BHASKARSTORYID).toString();
					int repostable = getRepostableFlag(bhaskarStoryid);
					System.out.println(bhaskarStoryid+"....................."+repostable);
					
				    record.put(Constants.IS_REPOSTABLE, repostable);
					
					record.put(Constants.ROWID, hit.getId());

					listWeightage.add(record);

					if (listWeightage.size() == batchSize) {
						elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
						System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
								+ resp.getHits().getTotalHits() + "; Records Processed: ");
						listWeightage.clear();

					}
				} catch (Exception e) {
					log.error("Error getting record from source index " + sourceIndex);
					e.printStackTrace();
					continue;
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
	
	public void calculateFields(String sourceIndex, String targetIndex) {
		long startTime = System.currentTimeMillis();
		try {
			int numRec = 5000;
			BoolQueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(Constants.IS_REPOSTABLE, 2));
			SearchResponse resp = client.prepareSearch(sourceIndex).setTypes(MappingTypes.MAPPING_REALTIME)
					.setQuery(qb)
					.setSize(numRec).execute().actionGet();
			List<Map<String, Object>> listWeightage = new ArrayList<Map<String, Object>>();
			System.out.println("Hit size to process : " + resp.getHits().getTotalHits());
			for (SearchHit hit : resp.getHits().getHits()) {
				try {
					Map<String, Object> record = hit.getSource();
					Double field1 = 0.0;
		            Double field2 = 0.0;
		            Double field3 = 0.0;
		            Double link_clicks = 1.0;
		            Double unique_reach = 1.0;
		            Double total_reach = 1.0;
		            Double reactions = 1.0;
		            Double reports = 1.0;
		            Double shares = 1.0;
		           
		            if(record.get(Constants.LINK_CLICKS)!=null){
		            	link_clicks += ((Integer)record.get(Constants.LINK_CLICKS)).doubleValue();
		            }
		            if(record.get(Constants.UNIQUE_REACH)!=null){
		            	unique_reach += ((Integer)record.get(Constants.UNIQUE_REACH)).doubleValue();
		            }
		            if(record.get(Constants.TOTAL_REACH)!=null){
		            	total_reach += ((Integer)record.get(Constants.TOTAL_REACH)).doubleValue();
		            }
		            if(record.get(Constants.SHARES)!=null){
		            	shares += ((Integer)record.get(Constants.SHARES)).doubleValue();
		            }
		            if(record.get(Constants.REACTION_ANGRY)!=null){
		            	reactions += ((Integer)record.get(Constants.REACTION_ANGRY)).doubleValue();
		            }
		            if(record.get(Constants.REACTION_HAHA)!=null){
		            	reactions += ((Integer)record.get(Constants.REACTION_HAHA)).doubleValue();
		            }
		            if(record.get(Constants.REACTION_LOVE)!=null){
		            	reactions += ((Integer)record.get(Constants.REACTION_LOVE)).doubleValue();
		            }
		           if(record.get(Constants.REACTION_SAD)!=null){
		        	   reactions += ((Integer)record.get(Constants.REACTION_SAD)).doubleValue();
		            }
		           if(record.get(Constants.REACTION_THANKFUL)!=null){
		        	   reactions += ((Integer)record.get(Constants.REACTION_THANKFUL)).doubleValue();
		            }
		           if(record.get(Constants.REACTION_WOW)!=null){
		        	   reactions += ((Integer)record.get(Constants.REACTION_WOW)).doubleValue();
		            }
		           if(record.get(Constants.HIDE_CLICKS)!=null){
		        	   reports += ((Integer)record.get(Constants.HIDE_CLICKS)).doubleValue();
		            }
		           if(record.get(Constants.HIDE_ALL_CLICKS)!=null){
		        	   reports += ((Integer)record.get(Constants.HIDE_ALL_CLICKS)).doubleValue();
		            }
		           if(record.get(Constants.REPORT_SPAM_CLICKS)!=null){
		        	   reports += ((Integer)record.get(Constants.REPORT_SPAM_CLICKS)).doubleValue();
		            }
		            
		            if(link_clicks!=0){
		            	field1 = unique_reach/link_clicks;
		            }
		            
		            if(total_reach !=0){
		            	field2 = (total_reach-unique_reach)/total_reach;
		            }
		            
		            if(reports!=0){
		            	field3 = (reactions*shares)/reports;
		            }
		            
		            record.put(Constants.FIELD1, field1);
		            record.put(Constants.FIELD2, field2);
		            record.put(Constants.FIELD3, field3);				
					record.put(Constants.ROWID, hit.getId());

					listWeightage.add(record);

					if (listWeightage.size() == batchSize) {
						elasticSearchIndexService.index(targetIndex, MappingTypes.MAPPING_REALTIME, listWeightage);
						System.out.println(DateUtil.getCurrentDateTime() + " Total records: "
								+ resp.getHits().getTotalHits() + "; Records Processed: ");
						listWeightage.clear();

					}
				} catch (Exception e) {
					log.error("Error getting record from source index " + sourceIndex);
					e.printStackTrace();
					continue;
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


	private int getRepostableFlag(String storyid) {

		int res = 0;
		if(connection==null)
			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");

		//String query = "SELECT cat_name FROM DNA_Websites where cat_id=?";
		String query = "SELECT is_repostable FROM table_spl_catid WHERE bh_storyid = ?";
		PreparedStatement st = null;
		try {
			st = connection.prepareStatement(query);
			st.setString(1, storyid);
			ResultSet resultSet = st.executeQuery();

			while (resultSet.next()) {

				res = resultSet.getInt(Constants.IS_REPOSTABLE);
			}

		} catch (SQLException e) {

			getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");
			e.printStackTrace();
		}
		return res;

	}

	public static void main(String[] args) {
		RepostableFlagSetter obj = new RepostableFlagSetter();
		//obj.getMySSQLCOnnection(ip, "3306", "web2", "web2", "realtime");		
		//duvi.reindex(args[0], args[1]);
		System.out.println("===============================Starting reindex========================================");
		//System.out.println(obj.getRepostableFlag("119745779"));
		//obj.calculateFields(args[0], args[1],args[2]);
		obj.calculateFields("fb_dashboard","fb_dashboard");
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

}
