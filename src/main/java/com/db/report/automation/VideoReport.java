package com.db.report.automation;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.model.UserHistory;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DBConfig;
import com.db.common.utils.IndexUtils;
import com.google.common.collect.Multimap;

/**
 * 
 * @author Hanish Bansal
 *
 */
public class VideoReport {

	private Client client = null;

	/** The Constant CLUSTER_NAME. */
	private static final String CLUSTER_NAME = "cluster.name";

	/** The settings. */
	private Settings settings = null;

	/** The host map. */
	private Multimap<String, Integer> hostMap = null;

	private static Logger log = LogManager.getLogger(VideoReport.class);

	private DBConfig config = DBConfig.getInstance();

	private static String lOutputFileName = "F:\\Projects\\VideoTracking\\VideoTrackingTest1.xls";
	
	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();
/*
	public void writeExcel(String lOutputFileName, String sheetName, List<Long> aOutputLines) {

		File lFile = new File(lOutputFileName);

		FileInputStream lFin = null;
		HSSFWorkbook lWorkBook = null;
		HSSFSheet  lWorkSheet = null;
		FileOutputStream lFout = null;
		POIFSFileSystem lPOIfs = null;
		if (lFile.exists()) {

			try {
				lFout = new FileOutputStream(lFile, true);

				lFin = new FileInputStream(lOutputFileName);
				lPOIfs = new POIFSFileSystem(lFin);

				lWorkBook = new HSSFWorkbook(lPOIfs);
//				try{
//				lWorkSheet = lWorkBook.createSheet(sheetName);
//				}catch(Exception e){e.printStackTrace();}
				lWorkSheet = lWorkBook.getSheet(sheetName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// Create new file
			try {
				lFout = new FileOutputStream(lOutputFileName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			lWorkBook = new HSSFWorkbook();
			lWorkSheet = lWorkBook.createSheet(sheetName);
		}
		try {
			int columnIndex=0;
			HSSFRow  lRow = lWorkSheet.createRow(lWorkSheet.getLastRowNum() + 1);
			System.out.println("lWorkSheet.getFirstCellNum() " + lRow.getFirstCellNum());
			System.out.println("lWorkSheet.getFirstCellNum() " + lRow.getLastCellNum());
			for (Long lValue : aOutputLines) {
					Cell lCell = lRow.createCell(columnIndex++);
					lWorkSheet.autoSizeColumn((short) columnIndex);
					lCell.setCellValue(String.valueOf(lValue));
					System.out.println(String.valueOf(lValue));
				
					
			}
			lWorkBook.write(lFout);
		//	lFout.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				lFout.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
*/
	public VideoReport(){
		initializeClient();
	}
	
	
	private void initializeClient() throws NoNodeAvailableException, ClusterBlockException {
		if (this.client != null) {
			client.close();
		}
		this.client = elasticSearchIndexService.getClient();
	}

	/**
	 * Return history(records of list of unique story ids visited of a user
	 * 
	 * @param sessionId
	 * @return
	 */
	public Map<String, UserHistory> getVideoReport() {
		// sessionId=
		// "26ba8fba-a893-c8b4-a3b8-e047f58c07db";
		// "ebe38fba-66e7-318d-5dee-a4fd7df77e97";
		String previousDate = 
				"2016-09-15";
		 //DateUtil.getPreviousDate().replaceAll("_", "-");
		Map<String, UserHistory> records = new LinkedHashMap<String, UserHistory>();

		BoolQueryBuilder boolQueryBuilderWeb = new BoolQueryBuilder();
		boolQueryBuilderWeb.must(QueryBuilders.matchQuery("channel", 521))
				.must(QueryBuilders.rangeQuery("datetime").from(previousDate).to(previousDate))
				.mustNot(QueryBuilders.matchQuery("cat_id", 8))
				.must(QueryBuilders.matchQuery("domaintype", "w"));
		SearchResponse srWeb = client.prepareSearch("video_tracking_2016_09").setTypes("realtime")
				.setTimeout(new TimeValue(2000)).setQuery(boolQueryBuilderWeb).setSize(0)
				.addAggregation(AggregationBuilders.terms("QuartileWise").field("quartile")
						
								.subAggregation(AggregationBuilders.cardinality("Unique").field("session_id")))
				.execute().actionGet();

		Terms QuartileWiseResult = srWeb.getAggregations().get("QuartileWise");
		Map<Integer, Long> webUsers = new TreeMap<Integer, Long>();
		Map<Integer, Long> mobileUsers = new TreeMap<Integer, Long>();
		for (Terms.Bucket entry : QuartileWiseResult.getBuckets()) {
			String quartile = entry.getKeyAsString();
			Cardinality cardinality  = entry.getAggregations().get("Unique");
			long usersCount = cardinality.getValue();
			webUsers.put(Integer.valueOf(quartile), usersCount);

		}
		
		BoolQueryBuilder boolQueryBuilderMobile = new BoolQueryBuilder();
		boolQueryBuilderMobile.must(QueryBuilders.matchQuery("channel", 521))
				.must(QueryBuilders.rangeQuery("datetime").from(previousDate).to(previousDate))
				.mustNot(QueryBuilders.matchQuery("cat_id", 8))
				.must(QueryBuilders.matchQuery("domaintype", "m"));
		SearchResponse srMobile = client.prepareSearch("video_tracking_2016_09").setTypes("realtime")
				.setTimeout(new TimeValue(2000)).setQuery(boolQueryBuilderMobile).setSize(0)
				.addAggregation(AggregationBuilders.terms("QuartileWise").field("quartile").size(5)
						
								.subAggregation(AggregationBuilders.cardinality("Unique").field("session_id")))
				.execute().actionGet();

		Terms QuartileWiseResultMobile = srMobile.getAggregations().get("QuartileWise");
	
		for (Terms.Bucket entry : QuartileWiseResultMobile.getBuckets()) {
			String quartile = entry.getKeyAsString();
			Cardinality cardinality  = entry.getAggregations().get("Unique");
			long usersCount = cardinality.getValue();
			mobileUsers.put(Integer.valueOf(quartile), usersCount);

		}
		
		System.out.println("webUsers: "+webUsers);
		System.out.println("mobileUsers: "+mobileUsers);
		System.out.println(previousDate+", WEB: "+webUsers.values());
		System.out.println(previousDate+", Mobile: "+mobileUsers.values());
		System.out.print("WEB: ");
		for (Long videoId : webUsers.values()) {
			System.out.print(videoId+"\t");
		}
		
		System.out.print("\n Mobile: ");
		for (Long videoId : mobileUsers.values()) {
			System.out.print(videoId+"\t");
		}
//		List<String> aOutputLines = new ArrayList<String>();
//		aOutputLines.add("Good Bye1");
//		aOutputLines.add("Job1");
//		aOutputLines.add("finish1");
//		List<Long> webUsersValue=new ArrayList<Long>(webUsers.values());
//		List<Long> mobileUsersValue=new ArrayList<Long>(mobileUsers.values());
//		writeExcel(lOutputFileName, "Mobile", mobileUsersValue);
//		writeExcel(lOutputFileName, "Web", webUsersValue);
		return records;
	}
	
	public Map<String, UserHistory> getRecoReport(String startDate, String endDate) throws ParseException {
		// sessionId=
		// "26ba8fba-a893-c8b4-a3b8-e047f58c07db";
		// "ebe38fba-66e7-318d-5dee-a4fd7df77e97";
		// DateUtil.getPreviousDate().replaceAll("_", "-");
		Map<String, Object> record = new HashMap<String, Object>();
		List<String> trackers = 
				Arrays.asList("news-rlreco");
				
//				Arrays.asList("news-ubreco","news-rlreco","news-keyreco","news-ureco","news-fsreco",
//				"news-preco","news-rec","news-fpreco","news-ltrec");
	
	BoolQueryBuilder boolQueryBuilderTracker=new BoolQueryBuilder();
	for (String tracker : trackers) {
		boolQueryBuilderTracker.should(QueryBuilders.prefixQuery(Constants.TRACKER, tracker));
	}
			
		BoolQueryBuilder boolQueryBuilderWeb = new BoolQueryBuilder();
		boolQueryBuilderWeb
		.must(boolQueryBuilderTracker)
		.must(QueryBuilders.matchQuery(Constants.HOST, "26"));
		String []indexes=IndexUtils.getIndexes(startDate, endDate);
		for (String indexName : indexes) {
			try{
			SearchResponse srWeb = client.prepareSearch(indexName)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryBuilderWeb).setSize(0)
//					.addAggregation(AggregationBuilders.terms("trackerWise").field(Constants.TRACKER))
					.execute().actionGet();
			String date=indexName.substring(indexName.indexOf("_")+1);
			//System.out.println(srWeb);
			long totalClicks=srWeb.getHits().getTotalHits();
			System.out.println(totalClicks);
			} catch(Exception e){
				log.error(e);
				System.out.println(" ");
			}
			//date+"\t"+
		}
		
	
//		System.out.println(hoursData);
        return null;
	}

	/**
	 * @param args
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ElasticsearchException
	 * @throws NumberFormatException
	 */
	public static void main(String[] args) throws Exception {

		VideoReport videoReport = new VideoReport();
		videoReport.getRecoReport("2017-05-01","2017-05-13");
		
//		List<Long> webUsersValue=new ArrayList<Long>();
//		
//		webUsersValue.add(111111111L);
//		webUsersValue.add(12121212L);
//		videoReport.writeExcel(lOutputFileName, "Web", webUsersValue);
	}

}
