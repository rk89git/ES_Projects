package com.db.common.services;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.db.common.constants.Constants;
import com.db.common.constants.MappingTypes;
import com.db.common.exception.DBAnalyticsException;
import com.db.common.utils.IndexUtils;

public class FPaidFBoSurveyService {

	private Client client = null;

	private static Logger log = LogManager.getLogger(FPaidFBoSurveyService.class);

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	private int numThreads = 5;

	private String startDate = null;

	private String endDate = null;

	private String[] listIndex = null;

	private List<String> fpaidTracker = Arrays.asList("news-fpaid", "news-bgp", "news-Vpaid", "news-njp", "news-nmp",
			"news-fpamn", "news-fpaamn", "news-NJP", "news-njnm", "news-NJS", "news-njb", "news-fpamn1", "news-njp1",
			"news-opd", "news-gpd", "news-ipt", "news-aff", "news-ptil", "news-icub", "news-gpromo", "news-cmp",
			"news-fpamn6", "news-GR", "news-abc", "news-xyz");

	public FPaidFBoSurveyService(String startDate, String endDate, String threadCount) {
		try {
			this.startDate = startDate;
			this.endDate = endDate;
			this.numThreads = Integer.valueOf(threadCount);
			this.client = elasticSearchIndexService.getClient();
		} catch (RuntimeException e) {
			throw new DBAnalyticsException(e);
		}
	}

	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			// Get dates between dates
			listIndex = IndexUtils.getIndexes(startDate, endDate);

			if (listIndex.length > 30) {
				throw new RuntimeException("Date  range cannot be more than 30 days");
			}

			// Case 1: Total hits count from tracker fpaid and fbo.
			BoolQueryBuilder boolQueryUserFpaid = new BoolQueryBuilder();
			boolQueryUserFpaid.must(QueryBuilders.termsQuery(Constants.TRACKER, fpaidTracker))
					.must(QueryBuilders.matchQuery(Constants.HOST, 15));

			SearchResponse srTotalHitsFPaid = client.prepareSearch(listIndex)
					.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryUserFpaid).setSize(1)
					.execute().actionGet();

			BoolQueryBuilder boolQueryUserFbo = new BoolQueryBuilder();
			boolQueryUserFbo.must(QueryBuilders.prefixQuery(Constants.TRACKER, "news-fbo"))
					.must(QueryBuilders.matchQuery(Constants.HOST, 15));
			/*
			 * SearchResponse srTotalHitsFBO = client.prepareSearch(indexDates)
			 * .setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).
			 * setQuery(boolQueryUserFbo).setSize(1) .execute().actionGet();
			 * long totalFboHits = srTotalHitsFBO.getHits().getTotalHits();
			 * System.out.println("Total FBO Hits: " + totalFboHits);
			 */

			long totalFPaidHits = srTotalHitsFPaid.getHits().getTotalHits();

			System.out.println("Total FPaid Hits: " + totalFPaidHits);

			// Logic to derive unique users
			Set<String> session_id_set = new HashSet<String>();
			BoolQueryBuilder boolQueryFPaidFBO = new BoolQueryBuilder();
			boolQueryFPaidFBO
					.must(new BoolQueryBuilder().should(QueryBuilders.prefixQuery(Constants.TRACKER, "news-fpaid"))
							.should(QueryBuilders.prefixQuery(Constants.TRACKER, "news-fbo")));
			boolQueryFPaidFBO.must(QueryBuilders.matchQuery(Constants.HOST, 15));

			for (String indexName : listIndex) {
				System.out.println("Searching  in index of date " + indexName);

				SearchResponse scrollResp = client.prepareSearch(indexName)
						.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
						// "realtime_2015_07_10"
						//.setSearchType(SearchType.SCAN)
						.addSort("_doc",SortOrder.ASC)
						.setScroll(new TimeValue(60000)).setQuery(boolQueryUserFpaid)
						.setSize(1000).execute().actionGet(); // 1000 hits per
																// shard will be
																// returned for
																// each
																// scroll
				long totalHits = scrollResp.getHits().getTotalHits();
				long processed = 0;
				// Scroll until no hits are returned
				while (true) {

					SearchHit searchHits[] = scrollResp.getHits().getHits();
					for (SearchHit hit : searchHits) {
						String session_id = hit.getSource().get("session_id").toString();
						session_id_set.add(session_id);

					}
					processed = processed + searchHits.length;
					System.out.println(
							"Index Name: " + indexName + "; Total Hits: " + totalHits + "; Processed: " + processed);
					scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
							.execute().actionGet();
					System.out.println("Number of unique users: " + session_id_set.size());
					// Break condition: No hits are returned
					if (scrollResp.getHits().getHits().length == 0) {
						break;
					}
				}
			}

			// End of unique users calculation

			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
			ArrayList<String> session_id_lst = new ArrayList<String>();
			session_id_lst.addAll(session_id_set);

			int lstSize = session_id_lst.size();
			int range_diff = (lstSize / numThreads);
			int start_range = 0;
			int end_range = range_diff;
			List<Future<Map<String, Long>>> futureList = new ArrayList<Future<Map<String, Long>>>();
			for (int i = 0; i < numThreads; i++) {
				System.out.println("Thread " + i);
				System.out.println("Start Range: " + start_range);
				System.out.println("End Range: " + end_range);
				Future<Map<String, Long>> future = executorService.submit(
						new SummaryDeriveRunnable(session_id_lst.subList(start_range, end_range - 1), listIndex, i));
				futureList.add(future);
				start_range = end_range;
				end_range = end_range + range_diff;

			}

			long onceFpaidUsers = 0;
			long twiceFpaidUsers = 0;
			long thriceFpaidUsers = 0;
			long fourthFpaidUsers = 0;
			long fifthFpaidUsers = 0;
			long six_tenFpaidUsers = 0;
			long eleven_twentyFpaidUsers = 0;
			long otherFpaidUsers = 0;

			long onceFboUsers = 0;
			long twiceFboUsers = 0;
			long thriceFboUsers = 0;
			long fourthFboUsers = 0;
			long fifthFboUsers = 0;
			long six_tenFboUsers = 0;
			long eleven_twentyFboUsers = 0;
			long otherFboUsers = 0;

			long convertedUsers = 0;
			long totalFpaidUsers = 0;

			// Variables to capture conversion rate details
			long oneIterationConverted = 0;
			long twoIterationConverted = 0;
			long threeIterationConverted = 0;
			long fourIterationConverted = 0;
			long fiveIterationConverted = 0;
			long six_ten_iterationConverted = 0;
			long eleven_twentyIterationConverted = 0;
			long otherIterationConverted = 0;

			for (Future<Map<String, Long>> future : futureList) {
				Map<String, Long> record = future.get();
				/*
				 * onceFpaidUsers = onceFpaidUsers +
				 * record.get("onceFpaidUsers"); twiceFpaidUsers =
				 * twiceFpaidUsers + record.get("twiceFpaidUsers");
				 * thriceFpaidUsers = thriceFpaidUsers +
				 * record.get("thriceFpaidUsers"); fourthFpaidUsers =
				 * fourthFpaidUsers + record.get("fourthFpaidUsers");
				 * fifthFpaidUsers = fifthFpaidUsers +
				 * record.get("fifthFpaidUsers"); six_tenFpaidUsers =
				 * six_tenFpaidUsers + record.get("six_tenFpaidUsers");
				 * eleven_twentyFpaidUsers = eleven_twentyFpaidUsers +
				 * record.get("eleven_twentyFpaidUsers"); otherFpaidUsers =
				 * otherFpaidUsers + record.get("otherFpaidUsers");
				 * 
				 * onceFboUsers = onceFboUsers + record.get("onceFboUsers");
				 * twiceFboUsers = twiceFboUsers + record.get("twiceFboUsers");
				 * thriceFboUsers = thriceFboUsers +
				 * record.get("thriceFboUsers"); fourthFboUsers = fourthFboUsers
				 * + record.get("fourthFboUsers"); fifthFboUsers = fifthFboUsers
				 * + record.get("fifthFboUsers"); six_tenFboUsers =
				 * six_tenFboUsers + record.get("six_tenFboUsers");
				 * eleven_twentyFboUsers = eleven_twentyFboUsers +
				 * record.get("eleven_twentyFboUsers"); otherFboUsers =
				 * otherFboUsers + record.get("otherFboUsers");
				 */
				// Conversion Analysis
				totalFpaidUsers = totalFpaidUsers + record.get("totalFpaidUsers");
				convertedUsers = convertedUsers + record.get("convertedUsers");

				oneIterationConverted = oneIterationConverted + record.get("oneIterationConverted");
				twoIterationConverted = twoIterationConverted + record.get("twoIterationConverted");
				threeIterationConverted = threeIterationConverted + record.get("threeIterationConverted");
				fourIterationConverted = fourIterationConverted + record.get("fourIterationConverted");
				fiveIterationConverted = fiveIterationConverted + record.get("fiveIterationConverted");
				six_ten_iterationConverted = six_ten_iterationConverted + record.get("six_ten_iterationConverted");
				eleven_twentyIterationConverted = eleven_twentyIterationConverted
						+ record.get("eleven_twentyIterationConverted");
				otherIterationConverted = otherIterationConverted + record.get("otherIterationConverted");

			}
			System.out.println("*****************************REPORT**************************************");

			System.out.println("Total FPaid Hits: " + totalFPaidHits);
			// .out.println("Total FBO Hits: " + totalFboHits);
			/*
			 * System.out.println("==============Fpaid====================");
			 * System.out.println("Count of users comes 1 time: " +
			 * onceFpaidUsers); System.out.println(
			 * "Count of users comes 2 time: " + twiceFpaidUsers);
			 * System.out.println("Count of users comes 3 time: " +
			 * thriceFpaidUsers); System.out.println(
			 * "Count of users comes 4 time: " + fourthFpaidUsers);
			 * System.out.println("Count of users comes 5 time: " +
			 * fifthFpaidUsers); System.out.println(
			 * "Count of users comes 6-10 time: " + six_tenFpaidUsers);
			 * System.out.println("Count of users comes 11-20: " +
			 * eleven_twentyFpaidUsers); System.out.println(
			 * "Count of users comes more than 20 time: " + otherFpaidUsers);
			 * 
			 * System.out.println("Count of users comes thrice: " +
			 * thriceFpaidUsers);
			 * 
			 * System.out.println("==============Fbo====================");
			 * System.out.println("Count of users comes 1 time: " +
			 * onceFboUsers); System.out.println("Count of users comes 2 time: "
			 * + twiceFboUsers); System.out.println(
			 * "Count of users comes 3 time: " + thriceFboUsers);
			 * System.out.println("Count of users comes 4 time: " +
			 * fourthFboUsers); System.out.println(
			 * "Count of users comes 5 time: " + fifthFboUsers);
			 * System.out.println("Count of users comes 6-10 time: " +
			 * six_tenFboUsers); System.out.println(
			 * "Count of users comes 11-20: " + eleven_twentyFboUsers);
			 * System.out.println("Count of users comes more than 20 time: " +
			 * otherFboUsers);
			 */
			System.out.println("==================Conversion Analysis===========================");

			// System.out.println("Total FPaid and Fbo users count: " +
			// lstSize);
			System.out.println("Total FPaid users count: " + totalFpaidUsers);
			System.out.println("COnverted (fpaid->fbo) users count: " + convertedUsers);
			System.out.println("Count of converted user after 1 iteration: " + oneIterationConverted);
			System.out.println("Count of converted user after 2 iteration: " + twoIterationConverted);
			System.out.println("Count of converted user after 3 iteration: " + threeIterationConverted);
			System.out.println("Count of converted user after 4 iteration: " + fourIterationConverted);
			System.out.println("Count of converted user after 5 iteration: " + fiveIterationConverted);
			System.out.println("Count of converted user after 6-10 iteration: " + six_ten_iterationConverted);
			System.out.println("Count of converted user after 11-20 iteration: " + eleven_twentyIterationConverted);
			System.out.println("Count of converted user after more than 20 iteration: " + otherIterationConverted);

			System.out.println("*****************************REPORT Ends**************************************");
			System.out.println(
					"Total execution time in Minutes: " + (System.currentTimeMillis() - startTime) / (60 * 1000.0));

			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		} catch (Exception e) {
			log.error(e);
			throw new DBAnalyticsException("Error getting user visits for last date." + e.getMessage());
		}

	}

	public class SummaryDeriveRunnable implements Callable<Map<String, Long>> {
		private final List<String> sessionIds;
		// private String date;
		private int threadIndex;
		private String indexDates[];
		int totalCount = 0;

		SummaryDeriveRunnable(List<String> sessionIds, String indexesForSummary[], int threadIndex) {
			this.sessionIds = sessionIds;
			this.threadIndex = threadIndex;
			this.indexDates = indexesForSummary;
			// this.date = date;
		}

		@Override
		public Map<String, Long> call() {
			System.out.println("Executing thread..");

			long onceFpaidUsers = 0;
			long twiceFpaidUsers = 0;
			long thriceFpaidUsers = 0;
			long fourthFpaidUsers = 0;
			long fifthFpaidUsers = 0;
			long six_tenFpaidUsers = 0;
			long eleven_twentyFpaidUsers = 0;
			long otherFpaidUsers = 0;

			long onceFboUsers = 0;
			long twiceFboUsers = 0;
			long thriceFboUsers = 0;
			long fourthFboUsers = 0;
			long fifthFboUsers = 0;
			long six_tenFboUsers = 0;
			long eleven_twentyFboUsers = 0;
			long otherFboUsers = 0;

			long totalUsers = sessionIds.size();
			long convertedUsers = 0;

			long countProcessed = 0;
			long totalFpaidUsers = 0;

			// Variables to capture conversion rate details
			long oneIterationConverted = 0;
			long twoIterationConverted = 0;
			long threeIterationConverted = 0;
			long fourIterationConverted = 0;
			long fiveIterationConverted = 0;
			long six_ten_iterationConverted = 0;
			long eleven_twentyIterationConverted = 0;
			long otherIterationConverted = 0;

			for (String session_id : sessionIds) {
				try {
					countProcessed++;

					BoolQueryBuilder boolQueryUserFpaid = new BoolQueryBuilder();
					boolQueryUserFpaid.must(QueryBuilders.matchQuery("session_id", session_id))
							.must(QueryBuilders.termsQuery(Constants.TRACKER, fpaidTracker))
							.must(QueryBuilders.matchQuery(Constants.HOST, 15));
					/*
					 * // FPAID USER FREQUENY SearchResponse sr1 =
					 * client.prepareSearch(indexDates)
					 * .setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY
					 * ).setQuery(boolQueryUserFpaid) //
					 * .addSort(Constants.DATE_TIME_FIELD, // SortOrder.ASC)
					 * .setSize(1).execute().actionGet(); if
					 * (sr1.getHits().getTotalHits() == 1) { onceFpaidUsers++; }
					 * else if (sr1.getHits().getTotalHits() == 2) {
					 * twiceFpaidUsers++; } else if
					 * (sr1.getHits().getTotalHits() == 3) { thriceFpaidUsers++;
					 * } else if (sr1.getHits().getTotalHits() == 4) {
					 * fourthFpaidUsers++; } else if
					 * (sr1.getHits().getTotalHits() == 5) { fifthFpaidUsers++;
					 * } else if (sr1.getHits().getTotalHits() >= 6 &&
					 * sr1.getHits().getTotalHits() <= 10) {
					 * six_tenFpaidUsers++; } else if
					 * (sr1.getHits().getTotalHits() >= 11 &&
					 * sr1.getHits().getTotalHits() <= 20) {
					 * eleven_twentyFpaidUsers++; } else if
					 * (sr1.getHits().getTotalHits() > 20) { otherFpaidUsers++;
					 * }
					 */
					BoolQueryBuilder boolQueryUserFbo = new BoolQueryBuilder();
					boolQueryUserFbo.must(QueryBuilders.matchQuery("session_id", session_id))
							.must(QueryBuilders.prefixQuery(Constants.TRACKER, "news-fbo"));
					boolQueryUserFbo.must(QueryBuilders.matchQuery(Constants.HOST, 15));

					/*
					 * // FBO USER FREQUENY SearchResponse sr2 =
					 * client.prepareSearch(indexDates)
					 * .setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY
					 * ).setQuery(boolQueryUserFbo)
					 * .setSize(1).execute().actionGet(); if
					 * (sr2.getHits().getTotalHits() == 1) { onceFboUsers++; }
					 * else if (sr2.getHits().getTotalHits() == 2) {
					 * twiceFboUsers++; } else if (sr2.getHits().getTotalHits()
					 * == 3) { thriceFboUsers++; } else if
					 * (sr2.getHits().getTotalHits() == 4) { fourthFboUsers++; }
					 * else if (sr2.getHits().getTotalHits() == 5) {
					 * fifthFboUsers++; } else if (sr2.getHits().getTotalHits()
					 * >= 6 && sr2.getHits().getTotalHits() <= 10) {
					 * six_tenFboUsers++; } else if
					 * (sr2.getHits().getTotalHits() >= 11 &&
					 * sr2.getHits().getTotalHits() <= 20) {
					 * eleven_twentyFboUsers++; } else if
					 * (sr2.getHits().getTotalHits() > 20) { otherFboUsers++; }
					 */
					// Case 3: Conversion of users from FPaid to FBO
					String fpaidIndex = null;
					for (String indexName : listIndex) {
						SearchResponse srSingleIndexFpaid = client.prepareSearch(indexName)
								.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY).setQuery(boolQueryUserFpaid)
								// .addSort(Constants.DATE_TIME_FIELD,
								// SortOrder.ASC)
								.setSize(1).execute().actionGet();
						if (srSingleIndexFpaid.getHits().getTotalHits() > 0) {
							fpaidIndex = srSingleIndexFpaid.getHits().getHits()[0].getIndex();
							break;
						}
					}

					if (fpaidIndex != null) {
						// Increase count of fpaid users whose first hit is
						// fpaid.
						totalFpaidUsers++;

						// String fpaidIndex =
						// sr1.getHits().getHits()[0].getIndex();
						String[] currIndex = fpaidIndex.split("_");
						String currIndexDate = currIndex[1] + "-" + currIndex[2] + "-" + currIndex[3];
						String fpaidIndexDate = new String(currIndexDate);
						Calendar calNext = Calendar.getInstance();
						SimpleDateFormat sdfNext = new SimpleDateFormat("yyyy-MM-dd");
						Date date;

						do {
							date = sdfNext.parse(currIndexDate);
							calNext.setTime(date);
							calNext.add(Calendar.DATE, 1);
							date = calNext.getTime();

							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							// System.out.println(dateFormat.format(dt));
							currIndexDate = dateFormat.format(date);
							SearchResponse sr3 = client.prepareSearch("realtime_" + currIndexDate.replace("-", "_"))
									.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
									.setQuery(boolQueryUserFbo).setSize(1).execute().actionGet();
							if (sr3.getHits().getHits().length > 0) {
								String fboIndex = sr3.getHits().getHits()[0].getIndex();
								String fboIndexDate = fboIndex.substring(fboIndex.indexOf("_") + 1).replaceAll("_",
										"-");
								String[] indexesForFpaid = IndexUtils.getIndexes(fpaidIndexDate, fboIndexDate);

								convertedUsers++;
								System.out.println("Thread " + threadIndex + ": Total user: " + totalUsers
										+ ", Processed: " + countProcessed + "; Converted: " + convertedUsers);
								SearchResponse searchResponseForFpaidIterations = client.prepareSearch(indexesForFpaid)
										.setTypes(MappingTypes.MAPPING_REALTIME_UNIQUE_USER_STORY)
										.setQuery(boolQueryUserFpaid).setSize(1).execute().actionGet();

								if (searchResponseForFpaidIterations.getHits().getTotalHits() == 1) {
									oneIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() == 2) {
									twoIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() == 3) {
									threeIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() == 4) {
									fourIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() == 5) {
									fiveIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() >= 6
										&& searchResponseForFpaidIterations.getHits().getTotalHits() <= 10) {
									six_ten_iterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() >= 11
										&& searchResponseForFpaidIterations.getHits().getTotalHits() <= 20) {
									eleven_twentyIterationConverted++;
								} else if (searchResponseForFpaidIterations.getHits().getTotalHits() > 20) {
									otherIterationConverted++;
								}
								break;
							}
						} while (date.compareTo(sdfNext.parse(endDate)) < 0);
					}

				} catch (Exception e) {
					System.err.println("Skipping User : " + session_id + "; Reason: " + e.getMessage());
					continue;
				}
			}

			Map<String, Long> threadResult = new HashMap<String, Long>();
			/*
			 * threadResult.put("onceFpaidUsers", onceFpaidUsers);
			 * threadResult.put("twiceFpaidUsers", twiceFpaidUsers);
			 * threadResult.put("thriceFpaidUsers", thriceFpaidUsers);
			 * threadResult.put("fourthFpaidUsers", fourthFpaidUsers);
			 * threadResult.put("fifthFpaidUsers", fifthFpaidUsers);
			 * threadResult.put("six_tenFpaidUsers", six_tenFpaidUsers);
			 * threadResult.put("eleven_twentyFpaidUsers",
			 * eleven_twentyFpaidUsers); threadResult.put("otherFpaidUsers",
			 * otherFpaidUsers);
			 * 
			 * threadResult.put("onceFboUsers", onceFboUsers);
			 * threadResult.put("twiceFboUsers", twiceFboUsers);
			 * threadResult.put("thriceFboUsers", thriceFboUsers);
			 * threadResult.put("fourthFboUsers", fourthFboUsers);
			 * threadResult.put("fifthFboUsers", fifthFboUsers);
			 * threadResult.put("six_tenFboUsers", six_tenFboUsers);
			 * threadResult.put("eleven_twentyFboUsers", eleven_twentyFboUsers);
			 * threadResult.put("otherFboUsers", otherFboUsers);
			 */
			threadResult.put("totalFpaidUsers", totalFpaidUsers);
			threadResult.put("convertedUsers", convertedUsers);
			threadResult.put("oneIterationConverted", oneIterationConverted);
			threadResult.put("twoIterationConverted", twoIterationConverted);
			threadResult.put("threeIterationConverted", threeIterationConverted);
			threadResult.put("fourIterationConverted", fourIterationConverted);
			threadResult.put("fiveIterationConverted", fiveIterationConverted);
			threadResult.put("six_ten_iterationConverted", six_ten_iterationConverted);
			threadResult.put("eleven_twentyIterationConverted", eleven_twentyIterationConverted);
			threadResult.put("otherIterationConverted", otherIterationConverted);

			System.out.println("--------------THREAD " + threadIndex + " result: " + threadResult);
			return threadResult;
		}
	}

	public static void main(String[] args) {
		FPaidFBoSurveyService duvi = new FPaidFBoSurveyService(args[0], args[1], args[2]);
		duvi.run();
	}

}