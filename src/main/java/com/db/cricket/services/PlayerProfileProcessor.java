package com.db.cricket.services;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import com.db.common.constants.Constants;
import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.PlayerProfile;
import com.db.common.constants.Indexes;
import com.db.common.constants.MappingTypes;
import com.db.common.services.ElasticSearchIndexService;
import com.db.common.utils.DateUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

/**
 * PlayerProfileProcessor deals with processing of a players profile record. The
 * processing goes through a number of methods due to the inherent structure of
 * players profile json.
 * 
 * @author Piyush Gupta
 */
@Service
public class PlayerProfileProcessor {

	private static final Logger LOG = LogManager.getLogger(PlayerProfileProcessor.class);

	private Map<String, Object> playerProfile = new HashMap<>();

	private ElasticSearchIndexService elasticSearchIndexService = ElasticSearchIndexService.getInstance();

	/**
	 * Parse the profile bio from the profile json
	 * 
	 * @param profileMap
	 *            the map as generated from the profile json.
	 */
	private void processProfileBio(Map<?, ?> profileMap) {
		long startTime = System.currentTimeMillis();
		LOG.info("Begginning to process player profile Bio...");

		/*
		 * No additional parsing required here
		 */
		playerProfile.put(PlayerProfile.PROFILE, (Map<?, ?>) profileMap.get(PlayerProfile.PROFILE));
		playerProfile.put(Constants.ROWID,
				((Map<?, ?>) ((Map<?, ?>) profileMap.get(PlayerProfile.PROFILE)).get(PlayerProfile.BIO))
						.get(PlayerProfile.PLAYER_ID));

		LOG.info("Time to process player profile bio: " + (System.currentTimeMillis() - startTime) + " ms.");
	}

	/**
	 * Process the last 5 match performance element.
	 * 
	 * @param profileMap
	 *            map of maps containing the last 5 performances data format wise.
	 */
	private void processLast5MatchPerformance(Map<?, ?> profileMap) {
		long startTime = System.currentTimeMillis();
		LOG.info("Begginning to process players performance records for last 5 matches...");

		Map<?, ?> formatWisePerformance = (Map<?, ?>) profileMap.get(PlayerProfile.LAST_5);

		Map<String, Object> formatWiseProcessedPerformanceMap = new HashMap<>();

		/*
		 * Convert the map of maps into a formatwise map of list, with each list
		 * containing the last five performances in order provided by the vendor.
		 */
		for (Entry<?, ?> format : formatWisePerformance.entrySet()) {
			List<Map<?, ?>> performanceEntryList = new ArrayList<>();
			Map<?, ?> valueMap = (Map<?, ?>) ((Map<?, ?>) format.getValue()).get(PlayerProfile.PERFORMANCE);
			for (Entry<?, ?> value : valueMap.entrySet()) {
				performanceEntryList.add((Map<?, ?>) value.getValue());
			}

			formatWiseProcessedPerformanceMap.put((String) format.getKey(), performanceEntryList);
		}

		playerProfile.put(PlayerProfile.LAST_5, formatWiseProcessedPerformanceMap);

		LOG.info("Time to process player performance data for last 5 matches: "
				+ (System.currentTimeMillis() - startTime) + " ms.");
	}

	/**
	 * Parse the captaincy performance records for ingestion.
	 * 
	 * @param captaincyMap
	 */
	@SuppressWarnings({ "unchecked" })
	private void processCaptaincyRecords(Map<?, ?> captaincyMap) {
		Map<?, ?> formatWiseCaptaincyRecords = (Map<?, ?>) captaincyMap.get(PlayerProfile.CAPTAINCY);

		List<String> fields = (List<String>) formatWiseCaptaincyRecords.remove(PlayerProfile.DESCRIPTION);

		Map<String, Object> processedCaptaincyRecord = new HashMap<>();

		/*
		 * Convert the list of values, given the description element, into a format wise
		 * mapping from description to value.
		 */
		for (Entry<?, ?> format : formatWiseCaptaincyRecords.entrySet()) {
			List<?> formatValues = (List<?>) format.getValue();

			Map<String, Object> captaincyRecord = parseRecordListToMap(fields, formatValues);

			for (int i = 0; i < formatValues.size(); i++) {
				captaincyRecord.put(fields.get(i), formatValues.get(i));
			}

			processedCaptaincyRecord.put((String) format.getKey(), captaincyRecord);
		}

		playerProfile.put(PlayerProfile.CAPTAINCY, processedCaptaincyRecord);
	}

	/**
	 * Process records for bowl_performance, bat_performance and mom fields
	 * 
	 * @param parentRecordMap
	 *            map for the given field name
	 * @param key
	 *            field name
	 */
	private void processRecordsForParentKey(Map<?, ?> parentRecordMap, String key) {
		long startTime = System.currentTimeMillis();
		LOG.info("Begginning to process records for profile element: " + key);

		Map<?, ?> formatWiseMomRecords = (Map<?, ?>) parentRecordMap.get(key);

		Map<String, Object> formatWiseProcessedMomRecords = new HashMap<>();

		/*
		 * Convert the map of maps into a formatwise map of list, with each list
		 * containing the last five performances in order provided by the vendor.
		 */
		for (Entry<?, ?> format : formatWiseMomRecords.entrySet()) {
			List<Map<?, ?>> momEntryList = new ArrayList<>();
			Map<?, ?> valueMap = (Map<?, ?>) format.getValue();
			for (Entry<?, ?> value : valueMap.entrySet()) {
				momEntryList.add((Map<?, ?>) value.getValue());
			}

			formatWiseProcessedMomRecords.put((String) format.getKey(), momEntryList);
		}

		playerProfile.put(key, formatWiseProcessedMomRecords);

		LOG.info("Time to process profile element: " + key + " " + (System.currentTimeMillis() - startTime) + " ms.");
	}

	/**
	 * Method to process overall stats of a player.
	 * 
	 * @param overallStatsMap
	 *            map containing overall stats for the player.
	 */
	@SuppressWarnings({ "unchecked" })
	private void processOverAllStats(Map<?, ?> overallStatsMap) {
		long startTime = System.currentTimeMillis();
		LOG.info("Beginning to process record for overall stats...");

		Map<String, Object> processedOverallStats = new HashMap<>();

		Map<?, ?> overall = (Map<?, ?>) overallStatsMap.get(PlayerProfile.OVERALL);

		for (Entry<?, ?> stat : overall.entrySet()) {

			Map<?, ?> formatWiseStats = (Map<?, ?>) stat.getValue();

			Map<String, Object> processedStats = new HashMap<>();

			List<String> fields = (List<String>) formatWiseStats.remove(PlayerProfile.DESCRIPTION);
			for (Entry<?, ?> format : formatWiseStats.entrySet()) {
				Map<String, Object> formatWiseProcessedResult = parseRecordListToMap(fields,
						(List<?>) format.getValue());
				processedStats.put((String) format.getKey(), formatWiseProcessedResult);
			}

			processedOverallStats.put((String) stat.getKey(), processedStats);
		}

		playerProfile.put(PlayerProfile.OVERALL, processedOverallStats);

		LOG.info("Time taken to process overall stats: " + (System.currentTimeMillis() - startTime));
	}

	/**
	 * Method to process stats specified in year, where and against fields.
	 * 
	 * @param statsMap
	 * @param key
	 */
	@SuppressWarnings({ "unchecked" })
	private void processStatsForKey(Map<?, ?> statsMap, String key) {

		long startTime = System.currentTimeMillis();
		LOG.info("Processing stats for field: " + key);
		Map<?, ?> statsForKey = (Map<?, ?>) statsMap.get(key);

		Map<String, Object> processedStats = new HashMap<>();
		List<Map<String, Object>> formatStatsList = new ArrayList<>();

		for (Entry<?, ?> format : statsForKey.entrySet()) {
			Map<String, Object> processedFormatStats = new HashMap<>();

			for (Entry<?, ?> statType : ((Map<?, ?>) format.getValue()).entrySet()) {
				Map<?, ?> records = (Map<?, ?>) statType.getValue();
				List<String> fields = (List<String>) records.remove(PlayerProfile.DESCRIPTION);

				List<Map<String, Object>> parsedRecords = new ArrayList<>();
				for (Entry<?, ?> record : records.entrySet()) {
					Map<String, Object> parsedRecord = parseRecordListToMap(fields, (List<?>) record.getValue());
					parsedRecord.put(key, (String) record.getKey());
					parsedRecords.add(parsedRecord);
				}

				processedFormatStats.put((String) statType.getKey(), parsedRecords);
			}
			processedFormatStats.put(PlayerProfile.SERIES_TYPE, format.getKey());
			formatStatsList.add(processedFormatStats);
			processedStats.put((String) format.getKey(), processedFormatStats);
		}

		playerProfile.put(key, formatStatsList);
		LOG.info("Time taken to process field: " + key + "; " + (System.currentTimeMillis() - startTime) + " ms.");
	}

	/**
	 * Utility method to convert a list of values, given a list describing each
	 * value, into a map.
	 * 
	 * @param description
	 *            The list of description keys.
	 * @param recordValues
	 *            list of values.
	 * @return
	 */
	private Map<String, Object> parseRecordListToMap(List<String> description, List<?> recordValues) {
		Map<String, Object> result = new HashMap<>();

		for (int i = 0; i < recordValues.size(); i++) {
			result.put(description.get(i), recordValues.get(i));
		}

		return result;
	}

	/**
	 * Converts the array of records in player profile, key year, into a list of
	 * individual records and inserts into elasticsearch.
	 * 
	 * Can Later be extended for location stats (under 'where' key) and opposition
	 * stats (under 'against' key).
	 */
	@SuppressWarnings({ "unchecked" })
	private void ingestPlayerStatistics() {

		List<Map<String, Object>> playerStatRecords = new ArrayList<>();

		try {
			List<Map<?, ?>> yearlyStatistics = (List<Map<?, ?>>) playerProfile.get(PlayerProfile.YEAR);

			Map<?, ?> playerProfileBio = (Map<?, ?>) ((Map<String, Object>) playerProfile.get(PlayerProfile.PROFILE))
					.get(PlayerProfile.BIO);

			String playerId = (String) playerProfileBio.get(PlayerProfile.PLAYER_ID);
			String playerName = (String) playerProfileBio.get(PlayerProfile.PLAYER_NAME);
			String playerFullName = (String) playerProfileBio.get(PlayerProfile.PLAYER_NAME_FULL);

			for (Map<?, ?> formatData : yearlyStatistics) {
				String format = (String) formatData.get(PlayerProfile.SERIES_TYPE);

				for (Map<?, ?> record : (List<Map<?, ?>>) formatData.get(PlayerProfile.BOWLING)) {
					Map<String, Object> stats = new HashMap<>((Map<String, Object>) record);
					stats.put(Constants.ROWID,
							playerId + "_" + format + "_year_" + record.get(PlayerProfile.YEAR) + "_Bowling");
					stats.put(PlayerProfile.STAT_TYPE, PlayerProfile.BOWLING);
					stats.put(PlayerProfile.SERIES_TYPE, format);
					stats.put(PlayerProfile.PLAYER_NAME_FULL, playerFullName);
					stats.put(PlayerProfile.PLAYER_NAME, playerName);
					stats.put(PlayerProfile.PLAYER_ID, playerId);
					stats.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					playerStatRecords.add(stats);
				}

				for (Map<?, ?> record : (List<Map<?, ?>>) formatData.get(PlayerProfile.BATTING_AND_FIELDING)) {
					Map<String, Object> stats = new HashMap<>((Map<String, Object>) record);
					stats.put(Constants.ROWID,
							playerId + "_" + format + "_year_" + record.get(PlayerProfile.YEAR) + "_Batting_Fielding");
					stats.put(PlayerProfile.STAT_TYPE, PlayerProfile.BATTING_AND_FIELDING);
					stats.put(PlayerProfile.SERIES_TYPE, format);
					stats.put(PlayerProfile.PLAYER_NAME_FULL, playerFullName);
					stats.put(PlayerProfile.PLAYER_NAME, playerName);
					stats.put(PlayerProfile.PLAYER_ID, playerId);
					stats.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());
					playerStatRecords.add(stats);
				}
			}

			if (!playerStatRecords.isEmpty()) {
				elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_PLAYER_STATISTICS,
						MappingTypes.MAPPING_REALTIME, playerStatRecords);
			}

		} catch (Exception e) {
			LOG.error("Failed to write records for yearly player stats: ", e);
		}
	}

	/**
	 * Central entry point for parsing and ingesting a player profile.
	 * 
	 * @param profileRecord
	 */
	public synchronized void processPlayerProfile(Map<String, Object> profileRecord) {
		long startTime = System.currentTimeMillis();

		try {
			/*
			 * Wildcard cast as no insertion will be done in this data.
			 */
			List<?> dataList = (List<?>) profileRecord.get(CricketConstants.DATA);

			processProfileBio((Map<?, ?>) dataList.get(0));
			processOverAllStats((Map<?, ?>) dataList.get(1));
			processStatsForKey((Map<?, ?>) dataList.get(2), PlayerProfile.AGAINST);
			processStatsForKey((Map<?, ?>) dataList.get(3), PlayerProfile.YEAR);
			processStatsForKey((Map<?, ?>) dataList.get(4), PlayerProfile.WHERE);
			processRecordsForParentKey((Map<?, ?>) dataList.get(5), PlayerProfile.BAT_PERFORMANCE);
			processRecordsForParentKey((Map<?, ?>) dataList.get(6), PlayerProfile.BOWL_PERFORMANCE);
			processRecordsForParentKey((Map<?, ?>) dataList.get(7), PlayerProfile.MOM);
			processLast5MatchPerformance((Map<?, ?>) dataList.get(8));
			processCaptaincyRecords((Map<?, ?>) dataList.get(9));
			playerProfile.put(Constants.DATE_TIME_FIELD, DateUtil.getCurrentDateTime());

			ingestPlayerStatistics();

			elasticSearchIndexService.indexOrUpdate(Indexes.CRICKET_PLAYER_PROFILE, MappingTypes.MAPPING_REALTIME,
					Collections.singletonList(playerProfile));

		} catch (Exception e) {
			LOG.error("Unable to process player profile: ", e);
		} finally {
			LOG.info("Total time taken to process player profile: " + (System.currentTimeMillis() - startTime)
					+ " ms. \nFinal Record for Ingestion: " + playerProfile);
			/*
			 * So that the map stays empty afterwards, as it is being resused for all
			 * profiles.
			 */
			playerProfile.clear();
		}
	}

	/**
	 * Main method to test development and changes.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Gson json = new GsonBuilder().create();

		final String testFilePath = args[0];

		Map<String, Object> profileData = json.fromJson(new JsonReader(new FileReader(new File(testFilePath))),
				Map.class);

		new PlayerProfileProcessor().processPlayerProfile(profileData);
	}
}
