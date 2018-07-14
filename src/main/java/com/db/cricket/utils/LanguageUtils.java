package com.db.cricket.utils;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.db.common.constants.Constants.PredictNWinConstants;
import com.db.common.utils.DBConfig;
import com.db.cricket.model.Scorecard;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

public class LanguageUtils {

	private static final Logger LOG = LogManager.getLogger(LanguageUtils.class);

	private static final Gson gson = new GsonBuilder().create();

	Map<String, Map<String, Object>> languageMap = new HashMap<>();

	private static LanguageUtils instance = null;

	private LanguageUtils() {

		String languageFilePath = DBConfig.getInstance().getString("language.file.path");
		try {
			if(StringUtils.isNotBlank(languageFilePath)) {
				JsonReader reader = new JsonReader(new FileReader(new File(languageFilePath)));
				languageMap = gson.fromJson(reader, Map.class);
			}else {
				LOG.info("Cricket Guru: No language file specified.");
			}

		} catch (Exception e) {
			LOG.error("Failed to populate language data", e);
		}
	}

	/**
	 * Create an instance of language utils.
	 * 
	 * @return
	 */
	public static LanguageUtils getInstance() {
		if (instance == null) {
			synchronized (LanguageUtils.class) {
				LanguageUtils.instance = new LanguageUtils();
			}
		}
		return instance;
	}

	/**
	 * Check and place language based names in the scorecard.
	 * 
	 * @param scorecard
	 * @param lang
	 */
	@SuppressWarnings("unchecked")
	public void scoreCardInLanguage(Scorecard scorecard, String lang) {
		// if ("eng".equalsIgnoreCase(lang) || "en".equalsIgnoreCase(lang)) {
		// return;
		// }

		if (languageMap.containsKey(scorecard.getTeamHome())) {
			scorecard.setTeamHomeName((String) languageMap.get(scorecard.getTeamHome()).get(lang));
		}

		if (languageMap.containsKey(scorecard.getTeamAway())) {
			scorecard.setTeamAwayName((String) languageMap.get(scorecard.getTeamAway()).get(lang));
		}

		if (languageMap.containsKey(scorecard.getCurrentBattingTeam())) {
			scorecard.setCurrentBattingTeamName((String) languageMap.get(scorecard.getCurrentBattingTeam()).get(lang));
		}

		if (languageMap.containsKey(scorecard.getWinningTeam())) {
			scorecard.setWinningTeamName((String) languageMap.get(scorecard.getWinningTeam()).get(lang));
		}

		Map<String, Object> teams = (Map<String, Object>) scorecard.getTeams();

		Set<String> teamKeySet = teams.keySet();

		for (String key : teamKeySet) {
			Map<String, Object> team = (Map<String, Object>) teams.get(key);

			if (languageMap.containsKey(key)) {
				team.put("Name_Full", languageMap.get(key).get(lang));
			}

			Map<String, Object> players = (Map<String, Object>) team.get("Players");

			for (String pKey : players.keySet()) {
				Map<String, Object> player = (Map<String, Object>) players.get(pKey);
				if (languageMap.containsKey(pKey)) {
					player.put("Name_Full", languageMap.get(pKey).get(lang));
				}
			}
		}
	}

	/**
	 * For every bid, put corresponding name in given language.
	 * 
	 * @param transactions
	 * @param lang
	 */
	public void bidsInLanguage(List<Map<String, Object>> transactions, String lang) {

		for (Map<String, Object> transaction : transactions) {
			String bidTypeId = (String) transaction.get("bidTypeId");

			if (languageMap.containsKey(bidTypeId)) {
				transaction.put(PredictNWinConstants.NAME, languageMap.get(bidTypeId).get(lang));
			} else if (transaction.containsKey(PredictNWinConstants.NAME)) {
				continue;
			} else {
				transaction.put(PredictNWinConstants.NAME, "");
			}
		}
	}

	public String namesInLanguage(String id, String lang) {
		if (id == null || id.isEmpty()) {
			LOG.warn("Received null Id");
			return null;
		}
		// throw new DBAnalyticsException("id cannot be null");
		Map<String, Object> values = languageMap.get(id);
		if (values == null)
			return null;
		String value = (String) values.get(lang);
		return value;
	}
}
