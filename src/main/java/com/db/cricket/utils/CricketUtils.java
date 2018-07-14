package com.db.cricket.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.db.common.constants.Constants.CricketConstants;
import com.db.common.constants.Constants.CricketConstants.IngestionConstants;
import com.db.common.constants.Constants.Host;
import com.db.common.model.HTTPModel;
import com.db.common.utils.HTTPUtils;
import com.db.cricket.model.MatchWinnerDetailPostRequest;
import com.db.cricket.model.NotificationIdResponse;
import com.db.cricket.model.PredictAndWinPostResponse;
import com.db.cricket.model.TicketIdAndGainPointResponse;
import com.db.cricket.model.TicketIdAndGainPointResponseList;
import com.db.cricket.predictnwin.constants.PredictAndWinConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CricketUtils {

	private static final Logger LOG = LogManager.getLogger(CricketUtils.class);

	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	@SuppressWarnings("unchecked")
	public static Map<String, Object> modifyMap(Map<String, Object> originalMap) {

		Map<String, Object> modifiedMap = new HashMap<>();

		HashMap<String, Object> teamsMap = (HashMap<String, Object>) originalMap.get("Teams");

		HashMap<String, Object> matchDetailsMap = (HashMap<String, Object>) originalMap.get("Matchdetail");

		HashMap<String, Object> matchInfoMap = new HashMap<>();

		if (matchDetailsMap != null) {
			for (Map.Entry<String, Object> entry : ((HashMap<String, Object>) matchDetailsMap.get("Match"))
					.entrySet()) {
				matchInfoMap.put(entry.getKey(), entry.getValue());
			}
			if (matchDetailsMap.containsKey("Series")) {
				matchInfoMap.put("Series_Name", ((Map<?, ?>) matchDetailsMap.get("Series")).get("Name"));
				matchInfoMap.put("Tour_Name", ((Map<?, ?>) matchDetailsMap.get("Series")).get("Tour_Name"));
				matchInfoMap.put("Series_Id", ((Map<?, ?>) matchDetailsMap.get("Series")).get("Id"));
			}

			if (matchDetailsMap.containsKey("Prematch")) {
				matchInfoMap.put("Prematch", matchDetailsMap.get("Prematch"));
			}

			if (matchDetailsMap.containsKey("Venue")) {
				matchInfoMap.put("Venue", ((HashMap<String, Object>) matchDetailsMap.get("Venue")).get("Name"));
				matchInfoMap.put("Venue_Id", ((HashMap<String, Object>) matchDetailsMap.get("Venue")).get("Id"));
			}
			if (matchDetailsMap.containsKey("Officials")) {
				matchInfoMap.put("Umpires",
						((HashMap<String, Object>) matchDetailsMap.get("Officials")).get("Umpires"));
				matchInfoMap.put("Referee",
						((HashMap<String, Object>) matchDetailsMap.get("Officials")).get("Referee"));
			}
			matchInfoMap.put("Weather", matchDetailsMap.get("Weather"));
			if (matchDetailsMap.containsKey("Tosswonby")) {
				matchInfoMap.put("Tosswonby",
						((HashMap<String, Object>) teamsMap.get(matchDetailsMap.get("Tosswonby"))).get("Name_Full"));
			}

			if (matchDetailsMap.containsKey("Player_Match")) {
				modifiedMap.put("Player_Match", (String) matchDetailsMap.get("Player_Match"));
			}

			if (matchDetailsMap.containsKey("Result")) {
				modifiedMap.put("Result", (String) matchDetailsMap.get("Result"));
			} else
				modifiedMap.put("Result", "");

			if (matchDetailsMap.containsKey("Winmargin")
					&& StringUtils.isNotBlank((String) matchDetailsMap.get("Winmargin"))) {
				modifiedMap.put("Winmargin", (String) matchDetailsMap.get("Winmargin"));
			}

			if (matchDetailsMap.containsKey("Winningteam")
					&& StringUtils.isNotBlank((String) matchDetailsMap.get("Winningteam"))) {
				modifiedMap.put("Winningteam", (String) matchDetailsMap.get("Winningteam"));
				modifiedMap.put("Winningteam_Name",
						((HashMap<String, Object>) teamsMap.get(matchDetailsMap.get("Winningteam"))).get("Name_Full"));
			}
			matchInfoMap.put("Status", matchDetailsMap.get("Status"));
			matchInfoMap.put("Day", matchDetailsMap.get("Day"));
			matchInfoMap.put("Session", matchDetailsMap.get("Session"));
			matchInfoMap.put("Equation", matchDetailsMap.get("Equation"));
			modifiedMap.put("Team_Home", matchDetailsMap.get("Team_Home"));
			modifiedMap.put("Team_Away", matchDetailsMap.get("Team_Away"));
		}
		String home_team_name = (String) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap.get("Teams"))
				.get(((HashMap<String, Object>) originalMap.get("Matchdetail")).get("Team_Home"))).get("Name_Full");

		modifiedMap.put("Team_Home_Short_Name",
				((HashMap<String, Object>) ((HashMap<String, Object>) originalMap.get("Teams"))
						.get(((HashMap<String, Object>) originalMap.get("Matchdetail")).get("Team_Home")))
								.get("Name_Short"));
		modifiedMap.put("Team_Home_Name", home_team_name);

		modifiedMap.put("Team_Away_Name",
				((HashMap<String, Object>) ((HashMap<String, Object>) originalMap.get("Teams"))
						.get(((HashMap<String, Object>) originalMap.get("Matchdetail")).get("Team_Away")))
								.get("Name_Full"));
		modifiedMap.put("Team_Away_Short_Name",
				((HashMap<String, Object>) ((HashMap<String, Object>) originalMap.get("Teams"))
						.get(((HashMap<String, Object>) originalMap.get("Matchdetail")).get("Team_Away")))
								.get("Name_Short"));
		modifiedMap.put("Matchdetail", matchInfoMap);

		List<HashMap<String, Object>> inningsList = (List<HashMap<String, Object>>) originalMap.get("Innings");
		if (inningsList != null) {
			if (inningsList.size() > 0) {
				String currentBattingTeam = (String) inningsList.get(inningsList.size() - 1).get("Battingteam");
				String current_batting_team_name = (String) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
						.get("Teams")).get(currentBattingTeam)).get("Name_Full");
				modifiedMap.put("current_batting_team_name", current_batting_team_name);

				String current_batting_team_name_short = (String) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
						.get("Teams")).get(currentBattingTeam)).get("Name_Short");

				modifiedMap.put("current_batting_team_name_short", current_batting_team_name_short);
				modifiedMap.put("current_batting_team", currentBattingTeam);
			}

			for (HashMap<String, Object> inning : inningsList) {
				String battingTeam = (String) inning.get("Battingteam");

				if (battingTeam != null) {
					String batting_team_name = (String) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
							.get("Teams")).get(battingTeam)).get("Name_Full");
					String batting_team_short_name = (String) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
							.get("Teams")).get(battingTeam)).get("Name_Short");
					inning.put("Team_Name", batting_team_name);
					inning.put("Team_Short_Name", batting_team_short_name);
				}

				String bowlingTeam = null;

				if (modifiedMap.get("Team_Home") != null && modifiedMap.get("Team_Away") != null) {
					if (modifiedMap.get("Team_Home").equals(battingTeam)) {
						bowlingTeam = (String) modifiedMap.get("Team_Away");
					} else
						bowlingTeam = (String) modifiedMap.get("Team_Home");
				}
				if (inning.get("Batsmen") != null) {
					for (HashMap<String, Object> batsman : (List<HashMap<String, Object>>) inning.get("Batsmen")) {
						String player_id = (String) batsman.get("Batsman");
						HashMap<String, Object> playersMap = (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
								.get("Teams")).get(battingTeam)).get("Players");

						if (playersMap.get(player_id) != null) {
							batsman.put("Batsman_Name",
									((HashMap<String, Object>) playersMap.get(player_id)).get("Name_Full"));
							batsman.put("Stats", ((HashMap<String, Object>) playersMap.get(player_id)).get("Batting"));
						}
						String bowler_id = (String) batsman.get("Bowler");

						String fielder_id = (String) batsman.get("Fielder");
						if (StringUtils.isNotBlank(bowler_id))
							batsman.put("Bowler_Name",
									((HashMap<String, Object>) ((HashMap<String, Object>) (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
											.get("Teams")).get(bowlingTeam)).get("Players")).get(bowler_id))
													.get("Name_Full"));

						if (StringUtils.isNotBlank(fielder_id)) {

							if (!fielder_id.equals("sub")) {
								Map<String, Object> playersObj = (HashMap<String, Object>) (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
										.get("Teams")).get(bowlingTeam)).get("Players");

								String fielderName = "";
								if (playersObj.containsKey(fielder_id)) {
									fielderName = (String) ((HashMap<String, Object>) (playersObj).get(fielder_id))
											.get("Name_Full");

								}
								batsman.put("Fielder_Name", fielderName);
							} else
								batsman.put("Field_Name", "sub");
						}
					}
				}
				if (inning.get("Bowlers") != null) {
					for (HashMap<String, Object> bowler : (List<HashMap<String, Object>>) inning.get("Bowlers")) {
						String player_id = (String) bowler.get("Bowler");
						HashMap<String, Object> playersMap = (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
								.get("Teams")).get(bowlingTeam)).get("Players");
						if (playersMap.get(player_id) != null) {
							bowler.put("Bowler_Name",
									((HashMap<String, Object>) playersMap.get(player_id)).get("Name_Full"));
							bowler.put("Stats", ((HashMap<String, Object>) playersMap.get(player_id)).get("Bowling"));
						}
					}
				}
				if (inning.containsKey("Partnership_Current")) {
					Map<String, Object> Partnership_Current_map = ((HashMap<String, Object>) inning
							.get("Partnership_Current"));
					if (Partnership_Current_map.containsKey("Batsmen")) {
						for (HashMap<String, Object> batsman : ((List<HashMap<String, Object>>) (Partnership_Current_map)
								.get("Batsmen"))) {

							String player_id = (String) batsman.get("Batsman");
							HashMap<String, Object> playersMap = (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
									.get("Teams")).get(battingTeam)).get("Players");

							if (playersMap.get(player_id) != null) {
								batsman.put("Batsman_Name",
										((HashMap<String, Object>) playersMap.get(player_id)).get("Name_Full"));
							}
						}
					}
				}

				if (inning.containsKey("FallofWickets")) {
					List<HashMap<String, Object>> fallofWickets = ((List<HashMap<String, Object>>) inning
							.get("FallofWickets"));
					for (HashMap<String, Object> batsman : fallofWickets) {

						String player_id = (String) batsman.get("Batsman");

						HashMap<String, Object> playersMap = (HashMap<String, Object>) ((HashMap<String, Object>) ((HashMap<String, Object>) originalMap
								.get("Teams")).get(battingTeam)).get("Players");

						if (playersMap.get(player_id) != null) {
							batsman.put("Batsman_Name",
									((HashMap<String, Object>) playersMap.get(player_id)).get("Name_Full"));
						}
					}
				}
			}
		}

		modifiedMap.put("Innings", inningsList);

		modifiedMap.put("Nuggets", originalMap.get("Nuggets"));

		modifiedMap.put("Teams", gson.toJson(originalMap.get("Teams")));

		List<HashMap<String, Object>> notesList = new ArrayList<>();
		if (originalMap.containsKey("Notes")) {
			for (Map.Entry<String, Object> entry : ((HashMap<String, Object>) originalMap.get("Notes")).entrySet()) {
				HashMap<String, Object> notesMap = new HashMap<>();
				notesMap.put("inningsNumber", entry.getKey());
				notesMap.put("notes", entry.getValue());
				notesList.add(notesMap);
			}
			modifiedMap.put("Notes", notesList);
		}

		return modifiedMap;
	}

	public static String AddOrdinal(String snNum) {
		int num = 0;
		try {
			num = Integer.parseInt(snNum);
		} catch (NumberFormatException nfe) {
			LOG.error(nfe);
		}

		if (num <= 0)
			return snNum;

		switch (num % 100) {
		case 11:
		case 12:
		case 13:
			return num + "th";
		}

		switch (num % 10) {
		case 1:
			return num + "st";
		case 2:
			return num + "nd";
		case 3:
			return num + "rd";
		default:
			return num + "th";
		}
	}

	public static String getNotificationId(Map<String, Object> match) {

		String serviceUrl = "http://wisdom0.bhaskar.com/inHouseNotification/inhouse_notification_dynamic.php";

		Map<String, Object> serviceDataMap = new HashMap<>();
		serviceDataMap.put("editorName", CricketConstants.CRICKET_NOTIFICATION_EDITOR);
		serviceDataMap.put("nicon", CricketConstants.COMMENT_ICON_URL);
		serviceDataMap.put("ntitle", match.get(IngestionConstants.MATCH_NUMBER) + ", "
				+ match.get(IngestionConstants.SERIES_SHORT_DISPLAY_NAME));

		Map<String, Object> options = new HashMap<>();
		options.put("storyid", (String) match.get(IngestionConstants.MATCH_ID_FIELD));
		options.put("time_to_live", 180);

		serviceDataMap.put("option", options);

		HTTPModel model = new HTTPModel();
		model.setUrl(serviceUrl);
		model.setBody(serviceDataMap);

		try {
			NotificationIdResponse response = HTTPUtils.post(model, NotificationIdResponse.class);

			LOG.info(response);
			return response.getNotification_id();
		} catch (Exception e) {
			LOG.error("Encountered error while retrieving sl_id from service.");
			return null;
		}
	}


	public static PredictAndWinPostResponse getBidPointsUpdated(Map<String, Object> serviceDataMap) {
		Map<String, Object> dataMapForPostRequest = new HashMap<>();
		dataMapForPostRequest.put("getdata", serviceDataMap);

		HTTPModel model = new HTTPModel();
		model.setUrl(PredictAndWinConstants.SERVICE_URL_FOR_BID_POINTS_UPDATE);
		model.setBody(serviceDataMap);
		try {
			PredictAndWinPostResponse response = HTTPUtils.post(model, PredictAndWinPostResponse.class);
			LOG.info("####Predict get points updated and Win PoST API key getdata##"
					+ gson.toJson(dataMapForPostRequest.get("getdata"))+ "; Response of post request of SQL data update: "+response);
			
			return response;
		} catch (Exception e) {
			LOG.error("###Exception in points updation for Post API  ." + e.getMessage());
			return null;
		}
	}
	

	public static PredictAndWinPostResponse matchWinnersPostAPI(
			List<MatchWinnerDetailPostRequest> matchWinnerDetailsPostList) {
		Map<String, Object> serviceDataMap = new HashMap<>();
		serviceDataMap.put("getdata", matchWinnerDetailsPostList);
		
		HTTPModel model = new HTTPModel();
		model.setUrl(PredictAndWinConstants.SERVICE_URL_FOR_MATCH_WINNER);
		model.setBody(serviceDataMap);

		try {
			PredictAndWinPostResponse response = HTTPUtils.post(model, PredictAndWinPostResponse.class);
			LOG.info("###match winners update POST API getData value" + gson.toJson(serviceDataMap.get("getdata")));
			return response;
		} catch (Exception e) {
			LOG.error("###Exception in winners POST API ." + e.getMessage());
			return null;
		}
	}

	/**
	 * Replace punctuation. Used for creating landing URL for cricket
	 * notification.
	 * 
	 * @param string
	 * @return
	 */
	public static String replacePunct(String string) {
		return string == null ? string : string.replace(",", "").replaceAll("\\s|/", "-");
	}

	/**
	 * Get notification id for the given host.
	 * 
	 * @param match
	 *            data for a match.
	 * @param target
	 *            the target host to get notification for.
	 * @return
	 */
	public static String getNotificationId(Map<String, Object> match, int target) {

		/*
		 * Populate the parameters regarding notification.
		 */
		Map<String, Object> serviceDataMap = new HashMap<>();
		serviceDataMap.put("editorName", CricketConstants.CRICKET_NOTIFICATION_EDITOR);
		serviceDataMap.put("nicon", CricketConstants.COMMENT_ICON_URL);
		serviceDataMap.put("ntitle", match.get(IngestionConstants.MATCH_NUMBER) + ", "
				+ match.get(IngestionConstants.SERIES_SHORT_DISPLAY_NAME));
		serviceDataMap.put("hosts", target);

		if (target == Host.DIVYA_APP_ANDROID_HOST || target == Host.DIVYA_APP_IPHONE_HOST
				|| target == Host.DIVYA_WEB_HOST || target == Host.DIVYA_MOBILE_WEB_HOST) {
			serviceDataMap.put("channel_slno", 960);
		} else {
			serviceDataMap.put("channel_slno", 521);
		}

		/*
		 * Populate the set of options to be sent.
		 */
		Map<String, Object> options = new HashMap<>();
		options.put("storyid", (String) match.get(IngestionConstants.MATCH_ID_FIELD));
		options.put("time_to_live", 180);

		serviceDataMap.put("option", options);

		HTTPModel model = new HTTPModel();
		model.setUrl(CricketConstants.NOTIFICATION_ID_SERVICE_URL);
		model.setBody(serviceDataMap);

		try {
			NotificationIdResponse response = HTTPUtils.post(model, NotificationIdResponse.class);

			LOG.info(response);
			return response.getNotification_id();
		} catch (Exception e) {
			LOG.error("Encountered error while retrieving sl_id from service.");
			return null;
		}
	}
}
