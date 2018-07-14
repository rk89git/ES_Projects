
package com.db.cricket.model;

import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Match {

	@SerializedName("daynight")
	private String daynight;

	@SerializedName("gmt_offset")
	private String gmtOffset;

	@SerializedName("group")
	private String group;

	@SerializedName("league")
	private String league;

	@SerializedName("live")
	private Integer live;

	@SerializedName("livecoverage")
	private String livecoverage;

	@SerializedName("match_Id")
	private String matchId;

	@SerializedName("matchfile")
	private String matchfile;

	@SerializedName("matchnumber")
	private String matchnumber;

	@SerializedName("matchresult")
	private String matchresult;

	@SerializedName("matchstatus")
	private String matchstatus;

	@SerializedName("matchdate_gmt")
	private String matchdateGmt;

	@SerializedName("matchdate_ist")
	private String matchdateIst;

	@SerializedName("matchdate_local")
	private String matchdateLocal;

	@SerializedName("matchtime_gmt")
	private String matchtimeGmt;

	@SerializedName("matchtime_ist")
	private String matchtimeIst;

	@SerializedName("matchtime_local")
	private String matchtimeLocal;

	@SerializedName("end_matchdate_gmt")
	private String endMatchdateGmt;

	@SerializedName("end_matchdate_ist")
	private String endMatchdateIst;

	@SerializedName("end_matchdate_local")
	private String endMatchdateLocal;

	@SerializedName("end_matchtime_gmt")
	private String endMatchtimeGmt;

	@SerializedName("end_matchtime_ist")
	private String endMatchtimeIst;

	@SerializedName("end_matchtime_local")
	private String endMatchtimeLocal;

	@SerializedName("matchtype")
	private String matchtype;

	@SerializedName("priority")
	private String priority;

	@SerializedName("recent")
	private String recent;

	@SerializedName("series_Id")
	private String seriesId;

	@SerializedName("seriesname")
	private String seriesname;

	@SerializedName("series_short_display_name")
	private String seriesShortDisplayName;

	@SerializedName("series_type")
	private String seriesType;

	@SerializedName("series_start_date")
	private String seriesStartDate;

	@SerializedName("series_end_date")
	private String seriesEndDate;

	@SerializedName("toss_elected_to")
	private String tossElectedTo;

	@SerializedName("toss_won_by")
	private String tossWonBy;

	@SerializedName("stage")
	private String stage;

	@SerializedName("teama")
	private String teama;

	@SerializedName("teama_short")
	private String teamaShort;

	@SerializedName("teama_Id")
	private String teamaId;

	@SerializedName("teamb")
	private String teamb;

	@SerializedName("teamb_short")
	private String teambShort;

	@SerializedName("teamb_Id")
	private String teambId;

	@SerializedName("tour_Id")
	private String tourId;

	@SerializedName("tourname")
	private String tourname;

	@SerializedName("upcoming")
	private String upcoming;

	@SerializedName("venue")
	private String venue;

	@SerializedName("venue_Id")
	private String venueId;

	@SerializedName("winningmargin")
	private String winningmargin;

	@SerializedName("winningteam_Id")
	private String winningteamId;

	@SerializedName("current_score")
	private String currentScore;

	@SerializedName("current_batting_team")
	private String currentBattingTeam;

	@SerializedName("teamscores")
	private String teamscores;

	@SerializedName("inn_team_1")
	private String innTeam1;

	@SerializedName("inn_score_1")
	private String innScore1;

	@SerializedName("inn_team_2")
	private String innTeam2;

	@SerializedName("inn_score_2")
	private String innScore2;

	@SerializedName("inn_team_3")
	private String innTeam3;

	@SerializedName("inn_score_3")
	private String innScore3;

	@SerializedName("inn_team_4")
	private String innTeam4;

	@SerializedName("inn_score_4")
	private String innScore4;

	@SerializedName("notifications_enabled")
	private Boolean notificationsEnabled;

	@SerializedName("notification_details")
	private Map<String, Object> notificationDetails;

	@SerializedName("widget_article_page")
	private Boolean inWidgetArticlePage;

	@SerializedName("widget_category_page")
	private Boolean inWidgetCategoryPage;

	@SerializedName("widget_home_page")
	private Boolean inWidgetHomePage;

	@SerializedName("widget_global")
	private Boolean widgetGlobal;

	@SerializedName("in_genius_history")
	private Boolean inGeniusHistory;
	
	@SerializedName("widget_event_enabled")
	private Boolean widgetEventEnabled;	

	@SerializedName("widget_sports_article_page")
	private Boolean inWidgetSportsArticlePage;

	private Map<String, Object> web_bhaskar;

	private Map<String, Object> wap_bhaskar;

	private Map<String, Object> app_bhaskar;

	private Map<String, Object> web_divya;

	private Map<String, Object> wap_divya;

	private Map<String, Object> app_divya;

	public Map<String, Object> getWeb_bhaskar() {
		return web_bhaskar;
	}

	public void setWeb_bhaskar(Map<String, Object> web_bhaskar) {
		this.web_bhaskar = web_bhaskar;
	}

	public Map<String, Object> getWap_bhaskar() {
		return wap_bhaskar;
	}

	public void setWap_bhaskar(Map<String, Object> wap_bhaskar) {
		this.wap_bhaskar = wap_bhaskar;
	}

	public Map<String, Object> getApp_bhaskar() {
		return app_bhaskar;
	}

	public void setApp_bhaskar(Map<String, Object> app_bhaskar) {
		this.app_bhaskar = app_bhaskar;
	}

	public Map<String, Object> getWeb_divya() {
		return web_divya;
	}

	public void setWeb_divya(Map<String, Object> web_divya) {
		this.web_divya = web_divya;
	}

	public Map<String, Object> getWap_divya() {
		return wap_divya;
	}

	public void setWap_divya(Map<String, Object> wap_divya) {
		this.wap_divya = wap_divya;
	}

	public Map<String, Object> getApp_divya() {
		return app_divya;
	}

	public void setApp_divya(Map<String, Object> app_divya) {
		this.app_divya = app_divya;
	}

	public Boolean getInWidgetSportsArticlePage() {
		return inWidgetSportsArticlePage;
	}

	public void setInWidgetSportsArticlePage(Boolean inWidgetSportsArticlePage) {
		this.inWidgetSportsArticlePage = inWidgetSportsArticlePage;
	}

	public Boolean getInWidgetArticlePage() {
		return inWidgetArticlePage;
	}

	public void setInWidgetArticlePage(Boolean inWidgetArticlePage) {
		this.inWidgetArticlePage = inWidgetArticlePage;
	}

	public Boolean getInWidgetCategoryPage() {
		return inWidgetCategoryPage;
	}

	public void setInWidgetCategoryPage(Boolean inWidgetCategoryPage) {
		this.inWidgetCategoryPage = inWidgetCategoryPage;
	}

	public Boolean getInWidgetHomePage() {
		return inWidgetHomePage;
	}

	public void setInWidgetHomePage(Boolean inWidgetHomePage) {
		this.inWidgetHomePage = inWidgetHomePage;
	}

	public Boolean getWidgetGlobal() {
		return widgetGlobal;
	}

	public void setWidgetGlobal(Boolean widgetGlobal) {
		this.widgetGlobal = widgetGlobal;
	}

	public Boolean getInGeniusHistory() {
		return inGeniusHistory;
	}

	public void setInGeniusHistory(Boolean inGeniusHistory) {
		this.inGeniusHistory = inGeniusHistory;
	}

	public Boolean getNotificationsEnabled() {
		return notificationsEnabled;
	}

	public void setNotificationsEnabled(Boolean notificationsEnabled) {
		this.notificationsEnabled = notificationsEnabled;
	}

	public Map<String, Object> getNotificationDetails() {
		return notificationDetails;
	}

	public void setNotificationDetails(Map<String, Object> notificationDetails) {
		this.notificationDetails = notificationDetails;
	}

	public String getDaynight() {
		return daynight;
	}

	public void setDaynight(String daynight) {
		this.daynight = daynight;
	}

	public String getGmtOffset() {
		return gmtOffset;
	}

	public void setGmtOffset(String gmtOffset) {
		this.gmtOffset = gmtOffset;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getLeague() {
		return league;
	}

	public void setLeague(String league) {
		this.league = league;
	}

	public Integer getLive() {
		return live;
	}

	public void setLive(Integer live) {
		this.live = live;
	}

	public String getLivecoverage() {
		return livecoverage;
	}

	public void setLivecoverage(String livecoverage) {
		this.livecoverage = livecoverage;
	}

	public String getMatchId() {
		return matchId;
	}

	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}

	public String getMatchfile() {
		return matchfile;
	}

	public void setMatchfile(String matchfile) {
		this.matchfile = matchfile;
	}

	public String getMatchnumber() {
		return matchnumber;
	}

	public void setMatchnumber(String matchnumber) {
		this.matchnumber = matchnumber;
	}

	public String getMatchresult() {
		return matchresult;
	}

	public void setMatchresult(String matchresult) {
		this.matchresult = matchresult;
	}

	public String getMatchstatus() {
		return matchstatus;
	}

	public void setMatchstatus(String matchstatus) {
		this.matchstatus = matchstatus;
	}

	public String getMatchdateGmt() {
		return matchdateGmt;
	}

	public void setMatchdateGmt(String matchdateGmt) {
		this.matchdateGmt = matchdateGmt;
	}

	public String getMatchdateIst() {
		return matchdateIst;
	}

	public void setMatchdateIst(String matchdateIst) {
		this.matchdateIst = matchdateIst;
	}

	public String getMatchdateLocal() {
		return matchdateLocal;
	}

	public void setMatchdateLocal(String matchdateLocal) {
		this.matchdateLocal = matchdateLocal;
	}

	public String getMatchtimeGmt() {
		return matchtimeGmt;
	}

	public void setMatchtimeGmt(String matchtimeGmt) {
		this.matchtimeGmt = matchtimeGmt;
	}

	public String getMatchtimeIst() {
		return matchtimeIst;
	}

	public void setMatchtimeIst(String matchtimeIst) {
		this.matchtimeIst = matchtimeIst;
	}

	public String getMatchtimeLocal() {
		return matchtimeLocal;
	}

	public void setMatchtimeLocal(String matchtimeLocal) {
		this.matchtimeLocal = matchtimeLocal;
	}

	public String getEndMatchdateGmt() {
		return endMatchdateGmt;
	}

	public void setEndMatchdateGmt(String endMatchdateGmt) {
		this.endMatchdateGmt = endMatchdateGmt;
	}

	public String getEndMatchdateIst() {
		return endMatchdateIst;
	}

	public void setEndMatchdateIst(String endMatchdateIst) {
		this.endMatchdateIst = endMatchdateIst;
	}

	public String getEndMatchdateLocal() {
		return endMatchdateLocal;
	}

	public void setEndMatchdateLocal(String endMatchdateLocal) {
		this.endMatchdateLocal = endMatchdateLocal;
	}

	public String getEndMatchtimeGmt() {
		return endMatchtimeGmt;
	}

	public void setEndMatchtimeGmt(String endMatchtimeGmt) {
		this.endMatchtimeGmt = endMatchtimeGmt;
	}

	public String getEndMatchtimeIst() {
		return endMatchtimeIst;
	}

	public void setEndMatchtimeIst(String endMatchtimeIst) {
		this.endMatchtimeIst = endMatchtimeIst;
	}

	public String getEndMatchtimeLocal() {
		return endMatchtimeLocal;
	}

	public void setEndMatchtimeLocal(String endMatchtimeLocal) {
		this.endMatchtimeLocal = endMatchtimeLocal;
	}

	public String getMatchtype() {
		return matchtype;
	}

	public void setMatchtype(String matchtype) {
		this.matchtype = matchtype;
	}

	public String getPriority() {
		return priority;
	}

	public void setPriority(String priority) {
		this.priority = priority;
	}

	public String getRecent() {
		return recent;
	}

	public void setRecent(String recent) {
		this.recent = recent;
	}

	public String getSeriesId() {
		return seriesId;
	}

	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	public String getSeriesname() {
		return seriesname;
	}

	public void setSeriesname(String seriesname) {
		this.seriesname = seriesname;
	}

	public String getSeriesShortDisplayName() {
		return seriesShortDisplayName;
	}

	public void setSeriesShortDisplayName(String seriesShortDisplayName) {
		this.seriesShortDisplayName = seriesShortDisplayName;
	}

	public String getSeriesType() {
		return seriesType;
	}

	public void setSeriesType(String seriesType) {
		this.seriesType = seriesType;
	}

	public String getSeriesStartDate() {
		return seriesStartDate;
	}

	public void setSeriesStartDate(String seriesStartDate) {
		this.seriesStartDate = seriesStartDate;
	}

	public String getSeriesEndDate() {
		return seriesEndDate;
	}

	public void setSeriesEndDate(String seriesEndDate) {
		this.seriesEndDate = seriesEndDate;
	}

	public String getTossElectedTo() {
		return tossElectedTo;
	}

	public void setTossElectedTo(String tossElectedTo) {
		this.tossElectedTo = tossElectedTo;
	}

	public String getTossWonBy() {
		return tossWonBy;
	}

	public void setTossWonBy(String tossWonBy) {
		this.tossWonBy = tossWonBy;
	}

	public String getStage() {
		return stage;
	}

	public void setStage(String stage) {
		this.stage = stage;
	}

	public String getTeama() {
		return teama;
	}

	public void setTeama(String teama) {
		this.teama = teama;
	}

	public String getTeamaShort() {
		return teamaShort;
	}

	public void setTeamaShort(String teamaShort) {
		this.teamaShort = teamaShort;
	}

	public String getTeamaId() {
		return teamaId;
	}

	public void setTeamaId(String teamaId) {
		this.teamaId = teamaId;
	}

	public String getTeamb() {
		return teamb;
	}

	public void setTeamb(String teamb) {
		this.teamb = teamb;
	}

	public String getTeambShort() {
		return teambShort;
	}

	public void setTeambShort(String teambShort) {
		this.teambShort = teambShort;
	}

	public String getTeambId() {
		return teambId;
	}

	public void setTeambId(String teambId) {
		this.teambId = teambId;
	}

	public String getTourId() {
		return tourId;
	}

	public void setTourId(String tourId) {
		this.tourId = tourId;
	}

	public String getTourname() {
		return tourname;
	}

	public void setTourname(String tourname) {
		this.tourname = tourname;
	}

	public String getUpcoming() {
		return upcoming;
	}

	public void setUpcoming(String upcoming) {
		this.upcoming = upcoming;
	}

	public String getVenue() {
		return venue;
	}

	public void setVenue(String venue) {
		this.venue = venue;
	}

	public String getVenueId() {
		return venueId;
	}

	public void setVenueId(String venueId) {
		this.venueId = venueId;
	}

	public String getWinningmargin() {
		return winningmargin;
	}

	public void setWinningmargin(String winningmargin) {
		this.winningmargin = winningmargin;
	}

	public String getWinningteamId() {
		return winningteamId;
	}

	public void setWinningteamId(String winningteamId) {
		this.winningteamId = winningteamId;
	}

	public String getCurrentScore() {
		return currentScore;
	}

	public void setCurrentScore(String currentScore) {
		this.currentScore = currentScore;
	}

	public String getCurrentBattingTeam() {
		return currentBattingTeam;
	}

	public void setCurrentBattingTeam(String currentBattingTeam) {
		this.currentBattingTeam = currentBattingTeam;
	}

	public String getTeamscores() {
		return teamscores;
	}

	public void setTeamscores(String teamscores) {
		this.teamscores = teamscores;
	}

	public String getInnTeam1() {
		return innTeam1;
	}

	public void setInnTeam1(String innTeam1) {
		this.innTeam1 = innTeam1;
	}

	public String getInnScore1() {
		return innScore1;
	}

	public void setInnScore1(String innScore1) {
		this.innScore1 = innScore1;
	}

	public String getInnTeam2() {
		return innTeam2;
	}

	public void setInnTeam2(String innTeam2) {
		this.innTeam2 = innTeam2;
	}

	public String getInnScore2() {
		return innScore2;
	}

	public void setInnScore2(String innScore2) {
		this.innScore2 = innScore2;
	}

	public String getInnTeam3() {
		return innTeam3;
	}

	public void setInnTeam3(String innTeam3) {
		this.innTeam3 = innTeam3;
	}

	public String getInnScore3() {
		return innScore3;
	}

	public void setInnScore3(String innScore3) {
		this.innScore3 = innScore3;
	}

	public String getInnTeam4() {
		return innTeam4;
	}

	public void setInnTeam4(String innTeam4) {
		this.innTeam4 = innTeam4;
	}

	public String getInnScore4() {
		return innScore4;
	}

	public void setInnScore4(String innScore4) {
		this.innScore4 = innScore4;
	}

	public Boolean getWidgetEventEnabled() {
		return widgetEventEnabled;
	}

	public void setWidgetEventEnabled(Boolean widgetEventEnabled) {
		this.widgetEventEnabled = widgetEventEnabled;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
