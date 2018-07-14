package com.db.cricket.model;

import java.util.List;

/**
 * CricketQuery POJO. Represents the query json for cricket.
 */
public class CricketQuery {

	/** Id of a cricket match */
	private String Match_Id = null;

	/** Used for date values as well as integer values as a start position. */
	private String from = null;

	/** The target date string for querying matches by date range */
	private String to = null;

	/**
	 * Whether the match is in progress or not. live=1 means the match is in
	 * progress. live=0 means the match has either ended or not happened yet.
	 */
	private String live = null;

	/**
	 * This field is holds 0 or 1 to indicate whether a match is upcoming or not.
	 */
	private String upcoming = null;

	/** The number of {@code CommentaryItem}'s to be returned. */
	private Integer count = null;

	/** Innings Number of the match */
	private String inningsNumber = null;

	/**
	 * Time interval in milliseconds after which each folder is precoessed after
	 * restarting the match.
	 */
	private String cricketRestartInterval = null;

	/** The directory containing the data files of a match. */
	private String cricketDirectory = null;

	/**
	 * Boolean value to indicate whether new commentaries are to be fetched. DEFAULT
	 * false to indicate older records after a specified id are to be returned.
	 */
	private Boolean newRecords = false;

	/** Id of a series of matches. */
	private List<String> seriesId = null;

	/**
	 * To indicate requirement of recent matches in match schedule.
	 * 
	 * Values: yes/no
	 */
	private String recent = null;

	/**
	 * Number of records to be fetched for a page.
	 * 
	 */
	private Integer pageSize = null;

	/**
	 * Page Number for which to fetch records.
	 */
	private Integer pageNumber = null;

	/**
	 * For sort order. Only available for Match Schedule
	 */
	private String sort;

	/**
	 * Id value of a Cricket Team
	 */
	private String teamId;

	private Boolean pastMatches = Boolean.FALSE;

	private String venueId;

	/**
	 * User Id for Predict N Win Users
	 */
	private String userId;

	private Boolean summary = Boolean.FALSE;

	private Boolean grouped = Boolean.FALSE;

	private String ticketId;

	private Boolean ticketSummary = Boolean.FALSE;

	private String lang = "en";

	private Boolean probableTeamsSquads = Boolean.FALSE;

	private String matchId;
	
	/** Id of a league of matches. */
	private List<String> leagueId = null;

	/**
	 * @return the matchId
	 */
	public String getMatchId() {
		return matchId;
	}

	/**
	 * @param matchId
	 *            the matchId to set
	 */
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}

	/**
	 * @return the lang
	 */
	public String getLang() {
		return lang;
	}

	/**
	 * @param lang
	 *            the lang to set
	 */
	public void setLang(String lang) {
		this.lang = lang;
	}

	/**
	 * @return the ticketSummary
	 */
	public Boolean getTicketSummary() {
		return ticketSummary;
	}

	/**
	 * @param ticketSummary
	 *            the ticketSummary to set
	 */
	public void setTicketSummary(Boolean ticketSummary) {
		this.ticketSummary = ticketSummary;
	}

	/**
	 * @return the ticketId
	 */
	public String getTicketId() {
		return ticketId;
	}

	/**
	 * @param ticketId
	 *            the ticketId to set
	 */
	public void setTicketId(String ticketId) {
		this.ticketId = ticketId;
	}

	/**
	 * @return the grouped
	 */
	public Boolean getGrouped() {
		return grouped;
	}

	/**
	 * @param grouped
	 *            the grouped to set
	 */
	public void setGrouped(Boolean grouped) {
		this.grouped = grouped;
	}

	/**
	 * @return the summary
	 */
	public Boolean getSummary() {
		return summary;
	}

	/**
	 * @param summary
	 *            the summary to set
	 */
	public void setSummary(Boolean summary) {
		this.summary = summary;
	}

	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * @param userId
	 *            the userId to set
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * @return the pastMatches
	 */
	public Boolean getPastMatches() {
		return pastMatches;
	}

	/**
	 * @param pastMatches
	 *            the pastMatches to set
	 */
	public void setPastMatches(Boolean pastMatches) {
		this.pastMatches = pastMatches;
	}

	public String getVenueId() {
		return venueId;
	}

	public void setVenueId(String venueId) {
		this.venueId = venueId;
	}

	public String getTeamId() {
		return teamId;
	}

	public void setTeamId(String teamId) {
		this.teamId = teamId;
	}

	/**
	 * For type of commentary required. Defaults to 1 for All commentary.
	 */
	private Integer type = 1;

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public Integer getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(Integer pageNumber) {
		this.pageNumber = pageNumber;
	}

	public String getRecent() {
		return recent;
	}

	public void setRecent(String recent) {
		this.recent = recent;
	}

	public String getMatch_Id() {
		return Match_Id;
	}

	public void setMatch_Id(String match_Id) {
		Match_Id = match_Id;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getLive() {
		return live;
	}

	public void setLive(String live) {
		this.live = live;
	}

	public String getUpcoming() {
		return upcoming;
	}

	public void setUpcoming(String upcoming) {
		this.upcoming = upcoming;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public String getInningsNumber() {
		return inningsNumber;
	}

	public void setInningsNumber(String inningsNumber) {
		this.inningsNumber = inningsNumber;
	}

	public String getCricketRestartInterval() {
		return cricketRestartInterval;
	}

	public void setCricketRestartInterval(String cricketRestartInterval) {
		this.cricketRestartInterval = cricketRestartInterval;
	}

	public String getCricketDirectory() {
		return cricketDirectory;
	}

	public void setCricketDirectory(String cricketDirectory) {
		this.cricketDirectory = cricketDirectory;
	}

	public Boolean getNewRecords() {
		return newRecords;
	}

	public void setNewRecords(Boolean newRecords) {
		this.newRecords = newRecords;
	}

	public List<String> getSeriesId() {
		return seriesId;
	}

	public void setSeriesId(List<String> seriesId) {
		this.seriesId = seriesId;
	}

	public Boolean getProbableTeamsSquads() {
		return probableTeamsSquads;
	}

	public void setProbableTeamsSquads(Boolean probableTeamsSquads) {
		this.probableTeamsSquads = probableTeamsSquads;
	}

	public List<String> getLeagueId() {
		return leagueId;
	}

	public void setLeagueId(List<String> leagueId) {
		this.leagueId = leagueId;
	}
}
