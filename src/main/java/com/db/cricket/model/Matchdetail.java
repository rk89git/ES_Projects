
package com.db.cricket.model;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Matchdetail {

	@SerializedName("Status")
	private String status;

	@SerializedName("Umpires")
	private String umpires;

	@SerializedName("Venue")
	private String venue;

	@SerializedName("Venue_Id")
	private String venueId;

	@SerializedName("Referee")
	private String referee;

	@SerializedName("Equation")
	private String equation;

	@SerializedName("Time")
	private String time;

	@SerializedName("Tour_Name")
	private String tourName;

	@SerializedName("Code")
	private String code;

	@SerializedName("Date")
	private String date;

	@SerializedName("Offset")
	private String offset;

	@SerializedName("Weather")
	private String weather;

	@SerializedName("Type")
	private String type;

	@SerializedName("Tosswonby")
	private String tosswonby;

	@SerializedName("Number")
	private String number;

	@SerializedName("Series_Name")
	private String seriesName;

	@SerializedName("Series_Id")
	private String seriesId;

	@SerializedName("Livecoverage")
	private String livecoverage;

	@SerializedName("Daynight")
	private String daynight;

	@SerializedName("Id")
	private String id;

	@SerializedName("Day")
	private String day;

	@SerializedName("Session")
	private Integer session;

	private String live;

	@SerializedName("Prematch")
	private String preMatch;

	public String getSeriesId() {
		return seriesId;
	}

	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	public String getPreMatch() {
		return preMatch;
	}

	public void setPreMatch(String preMatch) {
		this.preMatch = preMatch;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getUmpires() {
		return umpires;
	}

	public void setUmpires(String umpires) {
		this.umpires = umpires;
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

	public String getReferee() {
		return referee;
	}

	public void setReferee(String referee) {
		this.referee = referee;
	}

	public String getEquation() {
		return equation;
	}

	public void setEquation(String equation) {
		this.equation = equation;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getTourName() {
		return tourName;
	}

	public void setTourName(String tourName) {
		this.tourName = tourName;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getTosswonby() {
		return tosswonby;
	}

	public void setTosswonby(String tosswonby) {
		this.tosswonby = tosswonby;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String getSeriesName() {
		return seriesName;
	}

	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}

	public String getLivecoverage() {
		return livecoverage;
	}

	public void setLivecoverage(String livecoverage) {
		this.livecoverage = livecoverage;
	}

	public String getDaynight() {
		return daynight;
	}

	public void setDaynight(String daynight) {
		this.daynight = daynight;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public Integer getSession() {
		return session;
	}

	public void setSession(Integer session) {
		this.session = session;
	}

	public String getLive() {
		return live;
	}

	public void setLive(String live) {
		this.live = live;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
