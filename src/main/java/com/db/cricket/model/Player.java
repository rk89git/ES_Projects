package com.db.cricket.model;

import com.google.gson.annotations.SerializedName;

public class Player {

	@SerializedName("bowling_style")
	private String bowlingStyle;

	@SerializedName("is_selected")
	private String isSelected;
	
	@SerializedName("nationality_id")
	private String nationalityId;
	
	@SerializedName("batting_style")
	private String battingStyle;
	
	@SerializedName("is_probable")
	private String isProbable;
	
	@SerializedName("is_captain")
	private String isCaptain;
	
	private String nationality;
	
	@SerializedName("skill_name")
	private String skillName;
	
	@SerializedName("is_marquee")
	private String isMarquee;
	
	private String name;
	
	@SerializedName("short_name")
	private String shortName;
	
	@SerializedName("skill_id")
	private String skillId;
	
	@SerializedName("is_wicket_keeper")
	private String isWicketKeeper;
	
	private String id;
	
	@SerializedName("jersey_number")
	private String jerseyNumber;

	public String getBowlingStyle() {
		return bowlingStyle;
	}

	public void setBowlingStyle(String bowlingStyle) {
		this.bowlingStyle = bowlingStyle;
	}

	public String getIsSelected() {
		return isSelected;
	}

	public void setIsSelected(String isSelected) {
		this.isSelected = isSelected;
	}

	public String getNationalityId() {
		return nationalityId;
	}

	public void setNationalityId(String nationalityId) {
		this.nationalityId = nationalityId;
	}

	public String getBattingStyle() {
		return battingStyle;
	}

	public void setBattingStyle(String battingStyle) {
		this.battingStyle = battingStyle;
	}

	public String getIsProbable() {
		return isProbable;
	}

	public void setIsProbable(String isProbable) {
		this.isProbable = isProbable;
	}

	public String getIsCaptain() {
		return isCaptain;
	}

	public void setIsCaptain(String isCaptain) {
		this.isCaptain = isCaptain;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getSkillName() {
		return skillName;
	}

	public void setSkillName(String skillName) {
		this.skillName = skillName;
	}

	public String getIsMarquee() {
		return isMarquee;
	}

	public void setIsMarquee(String isMarquee) {
		this.isMarquee = isMarquee;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getSkillId() {
		return skillId;
	}

	public void setSkillId(String skillId) {
		this.skillId = skillId;
	}

	public String getIsWicketKeeper() {
		return isWicketKeeper;
	}

	public void setIsWicketKeeper(String isWicketKeeper) {
		this.isWicketKeeper = isWicketKeeper;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getJerseyNumber() {
		return jerseyNumber;
	}

	public void setJerseyNumber(String jerseyNumber) {
		this.jerseyNumber = jerseyNumber;
	}
}