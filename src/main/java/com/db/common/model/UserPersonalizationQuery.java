package com.db.common.model;

import java.util.List;

public class UserPersonalizationQuery {
	private String session_id;
	
	private String startDate;
	
	private String endDate;
	
	private String hosts;
	
	private int count;
	
	private int storyCountQualifier = 5;

	private String channel;

	private int firstTimeFlag=1;

	private List<Integer> cat_id;
	
	private String title;
	
	private long cat_ids;

	private String keywords;
	
	private String flag_v;
	
	private int domain_identifier;
	
	/**
	 * Field to take pcat_id value. Required for querying trending articles of
	 * particular category.
	 */
	private int pCatId = 0;

	private int inCount = 0;
	
	public int getInCount() {
		return inCount;
	}

	public void setInCount(int inCount) {
		this.inCount = inCount;
	}

	private String logicVersion = "v1";
	private String trackers = "news-hf";

	private String storyid;

	public String getSession_id() {
		return session_id;
	}

	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String channelSlNos) {
		this.hosts = channelSlNos;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getStoryCountQualifier() {
		return storyCountQualifier;
	}

	public void setStoryCountQualifier(int storyCountQualifier) {
		this.storyCountQualifier = storyCountQualifier;
	}

	public String getLogicVersion() {
		return logicVersion;
	}

	public void setLogicVersion(String logicVersion) {
		this.logicVersion = logicVersion;
	}

	public String getTrackers() {
		return trackers;
	}

	public void setTrackers(String trackers) {
		this.trackers = trackers;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}

	public int getpCatId() {
		return pCatId;
	}

	public void setpCatId(int pCatId) {
		this.pCatId = pCatId;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public int getFirstTimeFlag() {
		return firstTimeFlag;
	}

	public void setFirstTimeFlag(int firstTimeFlag) {
		this.firstTimeFlag = firstTimeFlag;
	}

	public List<Integer> getCat_id() {
		return cat_id;
	}

	public void setCat_id(List<Integer> cat_id) {
		this.cat_id = cat_id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public long getCat_ids() {
		return cat_ids;
	}

	public void setCat_ids(long cat_ids) {
		this.cat_ids = cat_ids;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getFlag_v() {
		return flag_v;
	}

	public void setFlag_v(String flag_v) {
		this.flag_v = flag_v;
	}

	public int getDomain_identifier() {
		return domain_identifier;
	}

	public void setDomain_identifier(int domain_identifier) {
		this.domain_identifier = domain_identifier;
	}
	
	
}
