package com.db.wisdom.model;

import java.util.Arrays;
import java.util.List;

import com.db.common.constants.Constants;
import com.db.common.utils.DateUtil;

public class WisdomQuery {
	private boolean isOrderAsc=true;
	
	private boolean enableDate = true;
	
	private boolean storyDetail = false;
	
	private boolean isReply = false;

	private List<String> uid;
	
	private String field;

	private List<Integer> cat_id;
	
	private List<String> cat_name;
	
	private List<Integer> pcat_id;

	private List<String> pp_cat_name;
	
	private List<String> ga_cat_name;

	private Integer story_attribute;

	private String startPubDate;

	private String endPubDate;

	private String startDate;

	private String endDate;

	private String compareStartDate;
	
	private String compareEndDate;

	private String storyid;
	
	private String ref_platform;

	private List<Integer> host;
	
	private List<String> host_type = Arrays.asList("m", "w");
		
	private List<Integer> channel_slno;

	//to query on tracker.simple field
	private List<String> tracker;
	//to query on tracker field for sessions in sessions index
	private List<String> sessionTracker;
	
	private String widget_name;
	private List<Integer> super_cat_id;
	private List<String> super_cat_name;
	private Integer sim_date_count;
	private List<String> socialTracker = Arrays.asList("news-fpaid", "news-bgp", "news-vpaid", "news-opd",
	                                                    "news-gpd", "news-njp", "news-fpamn", "news-aff",
	                                                    "news-facebook", "news-fbo");

	private List<String> flickerTracker = Arrays.asList("news-hf","news-fli","news-whf");
	
	private List<String> fpaidTracker = Arrays.asList("news-fpaid", "news-bgp", "news-vpaid", "news-opd", 
			"news-gpd", "news-njp", "news-fpamn", "news-aff");
	
	private List<String> forganicTracker = Arrays.asList("news-fbo");
	
	private List<String> rhsTracker = Arrays.asList("news-rec","news-rece","news-recd","news-recdb");
	
	private List<String> excludeTracker;

	private int count = 20;
	
	private int elementCount = 3;
	
	private String interval;
	
	private String parameter = "total_reach";
	
	private String dateField= Constants.DATE_TIME_FIELD;

	private  int categorySize=20;
	
	private  Integer type;
	
	private  String minutes;
	
	private boolean prevDayRequired = false;
	
	private Double ctr;	
	
	private String status_type;	
	
	private List<String> profile_id;
	
	private String weekDay;
	
	private List<String> ad_unit_id;
	
	private String url_domain;
	
	private String domain;
	
	private String platform;
	
	private List<String> competitor;
	
	private String title;
	
	private String session_type;
	
	private String prediction;
	
	private Integer score = 0;
	
	private String domaintype;
	
	String fuzziness = "AUTO";
	
	String category_type;
	
	private List<String> spl_tracker;
	
	private String filter;
	
	private List<String> identifierValue;
	
	private List<String> keywords;
	
	private List<Integer> flag_v;
	
	public String getSession_type() {
		return session_type;
	}

	public void setSession_type(String session_type) {
		this.session_type = session_type;
	}	
	
	public String getWidget_name() {
		return widget_name;
	}

	public void setWidget_name(String widget_name) {
		this.widget_name = widget_name;
	}

	public Double getCtr() {
		return ctr;
	}

	public void setCtr(Double ctr) {
		this.ctr = ctr;
	}

	public int getCategorySize() {
		return categorySize;
	}

	public void setCategorySize(int categorySize) {
		this.categorySize = categorySize;
	}

	public boolean isOrderAsc() {
		return isOrderAsc;
	}

	public List<String> getCat_name() {
		return cat_name;
	}

	public void setCat_name(List<String> cat_name) {
		this.cat_name = cat_name;
	}

	public List<String> getPp_cat_name() {
		return pp_cat_name;
	}

	public void setPp_cat_name(List<String> pp_cat_name) {
		this.pp_cat_name = pp_cat_name;
	}

	public void setOrderAsc(boolean orderAsc) {
		isOrderAsc = orderAsc;
	}

	public List<Integer> getCat_id() {
		return cat_id;
	}

	public void setCat_id(List<Integer> cat_id) {
		this.cat_id = cat_id;
	}

	public Integer getStory_attribute() {
		return story_attribute;
	}

	public void setStory_attribute(Integer story_attribute) {
		this.story_attribute = story_attribute;
	}

	public List<String> getTracker() {
		return tracker;
	}

	public void setTracker(List<String> tracker) {
		this.tracker = tracker;
	}

	public List<String> getExcludeTracker() {
		return excludeTracker;
	}

	public void setExcludeTracker(List<String> excludeTracker) {
		this.excludeTracker = excludeTracker;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String url) {
		this.storyid = url;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDatetime) {
		this.endDate = endDatetime;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String date) {
		this.startDate = date;
	}

	public List<String> getUid() {
		return uid;
	}

	public void setUid(List<String> uid) {
		this.uid = uid;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getStartPubDate() {
		return startPubDate;
	}

	public void setStartPubDate(String startPubDate) {
		this.startPubDate = startPubDate;
	}

	public String getEndPubDate() {
		return endPubDate;
	}

	public void setEndPubDate(String endPubDate) {
		this.endPubDate = endPubDate;
	}

	public String getCompareStartDate() {
		return compareStartDate;
	}

	public void setCompareStartDate(String compareStartDate) {
		this.compareStartDate = compareStartDate;
	}

	public String getCompareEndDate() {
		return compareEndDate;
	}

	public void setCompareEndDate(String compareEndDate) {
		this.compareEndDate = compareEndDate;
	}

	public List<Integer> getHost() {
		return host;
	}

	public void setHost(List<Integer> host) {
		this.host = host;
	}

	public List<Integer> getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(List<Integer> channel_slno) {
		this.channel_slno = channel_slno;
	}	

	public List<String> getHost_type() {
		return host_type;
	}

	public void setHost_type(List<String> host_type) {
		this.host_type = host_type;
	}

	public int getElementCount() {
		return elementCount;
	}

	public void setElementCount(int elementCount) {
		this.elementCount = elementCount;
	}

	public List<String> getSocialTracker() {
		return socialTracker;
	}

	public void setSocialTracker(List<String> socialTracker) {
		socialTracker = socialTracker;
	}	

	public List<String> getFlickerTracker() {
		return flickerTracker;
	}

	public void setFlickerTracker(List<String> flickerTracker) {
		this.flickerTracker = flickerTracker;
	}

	public List<String> getFpaidTracker() {
		return fpaidTracker;
	}

	public void setFpaidTracker(List<String> fpaidTracker) {
		this.fpaidTracker = fpaidTracker;
	}

	public List<String> getForganicTracker() {
		return forganicTracker;
	}

	public void setForganicTracker(List<String> forganicTracker) {
		this.forganicTracker = forganicTracker;
	}

	public List<String> getRhsTracker() {
		return rhsTracker;
	}

	public void setRhsTracker(List<String> rhsTracker) {
		this.rhsTracker = rhsTracker;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public boolean isEnableDate() {
		return enableDate;
	}

	public void setEnableDate(boolean enableDate) {
		this.enableDate = enableDate;
	}

	public boolean isStoryDetail() {
		return storyDetail;
	}

	public void setStoryDetail(boolean storyDetail) {
		this.storyDetail = storyDetail;
	}

	public String getInterval() {
		return interval;
	}

	public void setInterval(String interval) {
		this.interval = interval;
	}

	public String getParameter() {
		return parameter;
	}

	public void setParameter(String parameter) {
		this.parameter = parameter;
	}

	public String getDateField() {
		return dateField;
	}

	public void setDateField(String dateField) {
		this.dateField = dateField;
	}

	public List<String> getGa_cat_name() {
		return ga_cat_name;
	}

	public void setGa_cat_name(List<String> ga_cat_name) {
		this.ga_cat_name = ga_cat_name;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public String getMinutes() {
		return minutes;
	}

	public void setMinutes(String minutes) {
		this.minutes = minutes;
	}

	public boolean isPrevDayRequired() {
		return prevDayRequired;
	}

	public void setPrevDayRequired(boolean prevDayRequired) {
		this.prevDayRequired = prevDayRequired;
	}

	public List<Integer> getPcat_id() {
		return pcat_id;
	}

	public void setPcat_id(List<Integer> pcat_id) {
		this.pcat_id = pcat_id;
	}

	public String getStatus_type() {
		return status_type;
	}

	public void setStatus_type(String status_type) {
		this.status_type = status_type;
	}	
	
	public List<String> getProfile_id() {
		return profile_id;
	}

	public void setProfile_id(List<String> profile_id) {
		this.profile_id = profile_id;
	}

	public String getWeekDay() {
		return weekDay;
	}

	public void setWeekDay(String weekDay) {
		this.weekDay = weekDay;
	}	
	
	public List<String> getAd_unit_id() {
		return ad_unit_id;
	}

	public void setAd_unit_id(List<String> ad_unit_id) {
		this.ad_unit_id = ad_unit_id;
	}

	public String getUrl_domain() {
		return url_domain;
	}

	public void setUrl_domain(String url_domain) {
		this.url_domain = url_domain;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public List<String> getCompetitor() {
		return competitor;
	}

	public void setCompetitor(List<String> competitor) {
		this.competitor = competitor;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public boolean isReply() {
		return isReply;
	}

	public void setReply(boolean isReply) {
		this.isReply = isReply;
	}

	public String getFuzziness() {
		return fuzziness;
	}

	public void setFuzziness(String fuzziness) {
		this.fuzziness = fuzziness;
	}


	public List<String> getSuper_cat_name() {
		return super_cat_name;
	}

	public void setSuper_cat_name(List<String> super_cat_name) {
		this.super_cat_name = super_cat_name;
	}

	public List<Integer> getSuper_cat_id() {
		return super_cat_id;
	}

	public void setSuper_cat_id(List<Integer> super_cat_id) {
		this.super_cat_id = super_cat_id;
	}

	public String getPrediction() {
		return prediction;
	}

	public void setPrediction(String prediction) {
		this.prediction = prediction;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	public String getDomaintype() {
		return domaintype;
	}

	public void setDomaintype(String domaintype) {
		this.domaintype = domaintype;
	}

	public Integer getSim_date_count() {
		return sim_date_count;
	}

	public void setSim_date_count(Integer sim_date_count) {
		this.sim_date_count = sim_date_count;
	}
	
	public String getCategory_type() {
		return category_type;
	}

	public void setCategory_type(String category_type) {
		this.category_type = category_type;
	}

	public List<String> getSpl_tracker() {
		return spl_tracker;
	}

	public void setSpl_tracker(List<String> spl_tracker) {
		this.spl_tracker = spl_tracker;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public List<String> getIdentifierValue() {
		return identifierValue;
	}

	public void setIdentifierValue(List<String> identifierValue) {
		this.identifierValue = identifierValue;
	}
	
	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public String getRef_platform() {
		return ref_platform;
	}

	public void setRef_platform(String ref_platform) {
		this.ref_platform = ref_platform;
	}

	public List<String> getSessionTracker() {
		return sessionTracker;
	}

	public void setSessionTracker(List<String> sessionTracker) {
		this.sessionTracker = sessionTracker;
	}	
	
	public List<Integer> getFlag_v() {
		return flag_v;
	}

	public void setFlag_v(List<Integer> flag_v) {
		this.flag_v = flag_v;
	}
	
}
