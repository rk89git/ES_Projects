package com.db.wisdom.product.model;

import java.util.Arrays;
import java.util.List;

import com.db.common.constants.Constants;
import com.db.common.constants.WisdomConstants;
import com.db.common.utils.DateUtil;

public class WisdomProductQuery {
	private boolean isOrderAsc=true;
	
	private boolean enableDate = true;
	
	private boolean storyDetail = false;
	
	private boolean isReply = false;

	private List<String> authorId;
	
	private String field;

	private List<Integer> categoryId;
	
	private List<String> categoryName;
	
	private List<Integer> sectionId;

	private List<String> sectionName;
	
	private String startPubDate;

	private String endPubDate;

	private String startDate;

	private String endDate;

	private String compareStartDate;
	
	private String compareEndDate;

	private String storyid;

	private List<String> domainId;

	private List<String> tracker;
	
	private String widget_name;
	
	private List<String> socialReferer = Arrays.asList("facebook", "twitter");

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
	
	private String dateField= WisdomConstants.DATETIME;

	private  int categorySize=20;
	
	private  Integer type;
	
	private  String minutes;
	
	private boolean prevDayRequired = false;
	
	private Double ctr;	
	
	private String status_type;	
	
	private String profile_id;
	
	private String weekDay;
	
	private List<String> ad_unit_id;
	
	private String url_domain;
	
	private String domain;
	
	private List<String> platform = Arrays.asList("web","mobile","android","iphone");
	
	private List<String> competitor;
	
	private String title;
	
	private String relative_url;
	
	private String facebook_profile_id;
	
	String fuzziness = "AUTO";
	
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

	public List<String> getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(List<String> cat_name) {
		this.categoryName = cat_name;
	}

	public List<String> getSectionName() {
		return sectionName;
	}

	public void setSectionName(List<String> pp_cat_name) {
		this.sectionName = pp_cat_name;
	}

	public void setOrderAsc(boolean orderAsc) {
		isOrderAsc = orderAsc;
	}

	public List<Integer> getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(List<Integer> cat_id) {
		this.categoryId = cat_id;
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

	public List<String> getAuthorId() {
		return authorId;
	}

	public void setAuthorId(List<String> uid) {
		this.authorId = uid;
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

	public List<String> getDomainId() {
		return domainId;
	}

	public void setDomainId(List<String> domainId) {
		this.domainId = domainId;
	}	

	public int getElementCount() {
		return elementCount;
	}

	public void setElementCount(int elementCount) {
		this.elementCount = elementCount;
	}

	public List<String> getSocialReferer() {
		return socialReferer;
	}

	public void setSocialReferer(List<String> socialTracker) {
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

	public List<Integer> getSectionId() {
		return sectionId;
	}

	public void setSectionId(List<Integer> pcat_id) {
		this.sectionId = pcat_id;
	}

	public String getStatus_type() {
		return status_type;
	}

	public void setStatus_type(String status_type) {
		this.status_type = status_type;
	}	
	
	public String getProfile_id() {
		return profile_id;
	}

	public void setProfile_id(String profile_id) {
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

	public List<String> getPlatform() {
		return platform;
	}

	public void setPlatform(List<String> platform) {
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

	public String getRelative_url() {
		return relative_url;
	}

	public void setRelative_url(String relative_url) {
		this.relative_url = relative_url;
	}

	public String getFacebook_profile_id() {
		return facebook_profile_id;
	}

	public void setFacebook_profile_id(String facebook_profile_id) {
		this.facebook_profile_id = facebook_profile_id;
	}
	
}
