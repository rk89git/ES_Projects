package com.db.wisdom.model;

import java.util.List;
import java.util.Map;

public class StoryDetail {

	private String author_name;

	private String image;

	private Boolean isUCB;

	private Boolean isFacebook;

	private Long mpvs;
	
	private Long muvs;
	
	private Long wuvs;

	private Long mSlideDepth;
	
	private Long wSlideDepth;
	
	private Long aSlideDepth;
	
	private Long iSlideDepth;

	private Long slideDepth;
	
	private Long pvContribution;
	
	private Long uvContribution;

	private Map<String, Map<String, Long>> pageViewsTracker;// = new HashMap<>();

	private Integer slideCount;

	private Map<String, Map<String, Long>> sourceWiseTraffic;// = new HashMap<>();

	private String storyid;

	private String story_pubtime;

	private String title;

	private Long totalpvs;

	private Long totaluvs;

	private String uid;

	private String url;

	private Integer version;
	
	private String channel_slno;

	private Long wpvs;
	
	private Long apvs;
	
	private Long ipvs;
	
	private Long auvs;
	
	private Long iuvs;
	
	private WisdomArticleRating rating;
	
	private List<String> feedback;
	
	private String bestHour;
	
	private String bestDay;
	
	private String maxRange;
	
	private String maxProb;
	
	private String bestNearestHour;
	
	private String bestNearestDay;
	
	private String bestNearestHourrange;
	
	private String bestNearestHourProb;
	
	private Long commentCount;
	
	private Long pvs;
	
	private String flag_v;
	
    private String super_cat_name;
	
	private String cat_name;
	
	private String ga_section;
	
	public WisdomArticleRating getRating() {
		return rating;
	}

	public void setRating(WisdomArticleRating rating) {
		this.rating = rating;
	}

	public List<String> getFeedback() {
		return feedback;
	}

	public void setFeedback(List<String> storyfeedback) {
		this.feedback = storyfeedback;
	}	
	public String getAuthor_name() {
		return author_name;
	}

	public void setAuthor_name(String author_name) {
		this.author_name = author_name;
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public Boolean getIsUCB() {
		return isUCB;
	}

	public void setIsUCB(Boolean isUCB) {
		this.isUCB = isUCB;
	}

	public Boolean getIsFacebook() {
		return isFacebook;
	}

	public void setIsFacebook(Boolean isFacebook) {
		this.isFacebook = isFacebook;
	}

	public Long getMpvs() {
		return mpvs;
	}

	public void setMpvs(Long mpvs) {
		this.mpvs = mpvs;
	}

	public Long getmSlideDepth() {
		return mSlideDepth;
	}

	public void setmSlideDepth(Long mSlideDepth) {
		this.mSlideDepth = mSlideDepth;
	}	

	public Long getwSlideDepth() {
		return wSlideDepth;
	}

	public void setwSlideDepth(Long wSlideDepth) {
		this.wSlideDepth = wSlideDepth;
	}

	public Long getSlideDepth() {
		return slideDepth;
	}

	public void setSlideDepth(Long slideDepth) {
		this.slideDepth = slideDepth;
	}
	
	public Long getaSlideDepth() {
		return aSlideDepth;
	}

	public void setaSlideDepth(Long aSlideDepth) {
		this.aSlideDepth = aSlideDepth;
	}

	public Long getiSlideDepth() {
		return iSlideDepth;
	}

	public void setiSlideDepth(Long iSlideDepth) {
		this.iSlideDepth = iSlideDepth;
	}

	public Map<String, Map<String, Long>> getPageViewsTracker() {
		return pageViewsTracker;
	}

	public void setPageViewsTracker(Map<String, Map<String, Long>> pageViewsTracker) {
		this.pageViewsTracker = pageViewsTracker;
	}

	public Integer getSlideCount() {
		return slideCount;
	}

	public void setSlideCount(Integer slideCount) {
		this.slideCount = slideCount;
	}

	public Map<String, Map<String, Long>> getSourceWiseTraffic() {
		return sourceWiseTraffic;
	}

	public void setSourceWiseTraffic(Map<String, Map<String, Long>> sourceWiseTraffic) {
		this.sourceWiseTraffic = sourceWiseTraffic;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}

	public String getStory_pubtime() {
		return story_pubtime;
	}

	public void setStory_pubtime(String story_pubtime) {
		this.story_pubtime = story_pubtime;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Long getTotalpvs() {
		return totalpvs;
	}

	public void setTotalpvs(Long totalpvs) {
		this.totalpvs = totalpvs;
	}

	public Long getTotaluvs() {
		return totaluvs;
	}

	public void setTotaluvs(Long totaluvs) {
		this.totaluvs = totaluvs;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Long getWpvs() {
		return wpvs;
	}

	public void setWpvs(Long wpvs) {
		this.wpvs = wpvs;
	}

	public String getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}

	public Long getPvContribution() {
		return pvContribution;
	}

	public void setPvContribution(Long pvContribution) {
		this.pvContribution = pvContribution;
	}

	public Long getApvs() {
		return apvs;
	}

	public void setApvs(Long apvs) {
		this.apvs = apvs;
	}

	public Long getIpvs() {
		return ipvs;
	}

	public void setIpvs(Long ipvs) {
		this.ipvs = ipvs;
	}

	public Long getMuvs() {
		return muvs;
	}

	public void setMuvs(Long muvs) {
		this.muvs = muvs;
	}

	public Long getWuvs() {
		return wuvs;
	}

	public void setWuvs(Long wuvs) {
		this.wuvs = wuvs;
	}

	public Long getAuvs() {
		return auvs;
	}

	public void setAuvs(Long auvs) {
		this.auvs = auvs;
	}

	public Long getIuvs() {
		return iuvs;
	}

	public void setIuvs(Long iuvs) {
		this.iuvs = iuvs;
	}

	public Long getUvContribution() {
		return uvContribution;
	}

	public void setUvContribution(Long uvContribution) {
		this.uvContribution = uvContribution;
	}

	public String getBestHour() {
		return bestHour;
	}

	public void setBestHour(String bestHour) {
		this.bestHour = bestHour;
	}

	public String getBestDay() {
		return bestDay;
	}

	public void setBestDay(String bestDay) {
		this.bestDay = bestDay;
	}

	public String getMaxRange() {
		return maxRange;
	}

	public void setMaxRange(String maxRange) {
		this.maxRange = maxRange;
	}

	public String getMaxProb() {
		return maxProb;
	}

	public void setMaxProb(String maxProb) {
		this.maxProb = maxProb;
	}

	public String getBestNearestHour() {
		return bestNearestHour;
	}

	public void setBestNearestHour(String bestNearestHour) {
		this.bestNearestHour = bestNearestHour;
	}

	public String getBestNearestDay() {
		return bestNearestDay;
	}

	public void setBestNearestDay(String bestNearestDay) {
		this.bestNearestDay = bestNearestDay;
	}

	public String getBestNearestHourrange() {
		return bestNearestHourrange;
	}

	public void setBestNearestHourrange(String bestNearestHourrange) {
		this.bestNearestHourrange = bestNearestHourrange;
	}

	public String getBestNearestHourProb() {
		return bestNearestHourProb;
	}

	public void setBestNearestHourProb(String bestNearestHourProb) {
		this.bestNearestHourProb = bestNearestHourProb;
	}

	public Long getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}

	public Long getPvs() {
		return pvs;
	}

	public void setPvs(Long pvs) {
		this.pvs = pvs;
	}

	public String getFlag_v() {
		return flag_v;
	}

	public void setFlag_v(String flag_v) {
		this.flag_v = flag_v;
	}

	public String getSuper_cat_name() {
		return super_cat_name;
	}

	public void setSuper_cat_name(String super_cat_name) {
		this.super_cat_name = super_cat_name;
	}

	public String getCat_name() {
		return cat_name;
	}

	public void setCat_name(String cat_name) {
		this.cat_name = cat_name;
	}

	public String getGa_section() {
		return ga_section;
	}

	public void setGa_section(String ga_section) {
		this.ga_section = ga_section;
	}	
	
}
