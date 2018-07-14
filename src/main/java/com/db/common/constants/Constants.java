package com.db.common.constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.db.common.constants.Constants.CricketConstants.PlayerProfile;

/**
 * Class specifying the constants used in db-analytics.
 *
 */
public final class Constants {

	public static final String WEIGHTAGE_STATS_FIELD = "weightagestats";
	public static final String APP_VERSION = "app_version";
	public static final String DEVICE_OS_VERSION = "device_os_ver";
	public static final String APP_VERSION_PADDED = "app_version_padded";
	public static final String DEVICE_OS_VERSION_PADDED = "device_os_ver_padded";
	/**
	 * Constant specifying the number of shards used for index distribution.
	 */
	public static final String NUMBER_OF_SHARDS = "number_of_shards";

	/**
	 * Constant specifying the number of replicas per shard used for index
	 * distribution.
	 */
	public static final String NUMBER_OF_REPLICAS = "number_of_replicas";

	/**
	 * Constants specify for index name.
	 */
	public static final String INDEX_NAME = "indexName";

	/**
	 * Constants specify for index mapping.
	 */
	public static final String INDEX_MAPPING = "indexMapping";

	/**
	 * Constant specify the rowId field.
	 */

	public static final String PROFILE_ID = "profile_id";

	public static final String ROWID = "_id";

	public static final String DATE_TIME_FIELD = "datetime";

	public static final String DATE = "date";

	public static final String SESSION_ID_FIELD = "session_id";

	public static final String STORY_ID_FIELD = "storyid";

	public static final String CLASSFICATION_ID_FIELD = "classificationid";

	public static final String CAT_ID_FIELD = "cat_id";

	public static final String CAT_ID1 = "cat_id1";

	public static final String CAT_NAME = "cat_name";

	public static final String PARENT_CAT_ID_FIELD = "pcat_id";

	public static final String SESS_ID_FIELD = "sess_id";

	public static final String STORY_ATTRIBUTE = "story_attribute";

	public static final String TITLE = "title";

	public static final String URL = "url";

	public static final String NAME = "name";

	public static final String UVS = "uvs";

	public static final String PVS = "pvs";
	
	public static final String UPVS = "upvs";

	public static final String PGTOTAL = "pgtotal";

	public static final String TRACKER = "tracker";
	
	public static final String TRACKER_SIMPLE = "tracker.simple";

	public static final String REF_PLATFORM = "ref_platform";

	public static final String VERSION = "version";

	public static final String HOST = "host";

	public static final String SESSION_COUNT = "session_count";

	public static final String SESSION_TIMESTAMP = "session_timestamp";

	public static final String SECTION = "section";

	public static final String IMAGE = "image";
	
	public static final String SELF_RATING = "self_rating";

	public static final String DIMENSION = "dimension";

	public static final String STORY_PUBLISH_TIME = "story_pubtime";

	public static final String STORY_MODIFIED_TIME = "story_modifiedtime";

	public static final String REC_TYPE = "rec_type";

	public static final String CHANNEL_SLNO = "channel_slno";

	public static final String CHANNEL_ID = "channel_id";

	public static final String DEVICE_TOKEN = "device_token";

	public static final String DEVICE_NAME = "device_name";

	public static final String OS = "os";

	public static final String NETWORK = "network";

	public static final String STORY_COUNT = "story_count";

	public static final String SUCCESS = "success";

	public static final String FAILURE = "failure";

	public static final String CANONICAL = "canonical";

	public static final String EXECUTIONTIME = "executionTime";

	public static final String META_KEYWORDS = "meta_keywords";

	public static final String ARTICLE_IMAGE = "article_image";

	public static final String HOST_TYPE = "host_type";

	public static final String DEVICE_TYPE = "device_type";

	public static final String PLATFORM = "platform";

	public static final String MESSAGE = "message";

	/**
	 * BRAND_ID field for impression_logs table
	 */
	public static final String BRAND_ID = "brand_id";

	/**
	 * IMPRESSION_TIME field for impression_logs table
	 */
	public static final String IMPRESSION_TIME = "impression_time";

	public static final String COUNTRY = "country";

	public static final String STATUS = "status";

	public static final String SUBSCRIBED_ON = "subscribed_on";

	public static final String CITY = "city";

	public static final String STATE = "state";

	public static final String _SCORE = "_score";

	public static final String CITY_FIELD = "city_field";

	public static final String STATE_FIELD = "state_field";

	public static final String COUNTRY_FIELD = "country_field";

	public static final String STORIES = "stories";

	public static final Integer NOTIFICATION_OFF = 0;

	public static final Integer NOTIFICATION_ON = 1;

	public static final Integer NOTIFICATION_UN_REGISTERED = 2;

	public static final String SUBSCRIBER_ID = "subscriber_id";

	public static final String NOTIFICATION_STATUS = "notificationStatus";

	public static final String VIDEO_COUNT = "video_count";

	public static final String VIDEO_ID = "video_id";

	public static final String VIDEO_VIEWS = "video_views";

	public static final String LANGUAGE_ID_FIELD = "lang_id";

	public static final String EVENT_TYPE = "event_type";

	public static final String REFERER = "referer";

	public static final String APP = "app";

	public static final String IDENTIFICATION = "identification";

	public static final int EVENT_TYPE_IMPRESSION = 1;

	public static final int EVENT_TYPE_CLICK = 2;

	public static final String IMPRESSIONS = "impressions";

	public static final String CLICKS = "clicks";

	public static final String VIEWS = "views";

	public static final String CREATED_AT = "created_at";

	// For Recommendation
	public static final String DESCRIPTION = "description";

	public static final String MODIFIED_DESCRIPTION = "modified_description";

	public static final String NEWS_KEYWORDS = "news_keywords";

	public static final String KEYWORDS = "keywords";

	public static final String PUBLISHED_DATE = "published_date";

	public static final String CREATED_DATE = "created_date";

	public static final String LANGUAGE = "language";

	public static final String SITE_NAME = "site_name";

	public static final String BRAND_URL = "brand_url";

	public static final String BRAND = "brand";

	public static final String BRAND_NAME = "brand_name";

	public static final String REFERAL_DOMAIN = "referal_domain";

	public static final String REFERER_DOMAIN = "referer_domain";

	public static final String SEC_TRACK = "sec_track";

	public static final String SEC_CITY = "sec_city";

	public static final String SITE_TRACKER = "site_tracker";

	public static final String DOMAINTYPE = "domaintype";

	public static final String BROWSER = "browser";

	public static final String USER_ID = "user_id";

	public static final String RECOMMENDATION = "recommendation";

	public static final String POLL = "poll";

	public static final String META_NOUNS = "meta_nouns";

	public static final String CONTINENT = "continent";

	public static final String IP1 = "ip1";

	public static final String IP2 = "ip2";

	public static final String REF_HOST = "ref_host";

	public static final String NLP_NOUNS = "nlp_nouns";

	public static final String STORY_ID = "story_id";

	public static final String WIDGET_NAME = "widget_name";

	public static final String WIDGET_REC_POS = "widget_rec_pos";

	public static final String ENTITIES = "entities";

	public static final String POSITION = "position";

	public static final String SESS_ID = "sess_id";

	public static final String REFERER_HOST = "referer_host";

	public static final String REFERER_PATH = "referer_path";

	public static final String NEWS_UCB = "news-ucb";

	public static final Integer RELATED_ARTICLE_SIZE = 2;

	public static final Integer USER_PERSONALIZED_ARTICLE_TIME_RANGE_IN_HOUR = -4;

	public static final Integer DEFAULT_MOST_TRENDING_ARTICLE_TIME_RANGE_IN_HOUR = -4;

	public static final Integer MOST_TRENDING_FAILOVER_ARTICLE_TIME_RANGE_IN_HOUR = -240;

	public static final Integer RELATED_ARTICLE_TIME_RANGE_IN_HOURS = -240;

	public static final Integer MOST_TRENDING_ARTICLE_SIZE = 60;

	public static final Integer COLLOBORATING_MINIMUM_DOC_COUNT = 3;

	public static final String REC_DAILY_LOG_INDEX_PREFIX = "rec_log_";

	public static final Map<String, Integer> CUSTOM_RULE_MAP;

	public static final String AUTHOR_NAME = "author_name";

	public static final Integer AUTHOR_COUNT_FOR_DAILY_CONTRIBUTION = 500;

	public static final String UID = "uid";

	public static final String PGNO = "pgno";

	public static final String WPVS = "wpvs";
	public static final String MPVS = "mpvs";
	public static final String APVS = "apvs";
	public static final String IPVS = "ipvs";
	public static final String UCBPVS = "ucbpvs";
	public static final String OTHERPVS = "otherpvs";

	public static final String PID = "pid";
	public static final String ABBREVIATION = "abbr";

	public static final String PEOPLE = "people";

	public static final String ORGANIZATION = "organization";

	public static final String LOCATION = "location";

	public static final String OTHER = "other";

	public static final String EVENT = "event";

	public static final String STORY_TYPE = "story_type";

	public static final String TOTAL_REACH = "total_reach";

	public static final String UNIQUE_REACH = "unique_reach";

	public static final String LINK_CLICKS = "link_clicks";

	public static final String REACTION_THANKFUL = "reaction_thankful";

	public static final String REACTION_SAD = "reaction_sad";

	public static final String REACTION_ANGRY = "reaction_angry";

	public static final String REACTION_WOW = "reaction_wow";

	public static final String REACTION_HAHA = "reaction_haha";

	public static final String REACTION_LOVE = "reaction_love";

	public static final String HIDE_CLICKS = "hide_clicks";

	public static final String HIDE_ALL_CLICKS = "hide_all_clicks";

	public static final String REPORT_SPAM_CLICKS = "report_spam_clicks";

	public static final String IA_CLICKS = "ia_clicks";

	public static final String UNIQUE_VIDEO_VIEWS = "unique_video_views";

	public static final String IA_STORYID = "ia_storyid";

	public static final String IA_STORY_COUNT = "ia_story_count";

	public static final String IA_FLAG = "ia_flag";

	public static final String LIKES = "likes";

	public static final String COMMENTS = "comments";

	public static final String DVB_APP = "dvb_app";

	public static final String DB_APP = "db_app";

	public static final String IA_ALL_VIEWS = "ia_all_views";

	public static final String PAGE_UNIQUE_VIEWS = "page_unique_views";

	public static final String PAGE_VIEWS = "page_views";

	public static final String PAGE_UNLIKES = "page_unlikes";

	public static final String PAGE_LIKES = "page_likes";

	public static final String PAGE_POSITIVE_FEEDBACK_BY_TYPE = "page_positive_feedback_by_type";

	public static final String PAGE_NEGATIVE_FEEDBACK_BY_TYPE = "page_negative_feedback_by_type";

	public static final String PAGE_NEGATIVE_FEEDBACK = "page_negative_feedback";

	public static final String PAGE_CONSUMPTIONS_BY_CONSUMPTION_TYPE = "page_consumptions_by_consumption_type";

	public static final String PAGE_CONSUMPTIONS = "page_consumptions";

	public static final String PAGE_POST_ENGAGEMENTS = "page_post_engagements";

	public static final String TOTAL_ENGAGEMENT = "total_engagement";

	public static final String MAX_REACH_CLASS = "max_reach_class";

	public static final String MAX_REACH_PROB = "max_reach_prob";

	/**
	 * field1 = unique_reach/link_clicks
	 */

	public static final String FIELD1 = "field1";

	/**
	 * field2 = (total_reach-unique_reach)/total_reach
	 */

	public static final String FIELD2 = "field2";

	/**
	 * field3 = (reactions*shares)/reports
	 */

	public static final String FIELD3 = "field3";

	public static final String SCORE = "score";

	public static final String SLOT = "slot";

	public static final String IS_POPULAR = "is_popular";

	public static final String IS_REPOSTABLE = "is_repostable";

	public static final String POPULAR_PERCENTAGE = "popular_percentage";

	public static final String POPULARITY_SCORE = "popularity_score";

	public static final String POPULAR_COUNT = "popular_count";

	public static final String NOT_POPULAR_COUNT = "not_popular_count";

	public static final String INTERVAL = "interval";

	public static final String HOUR = "hour";

	public static final String DAY = "day";

	public static final String DAY_HOUR = "day_hour";

	public static final String MONTH = "month";

	public static final String MTD = "mtd";
	
	public static final String PREV_30_DAYS = "prev_30_days";

	public static final String COMMENT_ID = "comment_id";

	public static final String STATUS_TYPE = "status_type";

	public static final String SHARES = "shares";

	public static final String CREATED_DATETIME = "created_datetime";

	public static final String CRON_DATETIME = "cron_datetime";

	public static final String BHASKARSTORYID = "bhaskarstoryid";

	public static final String COMPETITOR = "competitor";

	public static final String PAGE_FANS = "page_fans";

	public static final String URL_DOMAIN = "url_domain";

	public static final String PICTURE = "picture";

	public static final String RATING = "rating";

	public static final String FILENAME = "filename";

	public static final String FEEDBACK = "feedback";

	public static final String DEVICEID = "deviceId";

	public static final String POLL_ID = "poll_id";

	public static final String BROWSER_TITLE = "browser_title";

	public static final String GA_SECTION = "ga_section";

	public static final String GA_SECTION1 = "ga_section1";

	public static final Map<String, String> REFHOSTTOREFPLATFORMMAPPING;

	public static final String TOP_RANGE = "top_range";

	public static final String TOP_PROB = "top_prob";

	// DFP fields

	public static final String DOMAIN = "domain";

	public static final String CATEGORY = "category";

	public static final String AD_UNIT_ID = "ad_unit_id";

	public static final String AD_EX_IMPRESSIONS = "ad_ex_impressions";

	public static final String AD_EX_CLICKS = "ad_ex_clicks";

	public static final String AD_EX_REVENUE = "ad_ex_revenue";

	public static final String E_CPM = "e_cpm";

	public static final String ADX_COV = "adx_cov";

	public static final String SESSION = "session";

	public static final String DOMAIN_SESSION = "domain_session";

	public static final String PV = "pv";

	public static final String UV = "uv";

	public static final String PAGE_DEPTH = "page_depth";

	public static final String PAGE_LOAD_TIME = "page_load_time";

	public static final String PAGE_LOAD_SAMPLE = "page_load_sample";

	public static final String BOUNCES = "bounces";

	public static final String UPD = "upd";

	public static final String TOTAL_IMPRESSIONS = "total_impressions";

	public static final String UNFILLED_IMPRESSIONS = "unfilled_impressions";

	public static final String SESS_DURATION = "sess_duration";

	public static final String IMP_STATUS = "imp_status";

	public static final String FLAG_V = "flag_v";

	public static final String A_EVENT = "a_event";

	public static final String FASHION_101_BRAND_ID_ENGLISH = "REC-FASHION-9254";

	public static final String FASHION_101_BRAND_ID_HINDI = "REC-FASHION-9069";

	public static final String QUARTILE = "quartile";

	static {
		Map<String, Integer> aMap = new HashMap<String, Integer>();
		// Custom rule to show stories of last 48 hours for LIVE
		aMap.put("live", -48);
		aMap.put("fifa", -48);
		CUSTOM_RULE_MAP = Collections.unmodifiableMap(aMap);

		Map<String, String> refHostToRefPlatformMapping = new HashMap<>();
		refHostToRefPlatformMapping.put("facebook", "social");
		refHostToRefPlatformMapping.put("google", "search");
		refHostToRefPlatformMapping.put("ucweb", "other");
		refHostToRefPlatformMapping.put("ucnews", "other");
		refHostToRefPlatformMapping.put("bhaskar", "bhaskar");
		refHostToRefPlatformMapping.put("google", "search");
		REFHOSTTOREFPLATFORMMAPPING = Collections.unmodifiableMap(refHostToRefPlatformMapping);
	}

	private Constants() {

	}

	public class WisdomNextConstant {
		public static final String PP_CAT_NAME = "pp_cat_name";
		public static final String PP_CAT_ID = "pp_cat_id";
		public static final String GA_CAT_NAME = "ga_cat_name";
		public static final String TYPE = "type";
		public static final String WIDGET_ID = "widget_id";
		public static final int CALENDAR_LIMIT = 31;
	}

	public class NotificationConstants {
		public static final String DIVYA_NOTIFICATION_INDEX = "divya_notification_detail_";
		public static final String DIVYA_ROW_ID_FILED = "_id";
		public static final String DIVYA_NOTIFICATION_IMPRESSION_FIELD = "impression";//eventype
		public static final String DIVYA_NOTIFICATION_CLICK_FIELD = "click";//eventype
		public static final String DIVYA_NOTIFICATION_IMPRESSION_DATETIME_FIELD = "impression_datetime";//current
		public static final String DIVYA_NOTIFICATION_CLICK_DATETIME_FIELD = "click_datetime";//current
		public static final String DIVYA_NOTIFICATION_SUB_CAT_ID_FIELD = "sub_cat_id";
		public static final String DIVYA_NOTIFICATION_STORY_TYPE_FIELD = "story_type";
		public static final String DIVYA_NOTIFICATION_APP_ID_FIELD = "app_id";//app_id
		public static final String DIVYA_NOTIFICATION_DOMAIN_TYPE_FIELD = "domain_type";//doma
		public static final String DIVYA_NOTIFICATION_WIDGET_NAME_FIELD = "widget_name";//widget name
      
		public static final String NOTIFICATION_TRACKER_PREFIX = "notify";
		public static final String APP_VC_FIELD = "app_vc";
		public static final String MINIMUM_TARGET_SHOULD_MATCH = "1";
		public static final String TARGET_FIELD = "target";
		public static final String RANGE_TARGET_FIELD = "rangeTarget";
		public static final String TARGET_OPERATOR_FIELD = "targetOperator";
		public static final String EXCLUDE_TARGET_FIELD = "excludeTarget";
		public static final String SOURCE_FIELD = "source";
		public static final String PARTNER_FIELD = "follow_partner";
		public static final String FOLLOW_TAG_FIELD = "follow_tag";
		public static final String HOST_FIELD = "host";
		public static final String APP_ID_FIELD = "app_id";
		public static final String AUTO_NOTIFICATION_KEY = "autoNotificationKey";
		public static final String SUBSCRIBED_AT = "subscribedAt";
		public static final String UN_SUBSCRIBED_AT = "unSubscribedAt";
		public static final String DAY_NOTIFICATION_COUNTER = "dayNotificationCounter";
		
		public static final String CAT_ID = "catId";
		public static final String STORY_ID = "storyId";
		public static final String MESSAGE = "message";
		public static final String SLID_KEY = "slidKey";
		public static final String DRY_RUN = "dryRun";

		public class BrowserNotificationConstants {
			public static final String MESSAGE_FIELD = "message";
			public static final String BROWSER_FIELD = "browser";
			public static final String BODY_FIELD = "body";
			public static final String STORY_URL_FIELD = "storyURL";
			public static final String IMAGE_FIELD = "image";
			public static final String ICON_FIELD = "icon";

		}

		public class AutomationConstants {
			public static final int REAL_TIME_WEB_UID = 262;
			public static final int BHASKAR_AUTOMATION_UID = 254;
			public static final int DIVYA_AUTOMATION_UID = 255;
			public static final String AUTO_NOTIFICATION_STORY_HOSTS_FIELD = "automatedNotificationStory.hosts";
		}

		public class AdminPanelConstants {
			public static final String TARGET_KEY = "targetKey";
			public static final String NOTIFICATION_ID = "notificationId";
			public static final String IS_SENT = "isSent";
			public static final String IS_ENABLE = "isEnable";
			public static final String RECIPIENT_COUNT = "recipientCount";
			public static final String NOTIFICATION_TIME = "notificationTime";
			public static final String AUTOMATED_NOTIFICATION_STORY = "automatedNotificationStory";
			public static final String UPDATED_AT = "updatedAt";

		}

	}

	public class Host {
		public static final int BHASKAR_APP_ANDROID_HOST = 16;
		public static final int BHASKAR_MOBILE_WEB_HOST = 15;
		public static final int BHASKAR_WEB_HOST = 1;
		public static final int DBVIDEOS_WEB_HOST = 48;
		public static final int DIVYA_APP_ANDROID_HOST = 22;
		public static final int BHASKAR_APP_IPHONE_HOST = 17;
		public static final int DIVYA_APP_IPHONE_HOST = 23;
		public static final int DIVYA_MOBILE_WEB_HOST = 20;
		public static final int DIVYA_WEB_HOST = 5;
		public static final int MONEY_WEB_HOST = 4;
		public static final int MARATHI_WEB_HOST = 10;
	}

	public class AppId {
		public static final String APP_ID_BHASKAR_ANDROID = "2";
	}

	public final class HostType {
		public static final String WEB = "w";

		public static final String MOBILE = "m";

		public static final String ANDROID = "a";

		public static final String IPHONE = "i";
	}

	public final class CommentStatus {
		public static final int DISABLED = 0;

		public static final int ENABLED = 1;

		public static final int ABUSIVE = 2;
		
		public static final int SPAMS = 10;
		
		public static final int SOFT_DELETE = 5;
	}
	
	public final class Comment {
		
		public static final String SPAM_WORD_ISENABLE = "isEnable";
		
		public static final String SPAM_WORD_STATUS = "status";
		
		public static final String SPAM_WORD = "word";
		
		public static final String SPAMS = "spams";
		
		public static final String FILTERS = "FILTERS";
		
		public static final String FILTER_RULES = "rules";
	}
	
	public static class Cricket{
		
		public static String STAGE = "stage";
		public static String SERIES = "series";
		
		public static class Series{
			
			public static final String NAME = "series_name";
			
			public static final String SHORT_NAME = "series_short_name";
			
			public static String ID = "series_id";
			
		}
		
		public static class Team{
			
			public static final String CUSTOM_NAME = "custom_name";
			
			public static final String CUSTOM_SHORT_NAME = "custom_short_name";
			
			public static final String ID = "team_id";
			
			public static final String NAME = "name";
			
			public static final String SHORT_NAME = "short_name";
			
			public static final String SUPPORT_STAFF = "support_staff";
		}
		
		public static class TeamStanding {
			
			public static final String SPORT_SPECIFIC_KEYS = "sport_specific_keys";
			
			public static final String POSITION = "position";
		}
		
		public static class PredictWinConstants{
			
			public static final int BHASKAR_APP_ANDROID_VENDOR_ID = 1011;

			public static final int BHAKSAR_APP_IOS_VENDOR_ID = 1008;

			public static final int DIVYA_APP_ANDROID_VENDOR_ID = 1010;

			public static final int DIVYA_APP_IOS_VENDOR_ID = 1009;

			public static final int CRICKET_PUNDIT_APP_ANDROID_VENDOR_ID = 1001;

			public static final int CRICKET_PUNDIT_APP_IOS_VENDOR_ID = 1002;

			public static final String VENDOR_TYPE_FIELD = "vendorType";

			public static final String CRICKET_PUNDIT = "cricketPundit";
			
			public static final Integer[] APP_VENDORS_LIST = {BHASKAR_APP_ANDROID_VENDOR_ID, BHAKSAR_APP_IOS_VENDOR_ID, DIVYA_APP_ANDROID_VENDOR_ID, DIVYA_APP_IOS_VENDOR_ID, CRICKET_PUNDIT_APP_ANDROID_VENDOR_ID, CRICKET_PUNDIT_APP_IOS_VENDOR_ID};
			
			public static final String BID_WIN_NOTIFICATION_MESSAGE = "Congrats!You have made the correct prediction";
			
			public static final String ICON_URL = "https://i15.dainikbhaskar.com/web2images/cricpundit/cricketpundit_icon.png";
			
			public static final String COINS_GAINED = "coinsGained";
			
			public static final String RANK = "rank";
			
			public static final String MATCH_ID = "matchId";
			
			public static final String BID_TYPE_ID = "bidTypeId";
			
			public static final String BID_TYPE="bidType";
			
			public static final String BID_TYPE_PLAYER = "player";

			public static final String BID_TYPE_TEAM = "team";

			public static final String BID_TYPE_TOSS = "toss";
			
			public static final String PREDICTION = "prediction";
			
			public static final String IS_BID_ACCEPTED = "isBidAccepted";
			
			public static final String IS_WIN = "isWin";
			
			public static final String REFUND_GEMS = "refundGems";
			
			public static final String USER_ID = "userId";
			
			public static final String TICKET_ID = "ticketId";
			
			public static final String VENDOR_ID = "vendorId";
			
			public static final String P_VENDOR_ID = "pVendorId";
			
			public static final String COINS_BID = "coinsBid";
			
			public static final String CREATED_DATE_TIME = "createdDateTime";
			
			public static final String REFUND_FACTOR = "refundFactor";
			
			public static final String IS_REFUND = "isRefund";
			
			public static final String UPDATED_DATE_TIME = "updatedDateTime";
			
			public static final String PRE_MATCH_BID = "preMatchBid";
			
			public static final String GAIN_FACTOR = "gainFactor";
			
			public static final String BID_WIN_EVENT = "bid_win_event";
			
			public static final String LAZY_BID_NOTIFICATION = "lazy_bid_notification";
			
			public static final String LAZY_NOTIFICATION_MESSAGE = "We missed you! Next game starts in 30 mins";
			
			public static final String TOSS_EVENT = "TOSS";
			
			public static final String[] EVENTS = {TOSS_EVENT};
		}
	}
	
	public static final List<String> INTERSTITIAL_IDS = Arrays.asList("149372688", "149383128", "51223836", "51259116");

	public static final String REPLY_COUNT = "reply_count";

	public static final String LIKE_COUNT = "like_count";

	public static final String REPORT_ABUSE_COUNT = "report_abuse_count";

	public static final String ID = "id";

	public static final String IN_REPLY_TO_COMMENT_ID = "in_reply_to_comment_id";

	public static final String IN_REPLY_TO_USER_ID = "in_reply_to_user_id";

	public static final String COMMENT_EVENT = "event";

	public static final String LIKE = "like";

	public static final String ABUSE = "abuse";

	public static final String POST_ID = "post_id";

	public static final String IS_REPLY = "isReply";

	public static final String STARS = "*****";

	public class CricketConstants {

		public static final String NOTIFICATION_STATUS_FIELD = "notifications_enabled";

		public static final String NOTIFICATION_DETAILS_FIELD = "notification_details";

		public static final String NOTIFICATION_TARGET_FIELD = "target";

		public static final String NOTIFICATION_SL_ID_FIELD = "sl_id";

		public static final String NOTIFICATION_ICON_FIELD = "nicon";

		public static final String NOTIFICATION_FOLLOW_TAG_FIELD = "follow_tag";

		public static final String CRICKET_COMMENT_IS_AUTOMATED = "isAutomated";

		public static final String CRICKET_COMMENT_USER_NAME = "Cricket Guru";

		public static final String FLAG_WIDGET_ARTICLE_PAGE = "widget_article_page";

		public static final String FLAG_WIDGET_SPORTS_ARTICLE_PAGE = "widget_sports_article_page";

		public static final String FLAG_WIDGET_CATEGORY_PAGE = "widget_category_page";

		public static final String FLAG_WIDGET_HOME_PAGE = "widget_home_page";

		public static final String FLAG_WIDGET_GLOBAL = "widget_global";

		public static final String FLAG_GENIUS_HISTORY = "in_genius_history";
		
		public static final String FLAG_WIDGET_EVENT_ENABLED = "widget_event_enabled";

		public static final String MATCH_UPCOMING_OR_LIVE_STATUS = "1";

		public static final String MATCH_STATUS_MATCH_ENDED = "Match Ended";

		public static final String MATCH_STATUS_MATCH_ABANDONED = "Match Abandoned";

		public static final String UPCOMING_FIELD = "upcoming";

		public static final String RECENT_FIELD = "recent";

		public static final String YES = "yes";

		public static final String QUERY_MATCH_ID = "Match_Id";

		public static final String TEAM_INDIA = "India";

		public static final long MATCH_SCHEDULER_TIME_IN_SEC = 7200;
		
		public static final long MATCH_START_SCHEDULER_TIME_IN_HOUR = 1;
		
		public static final long MATCH_STOP_SCHEDULER_TIME_IN_HOUR = 2;

		public static final String COMMENT_ICON_URL = "https://i10.dainikbhaskar.com/cricketscoreboard/image/cricket.jpg";

		public static final String CRICKET_NOTIFICATION_EDITOR = "CricketBot";

		public static final String FOLLOW_PREFIX = "CustomEvent_cricket_";

		public static final String NOTIFICATION_ID_SERVICE_URL = "http://wisdom.bhaskar.com/inHouseNotification/inhouse_notification_dynamic.php";

		public static final String IPL = "Indian Premier League";
		
		public static final String IPL_BHASKAR_IMAGE = "https://i9.dainikbhaskar.com/notification/customimages/ipl2018icon.jpg";
		
		public static final String IPL_DIVYA_BHASKAR_IMAGE = "https://i9.dainikbhaskar.com/notification/customimages/ipl2018icon1.jpg";

		public static final String STATUS_MATCH_ENDED = "Match Ended";
		
		public static final String STATUS_MATCH_ABANDONED = "Match Abandoned";
		
		public static final String DATA = "data";
		
		public class CommentaryType {

			public static final int ALL = 1;

			public static final int CURRENT_OVER = 2;

			public static final int WICKETS = 3;

			public static final int BOUNDARIES = 4;

			public static final int FOURS = 5;

			public static final int SIXES = 6;
		}

		public class Target {

			public static final int BHASKAR = 7;

			public static final int DIVYA_BHASKAR = 6;

			public static final int BHASKAR_WEB = 0;

			public static final int BHASKAR_WAP = 1;

			public static final int BHASKAR_APP = 2;

			public static final int DIVYA_BHASKAR_WEB = 3;

			public static final int DIVYA_BHASKAR_WAP = 4;

			public static final int DIVYA_BHASKAR_APP = 5;
		}

		public class IngestionConstants {

			public static final String INNINGS_FIELD = "Innings";

			public static final String OVERS_FIELD = "Overs";

			public static final String MATCHDETAIL_FIELD = "Matchdetail";

			public static final String TEAM_NAME_FIELD = "Team_Name";

			public static final String SERIES_NAME_FIELD = "Series_Name";

			public static final String WICKETS_FIELD = "Wickets";

			public static final String TOUR_NAME_FIELD = "Tour_Name";

			public static final String TOTAL_RUNS_FIELD = "Total";

			public static final String RESULT_FIELD = "Result";

			public static final String PREMATCH_FIELD = "Prematch";

			public static final String MATCH_DATE_IST_FIELD = "matchdate_ist";

			public static final String MATCH_TIME_IST_FIELD = "matchtime_ist";

			public static final String END_MATCH_DATE_IST_FIELD = "end_matchdate_ist";

			public static final String END_MATCH_TIME_IST_FIELD = "end_matchtime_ist";

			public static final String MATCH_ID_FIELD = "match_Id";

			public static final String TEAM_A = "teama";

			public static final String TEAM_B = "teamb";

			public static final String LIVE = "live";

			public static final String UPCOMING = "upcoming";

			public static final String RECENT = "recent";

			public static final String SERIES_ID = "series_Id";

			public static final String SERIES_SHORT_DISPLAY_NAME = "series_short_display_name";

			public static final String MATCH_NUMBER = "matchnumber";

			public static final String TEAMS = "Teams";

			public static final String COMMENTARY_MATCH_ID = "match_id";

			public static final String COMMENTARY_INNINGS_NUMBER = "inningsNumber";

			public static final String MATCH_DETAILS_NUMBER = "Number";

			public static final String TEAM_HOME_NAME = "Team_Home_Name";

			public static final String TEAM_AWAY_NAME = "Team_Away_Name";

			public static final String WINNING_TEAM_NAME = "Winningteam_Name";

			public static final String WINMARGIN = "Winmargin";

			public static final String FLAG_DIVYA_WEB = "web_divya";

			public static final String FLAG_DIVYA_WAP = "wap_divya";

			public static final String FLAG_DIVYA_APP = "app_divya";

			public static final String FLAG_BHASKAR_WEB = "web_bhaskar";

			public static final String FLAG_BHASKAR_WAP = "wap_bhaskar";

			public static final String FLAG_BHASKAR_APP = "app_bhaskar";
			
			public static final String SERIES_NAME = "seriesname";
			
			public static final String MATCH_STATUS = "matchstatus";
			
			public static final String BATSMEN = "Batsmen";
			
			public static final String BATSMAN = "Batsman";
			
			public static final String RUNS = "Runs";
			
			public static final String BALLS = "Balls";
			
			public static final String FOURS = "Fours";
			
			public static final String SIXES = "Sixes";
			
			public static final String DOTS = "Dots";
			
			public static final String STRIKERATE = "Strikerate";
			
			public static final String HOWOUT = "Howout";
			
			public static final String NO_OF_DISMISSALS = "no_of_dismissals";
			
			public static final String NO_OF_PLAYED_MATCHES = "no_of_played_matches";
			
			public static final String ISENABLED = "isEnabled";
			
			public static final String AVERAGE = "average";
			
			public static final String BATTINGTEAM = "Battingteam";
			
			public static final String TEAM_ID = "team_id";
		}
		
		public class IPLConstants {
//			public static final String BHASKAR_LIVE_SCORE_URL = "https://www.bhaskar.com/sports/ipl-2018/live-score/";
//			public static final String M_BHASKAR_LIVE_SCORE_URL = "https://m.bhaskar.com/sports/ipl-2018/live-score/";
//			public static final String DIVYA_BHASKAR_LIVE_SCORE_URL = "https://www.divyabhaskar.co.in/sports/ipl-2018/live-score/";
//			public static final String M_DIVYA_BHASKAR_LIVE_SCORE_URL = "https://m.divyabhaskar.co.in/sports/ipl-2018/live-score/";
			public static final String BHASKAR_LIVE_SCORE_URL = "https://www.bhaskar.com/cric-score/";
			public static final String M_BHASKAR_LIVE_SCORE_URL = "https://m.bhaskar.com/cric-score/";
			public static final String DIVYA_BHASKAR_LIVE_SCORE_URL = "https://www.divyabhaskar.co.in/cric-score/";
			public static final String M_DIVYA_BHASKAR_LIVE_SCORE_URL = "https://m.divyabhaskar.co.in/cric-score/";
		}
		
		public class PlayerProfile {

			private PlayerProfile() {
				throw new IllegalStateException("Utility class");
			}

			public static final String PROFILE = "profile";

			public static final String BIO = "Bio";

			public static final String PLAYER_ID = "Player_Id";

			public static final String PLAYER_NAME = "Player_Name";

			public static final String PLAYER_NAME_FULL = "Player_Name_Full";

			public static final String LAST_5 = "last_5";

			public static final String PERFORMANCE = "Performance";

			public static final String CAPTAINCY = "captaincy";

			public static final String DESCRIPTION = "Desc";

			public static final String OVERALL = "overall";

			public static final String SERIES_TYPE = "series_type";

			public static final String YEAR = "year";

			public static final String BOWLING = "Bowling";

			public static final String BATTING_AND_FIELDING = "Batting & Fielding";

			public static final String AGAINST = "against";
			
			public static final String STAT_TYPE = "stattype";
			
			public static final String WHERE = "where";
			
			public static final String BAT_PERFORMANCE = "bat_performance";
			
			public static final String BOWL_PERFORMANCE = "bowl_performance";
			
			public static final String MOM = "mom";
		}

		public class SessionTypeConstants {
			public static final String CHANNEL = "channel";
			public static final String AUTHOR = "author";
			public static final String SUPER_CAT_NAME = "super_cat_name";
			public static final String AUTHOR_SUPER_CAT_NAME = "author_super_cat_name";
			public static final String SUPER_CAT_ID = "super_cat_id";
			public static final String AUTHOR_SUPER_CAT_ID = "author_super_cat_id";
			public static final String BROWSER = "browser";
			public static final String NETWORK = "network";
			public static final String TRACKER = "tracker";
			public static final String REF_PLATFORM = "ref_platform";
			public static final String DEVICE = "device";
			public static final String OS = "os";
		}
		
	}
	public static final String ISAGREE = "isAgree";
	public static final String SUPER_CAT_ID = "super_cat_id";
	public static final String SUPER_CAT_NAME = "super_cat_name";
	public static final String FOLLOW_COMMENT = "follow_comment";
	public static final String SHAREABILITY = "shareability";
	public static final String MAX_SHAREABILITY = "max_shareability";
	public static final String MAX_SHARES = "max_shares";
	public static final String NORMALISED_SHAREABILITY = "normalised_shareability";
	public static final String IA_TIMESPENT = "ia_timespent";
	public static final String ACTIVE_USERS = "active_users";
	public static final String USER_COUNT = "user_count";
	public static final String TIME_SPENT = "time_spent";
	public static final String SESSION_TYPE = "session_type";
	public static final String ENGAGEMENT = "engagement";
	public static final String PROB = "prob";
	public static final String CTR = "ctr";
	public static final String POST_IMPRESSIONS_UNIQUE = "post_impressions_unique";
	public static final String POST_IMPRESSIONS_PAID = "post_impressions_paid";
	public static final  List<String> FACEBOOK_TRACKERS = Arrays.asList(
			"instant",
			"fbo",
			"fbo1",
			"fbo10",
			"money",
			"Fbo20",
			"Fbo21",
			"Fbo22",
			"Fbo23",
			"Fbo24",
			"Fbo25",
			"Fbo26",
			"Fbo27",
			"ABC",
			"bgp",
			"fbina",
			"fpaamn",
			"fpaid",
			"fpamn",
			"fpamn1",
			"fpamn6",
			"gpromo",
			"Gr",
			"NJB",
			"NJNM",
			"NJP",
			"NJP1",
			"NJS",
			"nmp",
			"opd",
			"Vpaid",
			"xyz",
			"mnp",
			"mini");
	
	public static final String ABBREVIATIONS = "abbreviation";
	public static final String SPL_TRACKER = "spl_tracker";
	public static final String URL_FOLDERNAME = "url_foldername";
	public static final String IDENTIFIER_VALUE="identifierValue";
	public static final String WITHOUT_UC="withoutUc";
	public static final String WITH_UC="with_UC";
	public static final String CHANNEL_OVERALL="channelOverall";
	public static final String OVERALL="overall";
	public static final String WITHOUT_UC_OVERALL="withoutUcOverall";
	public static final String PUB_DATE ="pub_date";
	public static final String USER_COOKIE_ID ="user_cookie_id";
	public static final String SHORT_NAME = "short_name";
	
	public class PredictNWinConstants {
		public static final String NAME = "name";
	}
	
	public class VideoReportConstants {
		public static final String SESSIONS = "sessions";
		public static final String  UNIQUE_VISITORS = "uniqueVisitors";
		public static final String  DAILY_ACTIVE_USERS = "daily_active_users";
		public static final String  SESSIONS_MTD = "sessionsMTD";
		public static final String  MONTHLY_ACTIVE_USERS = "monthly_active_users";
		public static final String  UNIQUE_VISITORS_PREV_30_DAYS = "uniqueVisitorsPrev30Days";
	}
	
	
	public class LogMessages {

		public static final String PREDICT_WIN_BID_PROCESSING_MESSAGE = "A bid is already being processed. Waiting for 3 sec...";
	}
	
	public class Result{
		public static final String YEAR = "year";
		public static final String STANDARD = "standard";
		public static final String BOARD = "board";
		public static final String ROLL_NO = "rollNo";
		public static final String FIELD = "field";
	}
	
	public static final String NOTIFICATION_IMPPRESSIONS = "notification_impressions";
	
	public static final String NOTIFICATION_CLICKS = "notification_clicks";
	
	public static final String[] SOCIAL_EDITOR_UIDS = new String[] { "187", "265", "275", "276", "928", "350", "451",
			"453", "847", "923", "931", "1056", "1124", "1159", "1168", "1278", "1326", "1339", "1368", "1466", "1473",
			"1503", "1536", "469", "955", "1633", "110", "1201", "1129", "1312", "356" };
}
