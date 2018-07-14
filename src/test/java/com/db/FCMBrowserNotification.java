package com.db;
import com.db.common.model.HTTPModel;
import com.db.common.utils.HTTPUtils;
import com.db.notification.v1.model.BrowserNotificationStory;
import com.db.notification.v1.model.NotificationPushResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DB on 07-06-2017.
 */
public class FCMBrowserNotification {

    public static void main(String[] args) throws IOException {
        //---------------code

        ObjectMapper mapper = new ObjectMapper();
        BrowserNotificationStory browserNotificationStory= new BrowserNotificationStory();
        browserNotificationStory.setNtitle("LOL");
        browserNotificationStory.setNbody("LOL");
        browserNotificationStory.setNicon("LOL");
        browserNotificationStory.setEditorId("Nah");

        BrowserNotificationStory.BrowserNotificationOption browserNotificationOption=  browserNotificationStory.new BrowserNotificationOption();
        Map<String, Object> requestMap = new HashMap<>();
        browserNotificationOption.setNrequireInteraction(true);
        browserNotificationOption.setNclickable(true);

        browserNotificationStory.setOption(browserNotificationOption);



        requestMap.put("registration_ids", Arrays.asList("fX5U5SKhq7w:APA91bGZteieqjvR_WTccZLoo5QTQIim6K4yBmj3WDhvtVvbml4iMRJkKdjgUnprzfatrIpV2y8qNThBySyaozqUPNircgYQJ--iXSbFC-B0Y9PRPAgcluD6_6pSKWy9_734CaTLcym-","fOWv4Ctb23A:APA91bGizhkCwnpzccJ5JWF9ZA1Rq34rhEpYOfscD5rNqfPaQa_5iXvq83EjskIk90cxTH-waAOQ4QdZuvwAyFiaFMlA99u_XOvTAJ-tAN0YQxv5gCRp_fxAn1HES5mjP3W1D9jiVSo3"));
requestMap.put("data",browserNotificationStory);
      String API_ACCESS_KEY="AIzaSyBRE4mo9HVxLQESM1eNp44eDCcpNrEIavY";

        HTTPModel httpModel= new HTTPModel();
        Map<String,String> headerMap= new HashMap<>();
        headerMap.put("Authorization", "key="+API_ACCESS_KEY);
        headerMap.put("Content-Type", "application/json");
        headerMap.put("Accept", "application/json");
        headerMap.put("cache-control", "no-cache");
        httpModel.setHeaders(headerMap);
        httpModel.setUrl("https://fcm.googleapis.com/fcm/send");
        httpModel.setBody(requestMap);

        NotificationPushResponse notificationPushResponse = HTTPUtils.post(httpModel, NotificationPushResponse.class);
        System.out.println(notificationPushResponse);

    }

}
