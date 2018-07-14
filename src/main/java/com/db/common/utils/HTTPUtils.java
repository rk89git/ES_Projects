package com.db.common.utils;

import com.db.common.model.HTTPModel;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by DB on 08-06-2017.
 */
public class HTTPUtils {
    static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static <T> T post(HTTPModel httpModel, Class<T> clazz) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(httpModel.getUrl());
        Map<String, String> headerMap = httpModel.getHeaders();
        Set<Map.Entry<String, String>> headerMapEntrySet = headerMap.entrySet();
        for (Map.Entry<String, String> headerMapEntry : headerMapEntrySet)
            post.setHeader(headerMapEntry.getKey(), headerMapEntry.getValue());
        post.setEntity(new StringEntity(gson.toJson(httpModel.getBody()), Charset.forName("UTF-8")));
        HttpResponse response = client.execute(post);
        T data = gson.fromJson(new InputStreamReader(response.getEntity().getContent()), clazz);
        return data;
    }

    public static String post(HTTPModel httpModel) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(httpModel.getUrl());
        Map<String, String> headerMap = httpModel.getHeaders();
        Set<Map.Entry<String, String>> headerMapEntrySet = headerMap.entrySet();
        for (Map.Entry<String, String> headerMapEntry : headerMapEntrySet)
            post.setHeader(headerMapEntry.getKey(), headerMapEntry.getValue());
        post.setEntity(new StringEntity(gson.toJson(httpModel.getBody()), Charset.forName("UTF-8")));
        HttpResponse response = client.execute(post);
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder output = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            output.append(line);
        }
        return output.toString();

    }

    public static String get(HTTPModel httpModel) throws IOException, URISyntaxException {
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder uriBuilder = new URIBuilder(httpModel.getUrl());
        Map<String, String> parameterMap = httpModel.getParameters();
        Set<Map.Entry<String, String>> parameterMapEntrySet = parameterMap.entrySet();
        for (Map.Entry<String, String> parameterMapEntry : parameterMapEntrySet)
            uriBuilder.addParameter(parameterMapEntry.getKey(), parameterMapEntry.getValue());
        HttpGet get = new HttpGet(uriBuilder.build());
        Map<String, String> headerMap = httpModel.getHeaders();
        Set<Map.Entry<String, String>> headerMapEntrySet = headerMap.entrySet();
        for (Map.Entry<String, String> headerMapEntry : headerMapEntrySet)
            get.setHeader(headerMapEntry.getKey(), headerMapEntry.getValue());
        HttpResponse response = client.execute(get);
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder output = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            output.append(line);
        }
        return output.toString();

    }

    public static Future<String> getASync(final HTTPModel httpModel) throws IOException, URISyntaxException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return get(httpModel);
            }
        });
        executorService.shutdown();
        return future;
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        HTTPModel model = new HTTPModel();
        model.setUrl("http://i9.dainikbhaskar.com/thumbnail/360x240/web2images/www.bhaskar.com/2017/08/11/nihalani_1502489008.jpg");
        System.out.println(get(model));

    }
}
