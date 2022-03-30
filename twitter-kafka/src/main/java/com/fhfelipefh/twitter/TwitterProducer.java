package com.fhfelipefh.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

  public TwitterProducer() throws IOException {}

  public static void main(String[] args) throws IOException {
      new TwitterProducer().run();
  }

  public void run() {
    //create a twitter client
    Client hosebirdClient = createTwitterClient();
    // Attempts to establish a connection.
    hosebirdClient.connect();

    // create a kafka producer


    // loop to send tweets to kafka
  }

  private String readFileToGetKeys(String fileName) throws IOException {
    try {
    return Files.readAllLines(new File(fileName).toPath()).get(0);
    } catch (IOException e) {
      throw new IOException("Error reading file " + fileName, e);
    }
  }

  String consumerKey = readFileToGetKeys("resources/consumerKey.txt");
  String consumerSecret = readFileToGetKeys("resources/consumerSecret.txt");
  String token = readFileToGetKeys("resources/token.txt");
  String secret = readFileToGetKeys("resources/secret.txt");

  public Client createTwitterClient() {
    // create a client
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<String> terms = Lists.newArrayList("kafka", "api");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

}
