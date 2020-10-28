package com.storm.test.SubmitTopology.controller;

import java.util.Map;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

@Service
public class SubmitTopology {

  @Value("${nimbus.host}")
  String nimbusHost;

  private static final Logger LOGGER = LoggerFactory.getLogger(SubmitTopology.class);


  public String submit(String name, String path) {
    
    TopologyBuilder builder = new TopologyBuilder();
    Config config = new Config();
    
    config.put(Config.NIMBUS_HOST, nimbusHost);
    config.setDebug(true);

    Map stormConfig = Utils.readStormConfig();
    stormConfig.put("nimbus.host", nimbusHost);

    NimbusClient nimbus;
    
    try {
      nimbus = new NimbusClient(stormConfig, nimbusHost, 6627);

      String submittedJar = StormSubmitter.submitJar(stormConfig, path);

      try {
        String jsonConfig = JSONValue.toJSONString(stormConfig);
        nimbus.getClient().submitTopology(name, submittedJar, jsonConfig, builder.createTopology());

      } catch (AlreadyAliveException e) {
        LOGGER.error("An instance of the topology is already running.");
        throw new HttpClientErrorException(HttpStatus.CONFLICT, "Topology already running");
      } catch (InvalidTopologyException e) {
        LOGGER.error("The topology is invalid.");
        throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Topology is invalid");
      } catch (TException e) {
        LOGGER.error("An error occured submitting the topology.");
        throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Exception");
      }
    } catch (TTransportException e) {
      LOGGER.error("There was an error connecting to the Nimbus host node.");
      throw new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "Network error");
    }

    return "Topology successfully submitted";
  }

}
