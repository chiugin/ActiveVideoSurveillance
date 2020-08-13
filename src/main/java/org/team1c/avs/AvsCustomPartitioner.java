// File:         AvsCustomAssignor.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  10-June-2020
// 
// Description:  Custom Partitioner

package org.team1c.avs;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * Partitioner class to partition video inputs based on their ids
 */
public class AvsCustomPartitioner implements Partitioner {

  public AvsCustomPartitioner() {}

  @Override
  /**
   * A function to map the input string key to its unique id
   * @param configs currently not used
   */
  public void configure(Map<String, ?> configs) {}

  @Override
  /**
   * This function will return an integer that will determine which partition will
   * this message occupy based on the key of the message
   *
   * @param topic Kafka message topic
   * @param key Kafka message key
   * @param keyBytes Kafka message key in bytes
   * @param value Kafka message load
   * @param valueBytes Kafka message load in bytes
   * @param cluster Kafka cluster
   * @return Unique integer id of partition used
   */
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {
    return Integer.parseInt((String)key);
  }


  @Override
  /**
   * Close the paritioner
   */
  public void close() {}

}