// File:         VideoStreamArchiver.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  12-June-2020
// 
// Description:  Starts the correct number of VideoStreamWriter based on number of video inputs
package org.team1c.avs;

import java.util.Properties;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.team1c.avs.Util;

/**
 * This class is used to create a VideoStreamWriter for each video input
 * 
 */
public class VideoStreamArchiver {

    // find the file path of the archiver properties
    public static final String PROPERTIES_FP = "./properties/archiver.properties";

    public static void main(String[] args) throws Exception {
        Properties consumerProp = Util.getProperties(PROPERTIES_FP);

        // query kafka cluster for number of cameras via a Kafka consumer
        Consumer<String, String> dummyConsumer = new KafkaConsumer<String, String>(consumerProp);
        dummyConsumer.subscribe(
            Collections.singletonList(consumerProp.getProperty("kafka.topic"))
        );
        // get the camera ids of the kafka topic
        List<Integer> cameraIds = Util.getCameraIds(
            dummyConsumer, 
            consumerProp.getProperty("kafka.topic")
        );

        // create a thread for each video input
        for (int camId : cameraIds) {
            System.out.println("Starting thread for camera: " + Integer.toString(camId));
            Thread t = new Thread(new VideoStreamWriter(
                camId,
                consumerProp
            ));
            t.start();
        }
    }
}