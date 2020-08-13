// File:         VideoStreamCollector.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  12-June-2020
// 
// Description:  connects to video sources and introduces frames into the system
package org.team1c.avs;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * This video takes in a property file and starts a VideoEventGenerator for each
 * video input specified
 */
public class VideoStreamCollector {

	//get properties of collector
	public static final String PROPERTIES_FP = "./properties/collector.properties";

	public static void main(String[] args) throws Exception {
		Properties collectorProp = Util.getProperties(PROPERTIES_FP);

		// String[] urls = collectorProp.getProperty("camera.url").split(",");
		//split the camera ids by "," to know how many cameras are to be read
		String[] ids = collectorProp.getProperty("camera.id").split(",");
		
		// Create a thread for each video input
		for (int i = 0; i < ids.length; i++) {
			System.out.println("Starting thread for camera: " + ids[i]);
			Thread t = new Thread(new VideoEventGenerator(
				ids[i].trim(),
				new KafkaProducer<String, String>(collectorProp),
				collectorProp.getProperty("kafka.topic"),
				Integer.parseInt(collectorProp.getProperty("camera.retries"))
			));
			t.start();
		}
	}
}
