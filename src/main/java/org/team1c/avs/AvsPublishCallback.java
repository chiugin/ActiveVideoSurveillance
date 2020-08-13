// File:         AvsPublishCallback.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  6-June-2020         
// 
// Description:  -

package org.team1c.avs;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This function is to provide a completion message through asynchronous sending of Kafka producers
 */
public class AvsPublishCallback implements Callback {
    
    private String cameraId;

    
    /**
     * constructor method to create AvsPublishCallback class which store the correspond camera id
     * @param cameraId The camera id to publish callback when sending has completed
     */
    public AvsPublishCallback(String cameraId) {
        super();
        this.cameraId = cameraId;
    }

    @Override
    /**
     * this method is called when message is received to check if there is metadata and exception
     * if there is, there is error and will output to terminal the error information
     * 
     * @param metadata metadata message
     * @param exception exception message
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("No metadata received from publishing frame; cam ID: " + cameraId);
        }
        if (exception != null) {
            exception.printStackTrace();
        }
    }
}