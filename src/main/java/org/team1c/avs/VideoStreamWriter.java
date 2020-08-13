// File:         VideoStreamWriter.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  12-June-2020
// 
// Description:  This class will create and assign Kafka consumer, consume frames, connect to hdfs
//               and repeatedly: send aggregate data to mongo db, send video files to hdfs, write 
//               frames get number of face count.

package org.team1c.avs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Collections;
import java.util.Base64;
import java.io.IOException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.videoio.VideoWriter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * This class is to write video frames and its data detected to MongoDB and HDFS
 */
public class VideoStreamWriter implements Runnable{
    private int camId;
    private Properties archiverProp;

    private int framesPerBatch = 1;
    private MongoWriter mongoWriter = null;
    private VideoWriter videoWriter = null;
    private HdfsWriter hdfsWriter = null;
    private String localFp = null;
    private String hdfsFp = null;

    // should be commented if OpenCV is not detected
	static { System.loadLibrary(Core.NATIVE_LIBRARY_NAME); }
    // static { System.load("/home/student/opencv_build/opencv/build/lib/libopencv_java420.so"); }
    //static { System.load("E:\\OpenCV_4.1.2\\opencv\\build\\java\\x64\\opencv_java412.dll"); }

    /**
     * constructor method to create a new VideoStreamWriter
     * 
     * @param camId camera id
     * @param archiverProp property file settings of archiver consumer
     */
    public VideoStreamWriter(int camId, Properties archiverProp) {
        this.camId = camId;
        this.archiverProp = archiverProp;
    }


    @Override
    /**
     * This function will run the video stream writer by:
     * - create and assign Kafka consumer
     * - consume frames
     * - connect to MongoDB
     * - connect to HDFS
     * - repeatedly:
     *      + send aggregate data to MongoDB
     *      + send video files to HDFS
     *      + write frames
     *      + get aggregated data (number of face count)
     * 
     * 
     * 
     */
    public void run() {
        // create and assign Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(archiverProp);
        consumer.assign(Collections.singletonList(
            new TopicPartition(archiverProp.getProperty("kafka.topic"), camId)
        ));

        // start consuming frames
        boolean initialized = false;
        int frameCounter = 0;
        int nfaces = 0;
        Gson gson = new Gson();
        String fileName = null;
        
        // connect to mongo db
        mongoWriter = new MongoWriter(
            archiverProp.getProperty("mongodb.host"), 
            Integer.parseInt(archiverProp.getProperty("mongodb.port")),
            archiverProp.getProperty("mongodb.db"),
            archiverProp.getProperty("mongodb.collection")
        );

        // connect to hdfs
        try {
            hdfsWriter = new HdfsWriter();
        } catch (IOException e) {
            System.out.println(e.toString());
            return;
        }
        while (true) {
            //get records from kafka broker
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                //deserialize the JSON data
                JsonObject frame = gson.fromJson(record.value(), JsonObject.class);

                if (!initialized) {
                    //start a new file for new batch
                    System.out.println("Initializing new batch");
                    fileName = generateFileName();
                    localFp = "../" + fileName;
                    hdfsFp = "/" + fileName;
                    videoWriter = getVideoWriter(frame, localFp);
                    framesPerBatch = (int) (
                        frame.get("fps").getAsDouble() * 
                        Long.parseLong(archiverProp.getProperty("accumulate.duration"))
                    );
                    initialized = true;
                    System.out.println("New batch " + localFp);
                    System.out.println("Frames per batch " + framesPerBatch);
                }

                // switch batch
                if (frameCounter >= framesPerBatch) {
                    System.out.println("Terminating batch");

                    // send aggregate data to mongo DB
                    double avgNFaces = (double) nfaces / framesPerBatch;
                    mongoWriter.insert(fileName, avgNFaces);
                    nfaces = 0;

                    // send video file to HDFS
                    videoWriter.release();
                    try {
                        hdfsWriter.send(localFp, hdfsFp);
                    } catch (IOException e) {
                        System.out.println("IO Exception when sending to HDFS " + e.toString());
                    }
                    fileName = generateFileName();
                    localFp = "../" + fileName;
                    hdfsFp = "/" + fileName;
                    videoWriter = getVideoWriter(frame, localFp);

                    frameCounter = 0;
                    initialized = false;  // flag to initialize new batch on next frame
                    System.out.println("Batch Terminated");
                }

                // write frame
                Mat mat = Util.ba2Mat(
                    frame.get("resolutiony").getAsInt(),
                    frame.get("resolutionx").getAsInt(),
                    CvType.CV_8UC3,
                    Base64.getDecoder().decode(frame.get("frame").getAsString())
                );
                videoWriter.write(mat);
                frameCounter++;

                // aggregate data
                nfaces += frame.get("nfaces").getAsInt();
            }
        }
    }


    /**
     * This method i used to generate avi file name "yyyy-MM-dd-HH-mm-ss.avi" 
     * @return String yyyy-MM-dd-HH-mm-ss.avi file name
     */
    public static String generateFileName() {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + ".avi";
    }

    /**
     * This method will get the FPS and resolution from a frame correspond to the filename and 
     * formatted as a VideoWriter class
     * 
     * 
     * @param frame one frame of video input
     * @param fileName String a file name that will be used to store the frame
     * @return VideoWriter
     */
    private static VideoWriter getVideoWriter(JsonObject frame, String fileName) {
        return new VideoWriter(
            fileName,
            VideoWriter.fourcc('M', 'J', 'P', 'G'),
            frame.get("fps").getAsDouble(),
            new Size(
                frame.get("resolutionx").getAsInt(),
                frame.get("resolutiony").getAsInt()
            ),
            true
        );
    }
}
