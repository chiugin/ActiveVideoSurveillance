// File:         VideoEventGenerator.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  12-June-2020
// 
// Description:  -

package org.team1c.avs;

import java.sql.Timestamp;
import java.util.Base64;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.concurrent.TimeUnit;

/**
 * This class is to generate events and push frames into Kafka broker by reading frames of input
 */
public class VideoEventGenerator implements Runnable {
	private String cameraId;
	private Producer<String, String> producer;
	private String topic;
	private int cameraRetries;

	/**
	 * Constructor method to create a new VideoEventGenerator
	 * 
	 * @param cameraId Camera Id of the video input
	 * @param producer Kafka Producer
	 * @param topic Kafka Topic
	 * @param cameraRetries Camera Retries number
	 */
	public VideoEventGenerator(String cameraId, Producer<String, String> producer, String topic,
		int cameraRetries) {
		this.cameraId = cameraId;
		this.producer = producer;
		this.topic = topic;
		this.cameraRetries = cameraRetries;
	}

	// load OpenCV libraries
	// Use NATIVE_LIBRARY_NAME if it is available for your machine, otherwise load the library 
	// directly
	static { System.loadLibrary(Core.NATIVE_LIBRARY_NAME); }
	// static { System.loadLibrary("opencv_videoio_ffmpeg412_64"); }
	// static { System.load("/home/student/opencv_build/opencv/build/lib/libopencv_java420.so"); }
	// static { System.load("E:\\OpenCV_4.1.2\\opencv\\build\\java\\x64\\opencv_java412.dll"); }
	// static { System.load("E:\\OpenCV_4.1.2\\opencv\\build\\bin\\opencv_videoio_ffmpeg412_64.dll"); }
	

	/**
	 * Overriden method for running as a thread.
	 */
	@Override
	public void run() {
		try {
			generateEvent();  //if input can be read, send data
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}


	/**
	 * Main function for video event generating. Reads frames from the given camera url
	 * @throws Exception Throw exception when camera cannot be opened 
	 */
	private void generateEvent() throws Exception {
		// extract properties
		Properties cameraProp = Util.getCameraProperties(cameraId);
		String url = cameraProp.getProperty("camera.url");
		int resolutionx = Integer.parseInt(cameraProp.getProperty("camera.resolutionx"));
		int resolutiony = Integer.parseInt(cameraProp.getProperty("camera.resolutiony"));
		int nChannels = Integer.parseInt(cameraProp.getProperty("camera.nchannels"));
		double fps = Double.parseDouble(cameraProp.getProperty("camera.fps"));

		// attempt to open camera
		VideoCapture camera = new VideoCapture();
		int attempts = 0;
		while (attempts < cameraRetries) {
			if (Util.isNumeric(url)) {  // camera is a local camera (e.g. USB webcam)
				camera.open(Integer.parseInt(url));
			} else {  // camera is an IP camera
				camera.open(url);
			}
			if (camera.isOpened()) {
				attempts += cameraRetries; //attempts used to open camera
			} else {
				attempts++;
			}
		}
		if (!camera.isOpened()) {
			//if camera cannot be opened after too much retries
			throw new Exception("Error opening camera: " + cameraId + " url: " + url + 
			" check file path or url in property files");
		}
		
		Mat mat = new Mat();
		Gson gson = new Gson();
		while (camera.read(mat)) {
			// resize image to configured size
			Imgproc.resize(
				mat,
				mat,
				new Size(resolutionx, resolutiony),
				0,
				0,
				Imgproc.INTER_CUBIC
			);

			// create byte array from mat object
			byte[] data = new byte[(int) (resolutionx * resolutiony * nChannels)];
			mat.get(0, 0, data);

			// form JSON object for frame
			JsonObject obj = new JsonObject();
			obj.addProperty("frame", Base64.getEncoder().encodeToString(data));
			obj.addProperty("resolutionx", resolutionx);
			obj.addProperty("resolutiony", resolutiony);
			obj.addProperty("channels", nChannels);
			obj.addProperty("fps", fps);
			obj.addProperty("initTime", System.currentTimeMillis());

			// serialize JSON object to string
			String serialized = gson.toJson(obj);
			
			// publish video frame to Kafka
			producer.send(
				new ProducerRecord<String, String>(topic, cameraId, serialized), 
				new AvsPublishCallback(cameraId)
			);
			
			TimeUnit.MILLISECONDS.sleep(32);  //artificial delay
		}
		camera.release();
		mat.release();
		System.out.println("Camera closed");
	}
}
