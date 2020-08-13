// File:         VideoStreamProcessor.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  12-June-2020
// 
// Description:  performs real time video processing on image frames

package org.team1c.avs;

import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.List;

import java.io.IOException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.opencv.core.CvType;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.concurrent.TimeUnit;

/**
 * This class performs real time video processing on image frames received from Kafka
 */
public class VideoStreamProcessor {
	
	// load OpenCV libraries
	// Use NATIVE_LIBRARY_NAME if it is available for your machine, otherwise load the library 
	// directly
	static { System.loadLibrary(Core.NATIVE_LIBRARY_NAME); }
	// static { System.load("/home/ubuntu/opencv/opencv-3.4/build/lib/libopencv_java3410.so"); }
	// static { System.load("E:\\OpenCV_4.1.2\\opencv\\build\\java\\x64\\opencv_java412.dll"); }

	// public static final String HAAR_CASCADE_FP = 
	// 	"E:\\OpenCV_4.1.2\\opencv\\sources\\data\\haarcascades\\haarcascade_frontalface_alt.xml";
	//Haar Cascade Frontal Face
	public static final String HAAR_CASCADE_FP = "/home/student/opencv_build/opencv/data/haarcascades/haarcascade_frontalface_alt.xml";

	//get consumer and producer property files
	public static final String CONSUMER_PROPERTIES_FP = "./properties/processor-consumer.properties";
	public static final String PRODUCER_PROPERTIES_FP = "./properties/processor-producer.properties";

	public static void main(String[] args) throws Exception {
		try {
			// create Kafka consumer
			Properties consumerProp = Util.getProperties(CONSUMER_PROPERTIES_FP);
			Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProp);
			consumer.subscribe(Collections.singletonList(consumerProp.getProperty("kafka.topic")));

			// create Kafka producer
			Properties producerProp = Util.getProperties(PRODUCER_PROPERTIES_FP);
			Producer<String, String> producer = new KafkaProducer<String, String>(producerProp);

			// start processing
			processFrames(consumer, producer, producerProp.getProperty("kafka.topic"));
		} catch (IOException e) {
			System.out.println("Error reading property files");
		}
	}

	/**
	 * This method is to process video frames based on the image processing algorithm
	 * 
	 * 
	 * @param consumer Kafka Consumer
	 * @param producer Kafka Producer
	 * @param topic Kafka Topic
	 */
	public static void processFrames(Consumer<String, String> consumer, 
		Producer<String, String> producer, String topic) throws Exception {
		Gson gson = new Gson();
		CascadeClassifier faceCascade = new CascadeClassifier();
		faceCascade.load(HAAR_CASCADE_FP);
		long prevLatency = 0;
		int delay = 0;
		while (true) {
			//get consumer records from Kafka
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : consumerRecords) {
				// extract frame from record
				String cameraId = record.key();
                JsonObject obj = gson.fromJson(record.value(), JsonObject.class);
				int resolutionx = obj.get("resolutionx").getAsInt();
				int resolutiony = obj.get("resolutiony").getAsInt();
				int channels = obj.get("channels").getAsInt();
				double fps = obj.get("fps").getAsDouble();
				long initTime = obj.get("initTime").getAsLong();
				byte[] byteArray = Base64.getDecoder().decode(obj.get("frame").getAsString());
				Mat mat = Util.ba2Mat(resolutiony, resolutionx, CvType.CV_8UC3, byteArray);
				System.out.printf("New Frame: CamID: %s\n", cameraId);

				// run image processing to get data on frame
				Long processTime = System.currentTimeMillis();
				int nfaces = processFrame(mat, faceCascade);
				processTime = System.currentTimeMillis() - processTime;

				// create and populate JSON object
				obj = new JsonObject();
				mat.get(0, 0, byteArray);
				obj.addProperty("frame", Base64.getEncoder().encodeToString(byteArray));
				obj.addProperty("nfaces", nfaces);
				obj.addProperty("resolutionx", resolutionx);
				obj.addProperty("resolutiony", resolutiony);
				obj.addProperty("channels", channels);
				obj.addProperty("fps", fps);
				obj.addProperty("initTime", initTime);
				obj.addProperty("procTime", processTime);
				obj.addProperty("postProcTime", System.currentTimeMillis());

				// serialize JSON object to string
				String serialized = gson.toJson(obj);

				// publish processed frame to Kafka
				System.out.println("Republishing frame");
				producer.send(
					new ProducerRecord<String, String>(topic, cameraId, serialized),
					new AvsPublishCallback(cameraId)
				);
			}
		}
	}


	/**
	 * Performs Haar Cascade face detection on the given image using the given classifier. Draws 
	 * green rectangles over detected faces. Returns the number of faces detected.
	 * @param mat image frame to process
	 * @param faceCascade cascade classifier to use
	 * @return number of matches detected
	 */
	public static int processFrame(Mat mat, CascadeClassifier faceCascade) {
		// downsize image for faster processing
		Mat grayFrame = new Mat();
		Size downsize = new Size(240, 160);
		Imgproc.cvtColor(mat, grayFrame, Imgproc.COLOR_BGR2GRAY);
		Imgproc.resize(grayFrame, grayFrame, downsize);
		Imgproc.equalizeHist(grayFrame, grayFrame);

		//detect faces
		MatOfRect faces = new MatOfRect();
		faceCascade.detectMultiScale(grayFrame, faces, 1.1, 1, 0, new Size(), new Size());

		// draw rectangles over detected faces
		List<Rect> listOfFaces = faces.toList();
		for (Rect face: listOfFaces){
			Point topleft = new Point(face.tl().x * 3, face.tl().y * 3);
			Point bottomright = new Point(face.br().x * 3, face.br().y * 3);
			Imgproc.rectangle(mat, topleft, bottomright, new Scalar(0,255,0), 3);
		}
		return listOfFaces.size(); //number of faces
	}
}

