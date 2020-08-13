// File:         Util.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  6-June-2020         
// 
// Description:  Collection of miscellaneous utility functions that is used frequently.

package org.team1c.avs;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Properties;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.opencv.core.Mat;
import org.opencv.core.CvType;

/**
 * Collection of miscellaneous utility functions that is used frequently.
 */
public class Util {

    //get camera property file
    public static final String CAMERA_PROPERTIES_BASE_FP = "./properties/cameras/";

    /**
     * Reads a properties file and returns a Java properties object.
     * @param filePath path to properties file
     * @return properties object
     */
    public static Properties getProperties(String filePath) throws IOException {
        InputStream inputStream = new FileInputStream(filePath);
        Properties prop = new Properties();
        prop.load(inputStream);
        return prop;
    }


    /**
     * Reads and returns the properties file for the given camera ID.
     * @param cameraId camera ID to be read
     * @return properties object
     */
    public static Properties getCameraProperties(String cameraId) throws IOException {
        Properties prop = getProperties(CAMERA_PROPERTIES_BASE_FP + cameraId + ".properties");
        return prop;
    }


    /**
     * Queries the given consumer's Kafka cluster to get the number of cameras/partitions there 
     * are.
     * Note: Camera IDs are equivalent to partition numbers.
     * @param consumer Kafka consumer to query with
     * @param topic Kafka topic to query for
     * @return list of integers representing the list of camera ids/partitions
     */
    public static List<Integer> getCameraIds(Consumer<?, ?> consumer, String topic) {
        List<Integer> cameraIds = consumer.partitionsFor(
            topic
        ).stream().map(p -> p.partition()).collect(Collectors.toList());
        return cameraIds;
    }


    /**
    * Creates a Mat object from a byte array.
    * @param rows number of rows
    * @param cols number of columns
    * @param type CvType of the image, CV_8UC1, CV_8UC2, etc
    * @param byteArray byte array containing pixel values
    * @return Mat object representing image of the given byte array
    */
    public static Mat ba2Mat(int rows, int cols, int type, byte[] byteArray) {
        Mat mat = new Mat(rows, cols, type);
        mat.put(0, 0, byteArray);
        return mat;
    }


    /**
     * Converts an OpenCV Mat object to a BufferedImage.
     * @param m OpenCV Mat object
     * @return BufferedImage object containing image from given Mat
     */
    public static BufferedImage Mat2BufferedImage(Mat m) {
	    int type = BufferedImage.TYPE_BYTE_GRAY;
	    if (m.channels() > 1) {
	        type = BufferedImage.TYPE_3BYTE_BGR;
	    }
	    int bufferSize = m.channels() * m.cols() * m.rows();
	    byte [] b = new byte[bufferSize];
	    m.get(0, 0, b); // get all the pixels
	    BufferedImage image = new BufferedImage(m.cols(), m.rows(), type);
	    final byte[] targetPixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
	    System.arraycopy(b, 0, targetPixels, 0, b.length);  
	    return image;
	}


    /**
     * Checks if the given string is representing a numeric.
     * @param string string to check
     * @return boolean result
     */
    public static boolean isNumeric(String string) {
	    if (string == null) {
	        return false;
	    }
	    try {
	        Double.parseDouble(string);  // attempt to parse
	    } catch (NumberFormatException e) {
	        return false;
	    }
	    return true;
	}
}
