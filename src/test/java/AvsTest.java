import org.junit.Test;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.objdetect.CascadeClassifier;
import org.team1c.avs.Util;
import org.team1c.avs.VideoStreamProcessor;
import org.team1c.avs.VideoStreamWriter;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Properties;
import org.opencv.core.Core;

import static org.junit.Assert.*;

public class AvsTest {
    // static { System.load("E:\\OpenCV_4.1.2\\opencv\\build\\java\\x64\\opencv_java412.dll"); }
    //static { System.loadLibrary(Core.NATIVE_LIBRARY_NAME); }
    static { System.load("/home/student/opencv_build/opencv/build/lib/libopencv_java420.so"); }


    //VideoStreamProcessor
    @Test
    public void processFrameTest1() {
        CascadeClassifier faceCascade = new CascadeClassifier();
        faceCascade.load("/home/student/opencv_build/opencv/data/haarcascades/haarcascade_frontalface_alt.xml");

        Mat familyPhoto = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/family.jpg");
        int expectedFaces = VideoStreamProcessor.processFrame(familyPhoto,faceCascade);
        assertEquals(expectedFaces, 4);
    }

    @Test
    public void processFrameTest2() {
        CascadeClassifier faceCascade = new CascadeClassifier();
        faceCascade.load("/home/student/opencv_build/opencv/data/haarcascades/haarcascade_frontalface_alt.xml");

        Mat applePhoto = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/apple.jpg");
        int expectedFaces = VideoStreamProcessor.processFrame(applePhoto,faceCascade);
        assertEquals(expectedFaces, 0);

    }





    @Test
    public void getPropertiesTest1() throws IOException {
        String filePath = "/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/test1.properties";
        Properties actualProperties = Util.getProperties(filePath);
        assertEquals(actualProperties.getProperty("camera.resolutionx"),"720");
        assertEquals(actualProperties.getProperty("camera.resolutiony"),"480");
        assertNotEquals(actualProperties.getProperty("camera.fps"),"");
        assertNotEquals(actualProperties.getProperty("camera.nchannels"),"100");
    }

    @Test
    public void getPropertiesTest2() throws IOException {
        String filePath = "/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/test2.properties";
        Properties actualProperties = Util.getProperties(filePath);
        assertEquals(actualProperties.getProperty("camera.fps"),"24.0");
        assertEquals(actualProperties.getProperty("camera.resolutionx"),"");
        assertNotEquals(actualProperties.getProperty("camera.resolutiony"),"00");
        assertNotEquals(actualProperties.getProperty("camera.nchannels"),"");
    }





    @Test
    public void getCameraPropertiesTest() throws IOException {
        String camID = "0";
        Properties actualProperties = Util.getCameraProperties(camID);
        assertEquals(actualProperties.getProperty("camera.resolutionx"),"720");
        assertEquals(actualProperties.getProperty("camera.resolutiony"),"480");
        assertNotEquals(actualProperties.getProperty("camera.fps"),"");
        assertNotEquals(actualProperties.getProperty("camera.nchannels"),"100");
    }




    @Test
    public void ba2MatTest1(){
        Mat expected = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/family.jpg");
        //HighGui.namedWindow("image", HighGui.WINDOW_AUTOSIZE);
        //HighGui.imshow("image", imageMat);
        //HighGui.waitKey();
        byte[] bytesArr = new byte[expected.channels() * expected.cols() * expected.rows()];
        expected.get(0,0,bytesArr);
        Mat actual = Util.ba2Mat(expected.rows(),expected.cols(), CvType.CV_8UC3,bytesArr);
        assertEquals(actual.rows(), 720);
        assertEquals(actual.cols(), 1280);
        assertEquals(actual.type(), 16);
    }


    @Test
    public void ba2MatTest2(){
        Mat expected = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/apple.jpg");
        //HighGui.namedWindow("image", HighGui.WINDOW_AUTOSIZE);
        //HighGui.imshow("image", imageMat);
        //HighGui.waitKey();
        byte[] bytesArr = new byte[expected.channels() * expected.cols() * expected.rows()];
        expected.get(0,0,bytesArr);
        Mat actual = Util.ba2Mat(expected.rows(),expected.cols(), CvType.CV_8UC3,bytesArr);
        assertEquals(actual.rows(), 1515);
        assertEquals(actual.cols(), 1600);
        assertEquals(actual.type(), 16);
    }




    @Test
    public void mat2BufferedImageTest1() {
        Mat m = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/family.jpg");
        BufferedImage actual = Util.Mat2BufferedImage(m);
        assertEquals(actual.getHeight(),720);
        assertEquals(actual.getWidth(), 1280);
        assertEquals(actual.getType(), 5);

    }

    @Test
    public void mat2BufferedImageTest2() {
        Mat m = Imgcodecs.imread("/home/student/Desktop/FIT3162-Team-1C/avs/src/test/java/apple.jpg");
        BufferedImage actual = Util.Mat2BufferedImage(m);
        assertEquals(actual.getHeight(),1515);
        assertEquals(actual.getWidth(), 1600);
        assertEquals(actual.getType(), 5);

    }


    @Test
    public void isNumericTest1(){
        String s = "100";
        boolean actualOutcome = Util.isNumeric(s);
        assertTrue(actualOutcome);
    }

    @Test
    public void isNumericTest2(){
        String s = "abc";
        boolean actualOutcome = Util.isNumeric(s);
        assertFalse(actualOutcome);
    }

    @Test
    public void isNumericTest3(){
        String s = null;
        boolean actualOutcome = Util.isNumeric(s);
        assertFalse(actualOutcome);
    }



    //VideoStreamWriter
    @Test
    public void generateFileNameTest1() {
        String output = VideoStreamWriter.generateFileName();
        boolean test1 = output.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}.avi");
        assertTrue(test1);
    }

    @Test
    public void generateFileNameTest2() {
        String output = VideoStreamWriter.generateFileName();
        boolean test2 = output.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}");
        assertFalse(test2);
    }

    @Test
    public void generateFileNameTest3() {
        String output = VideoStreamWriter.generateFileName();
        boolean test3 = output.matches("\\d{4}-\\d{2}-\\d{2}-\\d{4}-\\d{2}-\\d{2}.avi");
        assertFalse(test3);
    }


}
