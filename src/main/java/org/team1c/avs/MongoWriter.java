// File:         MongoWriter.java
// Author:       Ho Yi Ping, Khai Fung Lim, Fernando Ng and Chong Chiu Gin
// Last Modified Date:  8-June-2020
// 
// Description:  MongoDB writer class is used to insert (file and number of averaged face count) 
//               into MongoDB.

package org.team1c.avs;

import java.util.HashMap;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import org.bson.Document;

/**
 * MongoDB writer class is used to insert data into MongoDB.
 */
public class MongoWriter {
    private String host;
    private int port;
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> collection;

    /**
     * Constructor class to create a new instance of MongoWriter
     * 
     * @param host MongoDB Host
     * @param port MongoDB Port
     * @param database MongoDB Database
     * @param collection MongoDB Collection
     */
    public MongoWriter(String host, int port, String database, String collection) {
        this.host = host;
        this.port = port;
        this.client = new MongoClient(host, port);
        this.db = this.client.getDatabase(database);
        this.collection = db.getCollection(collection);
    }
    /**
     * This method is used to insert file and average faces into the MongoDb database
     * 
     * 
     * @param file String filename that will be inserted into the database
     * @param avgFaces double average faces that will be inserted into the database
     */
    public void insert(String file, double avgFaces) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("file", file);
        map.put("avgFaces", avgFaces);
        Document doc = new Document(map);
        this.collection.insertOne(doc);
    }

}