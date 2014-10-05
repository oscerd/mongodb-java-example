package com.ancosen.github.mongodb;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.ancosen.github.mongodb.config.MongoConfig;
import com.ancosen.github.mongodb.config.MongoPropertyReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

public class MongodbTest {

	@Test
	public void connection1() throws IOException{
    		MongoClient mongoClient;
			MongoPropertyReader reader = new MongoPropertyReader("config.properties");
			Properties properties = reader.getProperties();
			org.junit.Assert.assertNotNull(properties);
			MongoConfig config = new MongoConfig(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port")));
			mongoClient = new MongoClient( config.getMongoHost(), config.getMongoPort() );
	    	DB db = mongoClient.getDB( "mydb" );
	    	org.junit.Assert.assertNotNull(db);
	    	db.dropDatabase();
	    	mongoClient.close();
	}
	
	@Test
	public void collections() throws IOException{
    	MongoClient mongoClient;
			MongoPropertyReader reader = new MongoPropertyReader("config.properties");
			Properties properties = reader.getProperties();
			org.junit.Assert.assertNotNull(properties);
			MongoConfig config = new MongoConfig(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port")));
			mongoClient = new MongoClient( config.getMongoHost(), config.getMongoPort() );
	    	DB db = mongoClient.getDB( "mydb" );
	    	org.junit.Assert.assertNotNull(db);
	    	
	    	Set<String> colls = db.getCollectionNames();
	    	
	    	org.junit.Assert.assertEquals(0, colls.size());

	    	db.dropDatabase();
	    	mongoClient.close();
	}
	
	@Test
	public void collectionInsert() throws IOException{
    	MongoClient mongoClient;
			MongoPropertyReader reader = new MongoPropertyReader("config.properties");
			Properties properties = reader.getProperties();
			org.junit.Assert.assertNotNull(properties);
			MongoConfig config = new MongoConfig(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port")));
			mongoClient = new MongoClient( config.getMongoHost(), config.getMongoPort() );
	    	DB db = mongoClient.getDB( "mydb" );
	    	org.junit.Assert.assertNotNull(db);
	    	
	        BasicDBObject document = new BasicDBObject("author", "andrea")
	        .append("text", "Pippo")
	        .append("date", new Date());
	        
	        BasicDBObject document1 = new BasicDBObject("author", "peppe")
	        .append("text", "Pluto")
	        .append("date", new Date());
	        
	        BasicDBObject document2 = new BasicDBObject("author", "joseph")
	        .append("text", "Paperino")
	        .append("date", new Date());
	        	    	
	        DBCollection coll =  db.getCollection("pippo");
	        
	    	WriteResult p = coll.insert(document);
	    	
	    	ObjectId id = (ObjectId)document.get( "_id" );
	    	
	    	p = coll.insert(document1);
	    	
	    	id = (ObjectId)document.get( "_id" );
	    	
	    	p = coll.insert(document2);
	    	
	    	id = (ObjectId)document.get( "_id" );
	    	
	        Set<String> colls = db.getCollectionNames();
	    	
	    	org.junit.Assert.assertEquals(2, colls.size());
	    	
	    	DBCursor cursor = coll.find();
	    	org.junit.Assert.assertEquals(3, cursor.size());
	    	
	    	BasicDBObject query = new BasicDBObject("author", "peppe")
	    	.append("text", "Pluto");
	    	
	    	cursor = coll.find(query);
	    	org.junit.Assert.assertEquals(1, cursor.size());
	    	
	    	coll.drop();
	    	db.dropDatabase();
	    	mongoClient.close();
	}
}
