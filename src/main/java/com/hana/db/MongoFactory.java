package com.hana.db;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoFactory {

	private MongoClient client;
	private String host = "localhost";

	private static MongoFactory _instance;

	static boolean intialize(String host) {

		if (_instance == null) {
			try {
				_instance = new MongoFactory(host);
				return true;
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return false;

	}

	static boolean intialize() {

		if (_instance == null) {
			try {
				_instance = new MongoFactory();
				return true;
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return false;

	}

	public static DBCollection getColl(String dbname, String collname) {
		return _instance._getColl(dbname, collname);
	}

	private DBCollection _getColl(String dbname, String collname) {
		DB db = client.getDB(dbname);
		return db.getCollection(collname);
	}

	private MongoFactory() throws UnknownHostException {
		client = new MongoClient(this.host);
	}

	private MongoFactory(String host) throws UnknownHostException {
		this.host = host;
		client = new MongoClient(this.host);
	}
}