package com.hana.hd;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

	private static HDFSUtil _instance = null;
	private FileSystem hdfs;

	public static void setup(String url, String confPath) {
		if (_instance == null) {
			_instance = new HDFSUtil(url, confPath);
		}
	}
	
	public static void close() {
		if (_instance != null){
			try {
				_instance.hdfs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void mkdirs(String path) {
		Path p = new Path(path);
		try {
			if (_instance.hdfs.exists(p)) {
				return;
			} else {
				_instance.hdfs.mkdirs(p);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static boolean get(String remotePath, String localPath){

		Path remote = new Path(remotePath);
		try {
			if (_instance.hdfs.exists(remote))
			{
				_instance.hdfs.copyToLocalFile(false, remote, new Path(localPath), true);
				return true;
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
		
	}
	
	
	public static boolean put(String localPath, String remotePath){
		
		Path remote = new Path(remotePath);
		
		try {
			if (_instance.hdfs.exists(remote)) {
				_instance.hdfs.delete(remote, true);
			}
			
			_instance.hdfs.copyFromLocalFile(new Path(localPath), remote);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static boolean delete(String path) {
		Path p = new Path(path);
		try {
			if (_instance.hdfs.exists(p)) {
				_instance.hdfs.delete(p, true);
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public HDFSUtil(String url, String confPath) {
		Configuration hfconf = new Configuration();

		hfconf.addResource(new Path(confPath + "core-site.xml"));
		hfconf.addResource(new Path(confPath + "hdfs-site.xml"));
		try {
			hdfs = FileSystem.get(URI.create(url), hfconf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}