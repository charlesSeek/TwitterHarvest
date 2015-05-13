package unimelb.edu.au.twitterharvest;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class CouchBase {
	private String ip;
	private String bucketName;
	private Bucket bucket;
	public CouchBase(String ip, String bucketName) {
		super();
		this.setIp(ip);
		this.setBucketName(bucketName);
		Cluster  cluster = CouchbaseCluster.create(ip);
		bucket = cluster.openBucket(bucketName,"");
	}
	public void createDocument(String tweet){
		JsonObject json = JsonObject.fromJson(tweet);
		String id = json.getString("id_str");
		JsonDocument doc = JsonDocument.create(id,json);
		bucket.upsert(doc);
	}
	public void bucketClose(){
		bucket.close();
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	

}
