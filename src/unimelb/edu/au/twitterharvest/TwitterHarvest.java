/**
 * The TwitterHarvest program implements an application to retrieve
 * tweets, which are sent by Australian users, by using Twitter API
 * and store all the tweets into Couchbase database
 * @author shucheng cui
 * @ version 1.0
 * @since 2014-10-01
 */
package unimelb.edu.au.twitterharvest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.Query.Unit;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;

public class TwitterHarvest {
	public static void main(String[] args) throws InterruptedException{
		if (args.length != 2){
			System.out.println("please input the Australian city name and couchbase ip address");
			System.exit(-1);
		} else {
			while (true) {
				harvest(args[0],args[1]);
				Thread.sleep(5000);
			}
			
		}
	}
	/**
	 * This method is used to retrieve the tweets what happened in Australian
	 * @param JsonFile this is the json file, which store the tweets
	 * @param cityName this is the city name that the tweets are retrieved from
	 * main cities include: Sydney,Melbourne,Brisbane,Perth and Adelaide
	 */
	public static void harvest(String cityName,String ip){
		double latitude = 0;
		double longitude = 0;
		Twitter twitter = new TwitterFactory().getInstance();
		Query query = new Query();
		if (cityName.toLowerCase().equals("sydney")){
			latitude = -33.8675;
			longitude = 151.2069;
		}
		if (cityName.toLowerCase().equals("melbourne")){
			latitude = -37.8216;
			longitude = 144.9785;
		}
		if (cityName.toLowerCase().equals("brisbane")){
			latitude = -27.4710;
			longitude = 153.0234;
		}
		if (cityName.toLowerCase().equals("adelaide")){
			latitude = -34.9286;
			longitude = 138.5999;
		}
		if (cityName.toLowerCase().equals("perth")){
			latitude = -31.9535;
			longitude = 115.8570;
		}
		QueryResult result;
		new File("data").mkdir();
		GeoLocation location = new GeoLocation(latitude,longitude);
		Unit unit = Query.Unit.valueOf("km");
		query.setGeoCode(location, 40.0, unit);
		query.setCount(100);
		query.setLang("en");
		try {
			result = twitter.search(query);
			List<Status> tweets = result.getTweets();
			CouchBase couchbase = new CouchBase(ip,cityName);
			for (Status tweet:tweets){
				String rawJson = TwitterObjectFactory.getRawJSON(tweet);
				//System.out.println(rawJson);
				couchbase.createDocument(rawJson);
				writeJsonToFile(rawJson,"data/"+cityName+".json");
			}
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void writeJsonToFile(String tweet,String file){
		BufferedWriter bw = null;
		try {
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)));
				bw.write(tweet+"\n");
				bw.close();
		} catch (IOException e){
			e.printStackTrace();
		}
		
	}
}
