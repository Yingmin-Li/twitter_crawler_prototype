package worker;

import java.io.*;
import java.net.*;
import java.util.*;
import org.json.simple.*;

class TwitterClient {
	private String username;
	private String password;
	private String encodedAuth;
	
	public TwitterClient(String username, String password) {
		this.username = username;
		this.password = password;
		String auth = this.username + ":" + this.password;
		this.encodedAuth = (new sun.misc.BASE64Encoder()).encode(auth.getBytes());
	}
	
	public int getFollowersIDs(int userId, int page, List<Integer> aggregator) {
		int statusCode = 0;
		HttpURLConnection conn = null;
		try {
			URL url = new URL("http://www.twitter.com/followers/ids.json?page=" + page + "&user_id=" + userId);
			conn = (HttpURLConnection)url.openConnection();
			conn.setRequestProperty("Authorization", "Basic " + this.encodedAuth);
			conn.connect();
		
			statusCode = conn.getResponseCode();
			if (statusCode == HttpURLConnection.HTTP_OK) {
				JSONArray array = (JSONArray)JSONValue.parse(new InputStreamReader(conn.getInputStream()));
				if (array != null) {
					for (int i = 0; i < array.size(); i ++) {
						String intString = array.get(i).toString();
						if (intString.length() < 11) {
							Integer followerId = Integer.parseInt(array.get(i).toString());
							aggregator.add(followerId);
						}
					}
				}
			} 
		} catch (IOException e) {
			System.out.println("Code " + statusCode + " for " + userId + ": " + e);
			if (statusCode == 0) e.printStackTrace();
		} catch (Exception e) {
			System.out.println("Unexpected exception with code " + statusCode + " for " + userId + " page " + page + ": " + e);
			e.printStackTrace();
			statusCode = 0;
		} finally {
			if (conn != null) {
				try {
					conn.disconnect();
					conn.getInputStream().close();
				} catch (Exception e) {}
			}
		}
		return statusCode;
	}
}
