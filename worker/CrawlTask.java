package worker;

import common.*;
import java.util.*;

public class CrawlTask implements Runnable {
	private int twitterId;
	private CrawlResult result = null;
	private String username;
	private String password;
	private volatile boolean crawled = false;
	private volatile boolean finished = false;
	private int failCount = 0;
	
	private int MAX_FAILS = 8;
	private int PAGE_SIZE = 5000;
	
	public CrawlTask(int twitterId, String username, String password) {
		this.twitterId = twitterId;
		this.username = username;
		this.password = password;
	}
	
	public int getTwitterId() { return this.twitterId; }
	public CrawlResult getResult() { return this.result; }
	public boolean isFinished() { return this.finished; }
	
	public void run() {		
		TwitterClient client = new TwitterClient(this.username, this.password);
		int page = 1;
		LinkedList<Integer> idList = new LinkedList<Integer>();
		
		// Not finished before this point
		try {
			while (!this.crawled) {
				if (this.failCount > MAX_FAILS) {
					this.result = new CrawlResult(this.twitterId, ResultCode.FAILED);
					this.crawled = true;
				} else {
					LinkedList<Integer> followersPage = new LinkedList<Integer>();
					int responseCode = client.getFollowersIDs(this.twitterId, page, followersPage);
					if (responseCode == 200) {
						idList.addAll(followersPage);
						if ((followersPage.size() < PAGE_SIZE && page == 1) || (followersPage.size() == 0)) {
							this.crawled = true;
						} else {
							page++;
							Thread.sleep(200); 
						}
					} else {
						switch (responseCode) {
							case 400 : Thread.sleep(1000); this.failCount++; break; 
							case 401 : this.result = new CrawlResult(this.twitterId, ResultCode.NOT_AUTHORIZED); this.crawled = true; break;
							case 403 : this.result = new CrawlResult(this.twitterId, ResultCode.INVALID_ACCOUNT); this.crawled = true; break;
							case 404 : this.result = new CrawlResult(this.twitterId, ResultCode.NOT_FOUND); this.crawled = true; break;
							default: this.failCount++; break;
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Unexpected exception for " + this.twitterId + ": " + e);
			System.out.println(e);
		}
	
		// Must be crawled at this point
		// If no fails yet, create the success result
		if (this.result == null) {
			int[] followers = new int[idList.size()];
			for (int i = 0; i < followers.length; i++) {
				followers[i] = idList.removeFirst();
			}
			this.result = new CrawlResult(this.twitterId, ResultCode.SUCCESS, followers);
		}
		
		// Finished and ready for results retrieval
		this.finished = true;
	}
}
