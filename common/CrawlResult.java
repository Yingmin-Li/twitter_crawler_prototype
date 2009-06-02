package common;
import java.io.Serializable;

public class CrawlResult implements Serializable {
	private static final long serialVersionUID = -7134169384558490400L;
	private ResultCode result;
	private int[] followers;
	private int twitterId;
	
	public CrawlResult(int twitterId, ResultCode result, int[] followers) {
		this.twitterId = twitterId;
		this.result = result;
		this.followers = followers;
	}
	
	public CrawlResult(int twitterId, ResultCode result) {
		this.twitterId = twitterId;
		this.result = result;
		this.followers = new int[] {};
	}
	
	public ResultCode getResult() { return this.result; }
	public int getTwitterId() { return this.twitterId; }
	public int[] getFollowers() { return this.followers; }
	public boolean isSuccessful() { return this.result == ResultCode.SUCCESS; }
}