package message;

import common.*;

public class CrawlResultMessage extends Message{
	private static final long serialVersionUID = -1280447990349561359L;
	private CrawlResult[] results;
	
	public CrawlResultMessage(String key, CrawlResult[] results) {
		super(key);
		this.results = results;
	}
	
	public CrawlResult[] getResults() {
		return this.results;
	}
}
