package common;

// The possible results of crawl tasks
public enum ResultCode {
	SUCCESS,
	INVALID_ACCOUNT,
	NOT_FOUND,
	NOT_AUTHORIZED,
	FAILED;
	
	public int toInt() {
		int retval = 0;
		switch (this) {
			case SUCCESS: retval = 0;
			case INVALID_ACCOUNT: retval = 1;
			case NOT_FOUND: retval = 2;
			case NOT_AUTHORIZED: retval = 3;
			case FAILED: retval = 4;
		}
		return retval;
	}
}