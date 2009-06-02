package common;

import java.io.*;
import java.util.Date;

// Basic logging class, opens the log on instantiation
public class Logger {	
	private String fileName;
	private PrintWriter writer; 
	
	public Logger(String fileName) {
		this.fileName = fileName;
		try {
			this.writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream(this.fileName, true)));
		} catch (Exception e) {
			System.out.println(e);
			System.exit(-1);
		}				
	}
	
	public void logStatus(String status) { 
		String message = this.timeStamp() + ": STATUS " + status;
		this.logMessage(message);
	}
	
	public void logError(String error) {
		String message = this.timeStamp() + ": ERROR " + error;
		this.logMessage(message);
	}
	
	private void logMessage(String message) {
		try {
			System.out.println(message);
			this.writer.println(message);
			this.writer.flush();
		} catch (Exception e) {
			System.out.print(e);
			System.exit(-1);
		}
	}
	
	private String timeStamp() { return (new Date()).toString(); }
	
	public void close() throws IOException {
		this.writer.flush();
		this.writer.close();		
	}
}
