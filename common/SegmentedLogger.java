package common;

import java.util.*;
import java.io.*;
import java.util.zip.*;

// Segmented logger writes log files, and handles flushing
public class SegmentedLogger {
	private LinkedList<CrawlResult> queue;
	private String baseName;
	private DataOutputStream output;
	private String EXTENSION = ".txt";
		
	// Counters and flushing parameters
	private long loggedCount = 0;
	private int segmentCount = 0;
	private int FLUSH_THRESHOLD = 100;
	private int SEGMENT_THRESHOLD = 100000;
	
	// Constructor
	public SegmentedLogger(String baseName) {
		this.queue = new LinkedList<CrawlResult>();
		this.baseName = baseName;
		try {
			this.output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(this.segmentName(), true)));
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	// Convenience method to tell us current segmented file name
	private String segmentName() {
		return (this.baseName + "_" + this.segmentCount + this.EXTENSION);
	}
	
	// Add a result to the queue, flushing when appropriate
	public void addResult(CrawlResult set) {
		this.queue.add(set);
		if (this.queue.size() == this.FLUSH_THRESHOLD) {
			this.flush();
		}
	}
	
	// Flush the queue, and segment if necessary
	public void flush() {
		try {
			int writeCount = this.queue.size();
			while (this.queue.size() > 0) {
				this.writeResult(this.queue.removeFirst());
			}
			this.output.flush();
			this.loggedCount += writeCount;			
			if (this.loggedCount % this.SEGMENT_THRESHOLD == 0 && this.loggedCount > 0) {
				this.segment();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	// Does not flush
	private void writeResult(CrawlResult set) throws IOException {
		this.output.writeInt(set.getTwitterId());
		this.output.writeInt(set.getResult().toInt());
		if (set.isSuccessful()) {
			int[] followers = set.getFollowers();
			this.output.writeInt(followers.length);
			for (int followerId : followers) {
				this.output.writeInt(followerId);
			}
		}
	}
	
	// Close the current segment and make a new one
	private void segment() {
		try {
			// Close the segment, gzip it 
			this.output.close();
			Thread gzipper = new Thread(new GzipFileTask(this.segmentName()));
			gzipper.start();
			
			// Increment the segment count and create the new segment
			this.segmentCount += 1;
			this.output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(this.segmentName(), true)));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	// Access functions - logged so far
	public long loggedCount() { return this.loggedCount; }
	public long queueSize() { return this.queue.size(); }
	
	// Close the current segment
	public void close() {
		try {
			this.flush();
			this.output.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}

class GzipFileTask implements Runnable {
	private String fileName;
	GzipFileTask(String fileName) {
		this.fileName = fileName;
	}
	
	public void run() {
		try {
			String outFile = this.fileName + ".gz";
			FileInputStream in = new FileInputStream(this.fileName);
			GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(outFile));
			
			// Gzip file
			byte[] buff = new byte[4096];
			while (in.read(buff) > 0) {	out.write(buff); }
			in.close();
			out.finish();
			out.close();
			
			// Delete old file file
			(new File(this.fileName)).delete();						
		} catch (Exception e) {
			System.out.println("Failed to gzip segment.");
			System.out.println(e);
		}		
	}
}