package common;

import java.io.*;
import java.util.*;

// This file will need to have a segmented queue
// This is NOT thread safe!
public class IntFileQueue {
	private String fileName;
	
	private DataInputStream front;
	private DataOutputStream back;
	
	private int count;
	private int dequeueCount;
	private int DEQUEUE_THRESHOLD = 10000000;
	
	public IntFileQueue() {
		this.fileName = "queue/" + Long.toString((new Date()).getTime());
		this.count = 0;
		try {
			this.back = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(this.fileName, true)));
			this.front = new DataInputStream(new BufferedInputStream(new FileInputStream(this.fileName)));			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}	
	}
	
	public int size() {
		return this.count;
	}
	
	public int dequeue() {
		int retval = 0;
		if (!this.isEmpty()) {
			try {
				retval = this.front.readInt();
				this.count--;
				this.dequeueCount++;
			} 
			catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}			
		}
		this.reconstructFile();
		return retval;
	}
	
	public int[] dequeue(int max) {
		int readCount = Math.min(max, this.count);
		int[] retval = new int[readCount];
		try {
			for (int i = 0; i < readCount; i++) { retval[i] = this.front.readInt();	}
			this.count -= readCount;
			this.dequeueCount += readCount;
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		this.reconstructFile();
		return retval;
	}
		
	// Writes one integer - try to write in batch instead
	public void enqueue(int item) {
		try {
			this.back.writeInt(item);
			this.back.flush();
			this.count++;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}	
	
	public void enqueue(int[] items) {
		try {
			for (int item: items) { this.back.writeInt(item); }
			this.back.flush();
			this.count += items.length;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void enqueue(LinkedList<Integer> items) {
		try {
			for (int item: items) { this.back.writeInt(item); }
			this.back.flush();
			this.count += items.size();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public boolean isEmpty() {
		return (this.count == 0);
	}
	
	public void close() {
		try {
			this.front.close();
			this.back.close();
			File f = new File(this.fileName);
			f.delete();
		} 
		catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private void reconstructFile() {
		if (this.dequeueCount > DEQUEUE_THRESHOLD) {
			try {
				// Create a new temporary file and copy the current file into it from the current position
				String newFile = this.fileName = Long.toString((new Date()).getTime());
				DataOutputStream newBack = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile, true)));
				DataInputStream newFront = new DataInputStream(new BufferedInputStream(new FileInputStream(newFile)));
			
				int readCount = this.count;
				while (readCount-- > 0) { newBack.writeInt(this.front.readInt()); }
				newBack.flush();
				
				this.front.close();
				this.back.close();				
				this.front = newFront;
				this.back = newBack;
				
				File f = new File(this.fileName);
				f.delete();
				this.fileName = newFile;
				
				// Reset the dequeue count
				this.dequeueCount = 0;
			}
			catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
}
