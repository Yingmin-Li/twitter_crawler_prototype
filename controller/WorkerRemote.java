package controller;

import java.io.*;
import java.util.*;
import common.*;
import message.*;
import java.util.concurrent.*;

public class WorkerRemote {
	private String name;
	private String username;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private Controller controller;
	private ExecutorService threadPool = Executors.newFixedThreadPool(1); // Socket thread
	
	private volatile boolean running = false;
	private volatile boolean sending = true;
	
	// Out queue, in queue and pending hash set
	private LinkedList<Integer> outQueue = new LinkedList<Integer>();
	private LinkedList<CrawlResult> inQueue = new LinkedList<CrawlResult>();
	private HashSet<Integer> pending = new HashSet<Integer>();
	
	public WorkerRemote(String name, String username, Controller controller, ObjectInputStream in, ObjectOutputStream out) {
		this.name = name;
		this.username = username;
		this.in = in;
		this.out = out;
		this.controller = controller;
	}
	
	public String getName() { return this.name; }
	public String getUsername() { return this.username; }
	public boolean isRunning() { return this.running; }
	public boolean isSending() { return this.sending; }
	
	// Push outQueue 
	public void pushId(int[] ids) {
		synchronized(this.outQueue) {
			for (int id : ids) { 
				this.outQueue.add(id); 
			}
		}
	}
	
	public void pushId(LinkedList<Integer> ids) {
		synchronized(this.outQueue) {
			for (int id : ids) {
				this.outQueue.add(id); 
			} 
		}
	}
	
	public void pushId(int id) {
		synchronized(this.outQueue) {
			this.outQueue.add(id);
		}
	}
	
	// Pop inQueue
	public CrawlResult popResult() {		
		if (this.inQueue.size() > 0) {
			synchronized(this.inQueue) {
				return this.inQueue.removeFirst();
			}
		} else {
			return null;
		}
	}
	
	// Pop all inQueue
	public LinkedList<CrawlResult> popResults() {
		LinkedList<CrawlResult> results = new LinkedList<CrawlResult>();
		if (this.inQueue.size() > 0) {			
			synchronized(this.inQueue) {
				for (CrawlResult result : this.inQueue) {
					results.add(result);
				}
				this.inQueue.clear();
			}
		} 
		return results;
	}
	
	// Pop outQueue, add pending, send (thread)
	// Called by sendThread
	public void sendJob() throws IOException {
		if (this.sending) {
			if (this.outQueue.size() > 0) {			
				synchronized(this.outQueue) {			
					// Get the id numbers from the outQueue
					int[] ids = new int[this.outQueue.size()];
					for (int i = 0; i < ids.length; i++) { ids[i] = this.outQueue.removeFirst(); }
					
					// Inner lock on pending
					synchronized(this.pending) {
						for (int id : ids) { this.pending.add(id); }
					}
					
					this.outQueue.clear();
					
					// Create and send the assignment message
					AssignmentMessage am = new AssignmentMessage(Secret.SECRET, ids);
					this.out.writeObject(am);
					this.out.flush();
				}			
				this.sending = false;
			}		
		}
	}
	
	// Receive, remove pending, push inQueue
	// Called by receiveThread
	public void receiveResults() throws IOException, ClassNotFoundException {
		if (!this.sending) {
			Object o = this.in.readObject();
			if (o instanceof CrawlResultMessage && this.controller.validate((Message)o)) {
				CrawlResult[] results = ((CrawlResultMessage)o).getResults();
				synchronized (this.inQueue) {
					synchronized(this.pending) {
						for (CrawlResult result : results) {
							this.pending.remove(result.getTwitterId());
							this.inQueue.add(result);
							
							if (this.pending.size() == 0) {
								this.sending = true;
							}
						}
					}
				}
			} 
		}
	}

	
	// None pending? - easy way to check if this worker is ready
	public boolean nonePending() {
		synchronized (this.outQueue) {
			synchronized (this.pending) {
				return (this.outQueue.size() == 0 && this.pending.size() == 0);
			}
		}
	}
	
	// What ids are pending?
	public LinkedList<Integer> pendingIds() {
		synchronized (this.outQueue) {
			LinkedList<Integer> retval = new LinkedList<Integer>(this.outQueue);	
			synchronized(this.pending) {				
				Iterator<Integer> iter = this.pending.iterator();
				while (iter.hasNext()) {
					retval.add(iter.next());
				}
			}
			return retval;
		}
	}
	
	// Start the thread after construction - this will only work once
	public void start() {
		this.running = true;
		this.threadPool.execute(new SocketThread(this));
	}
	
	// Stop the thread after construction
	public void stop() {
		try {
			this.running = false;
			this.in.close();
			this.out.close();
		} catch (IOException e) {
			System.out.println("Shut down worker error!");
		}
	}
}

class SocketThread implements Runnable {
	private WorkerRemote worker;
	SocketThread(WorkerRemote worker) {
		this.worker = worker;
	}
	
	public void run() {
		while (this.worker.isRunning()) {
			try {
				if (this.worker.isSending()) {
					this.worker.sendJob();
					Thread.sleep(10000);
				} else {
					this.worker.receiveResults();
					Thread.sleep(2000);
				}
			} catch (Exception e) {
				this.worker.stop();
			}
		}
	}
}