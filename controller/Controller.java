package controller;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import common.*;
import message.*;

// Multithreading considerations
//	crawled, pending, failed, queue, successLog, failLog are only accessed by main controller thread
//	usedPads, workers are accessed by registrar and main controller thread
public class Controller {
	private int JOB_MAX = 2000;
	
	private ServerSocket serverSocket;
	private ExecutorService threadPool;
	private LinkedList<WorkerRemote> workers;	// Explicitly synchronize
	private Registrar registrar;
	
	private HashSet<String> usedPads;			// Explicitly synchronize
	
	// Hash sets that keep track of jobs
	private int seed;
	private HashSet<Integer> crawled;
	private HashSet<Integer> pending;
	private HashSet<Integer> failed;
	
	// Timing and stats
	private long crawlCount = 0;
	private long startTime;
	
	// File based queue 
	private IntFileQueue queue;
	
	// Logs to hold successful and failed cases
	private SegmentedLogger successLog;
	private SegmentedLogger failLog;	
	
	// Log for status
	private Logger statusLog;
	
	public Controller(int listenPort, int seed, String baseName) {
		try {
			this.serverSocket = new ServerSocket(listenPort);
			this.threadPool = Executors.newCachedThreadPool();
			this.workers = new LinkedList<WorkerRemote>();	
			this.registrar = new Registrar(this, this.serverSocket);
			
			this.usedPads = new HashSet<String>();
			this.crawled = new HashSet<Integer>();
			this.pending = new HashSet<Integer>();
			this.failed = new HashSet<Integer>();
			
			this.seed = seed;
			this.queue = new IntFileQueue();
			
			// Establish the logs
			this.successLog = new SegmentedLogger(baseName + "_s");
			this.failLog = new SegmentedLogger(baseName + "_f");
			this.statusLog = new Logger("controller_log.txt");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void start() {
		try {
			// Start the server socket
			this.threadPool.execute(this.registrar);
			
			// Throw in the seed
			this.queue.enqueue(this.seed);
			
			// Record start time
			this.startTime = System.currentTimeMillis();
			
			// Main job loop
			while (this.queue.size() > 0 || this.pending.size() > 0) {
				this.retrieveResults();
				this.handleFailures();
				this.assignJobs();
			}			
			
			// Shut down workers
			synchronized(this.workers) { for (WorkerRemote worker : this.workers) worker.stop(); }
			this.successLog.close();
			this.failLog.close();
			this.threadPool.shutdownNow();
			
			// Log done, close logs
			this.statusLog.logStatus("DONE " + (this.successLog.loggedCount() + this.failLog.loggedCount()) + " users crawled.");
			this.statusLog.logStatus("SUCCESS: " + this.successLog.loggedCount());
			this.statusLog.logStatus("FAIL: " + this.failLog.loggedCount());
			this.statusLog.close();
		} catch (Exception e) {
			this.statusLog.logError(e.toString());
		}
	}
	
	// Check for results, grab and log results, enqueue more to crawl
	private void retrieveResults() {
		synchronized (this.workers) {
			Iterator<WorkerRemote> iter = this.workers.iterator();
			while (iter.hasNext()) {
				WorkerRemote current = iter.next();
				LinkedList<CrawlResult> results = current.popResults();
				if (results.size() > 0) {
					for (CrawlResult result : results) {
						int twitterId = result.getTwitterId();
					
						// Update the status of the twitter id in the tracking hashes and log it as well
						this.pending.remove(twitterId);
						if (result.getResult() == ResultCode.SUCCESS) {
							this.crawled.add(twitterId);
							this.successLog.addResult(result);
						} else {
							this.failed.add(twitterId);
							this.failLog.addResult(result);
						}
						
						// Loop through the followers - only enqueue those we have not processed already
						LinkedList<Integer> toEnqueue = new LinkedList<Integer>();
						for (int followerId : result.getFollowers()) {
							if (!this.processed(followerId)) {
								toEnqueue.add(followerId);						
							}
						}
						this.queue.enqueue(toEnqueue);
						
						// Log a status message every 10k crawled
						this.crawlCount++;
						if (crawlCount % 10000 == 0 && crawlCount > 0) {
							long nowMilliseconds = System.currentTimeMillis();
							this.statusLog.logStatus("Crawled " + this.crawlCount + " at " + ((nowMilliseconds - this.startTime)/1000) + " seconds.");
						}
					}
				}
			}
		}
	}
	
	// Assign jobs to workers with no pending ids
	private void assignJobs() {
		if (this.queue.size() > 0) {
			synchronized(this.workers) {
				Iterator<WorkerRemote> iter = this.workers.iterator();
				while (iter.hasNext()) {
					WorkerRemote current = iter.next();
					if (current.isRunning() && current.nonePending()) {		
						LinkedList<Integer> toCrawl = new LinkedList<Integer>();
						int[] ids = this.queue.dequeue(JOB_MAX);
						for (int twitterId : ids) {
							if (!this.processed(twitterId)) {
								toCrawl.add(twitterId);
								this.pending.add(twitterId);
							}
						}	
						current.pushId(toCrawl);
						this.statusLog.logStatus(this.queue.size() + " in queue, " + this.workers.size() + " workers");
						this.statusLog.logStatus("Assigned " + toCrawl.size() + " ids to " + current.getName() + " with account " + current.getUsername());
					}
				}
			}
		}
	}
	
	// Handle any failed workers by getting all pending ids, removing from pending hash and adding back into queue
	private void handleFailures() {
		synchronized(this.workers) {
			Iterator<WorkerRemote> iter = this.workers.iterator();
			while (iter.hasNext()) {
				WorkerRemote current = iter.next();
				if (!current.isRunning()) {
					this.statusLog.logStatus(current.getName() + " with account " + current.getUsername() + " dropped out, rolling back");
					LinkedList<Integer> pending = current.pendingIds();
					this.pending.removeAll(pending);
					this.queue.enqueue(pending);
					iter.remove();
				}
			}
		}
	}
		
	// Have we already processed this id?
	private boolean processed(int twitterId) {
		return (this.crawled.contains(twitterId) || this.pending.contains(twitterId) || this.failed.contains(twitterId));
	}
	
	// Add a worker to the workers queue
	// Accesses synchronized workers
	public void addWorker(WorkerRemote worker) {
		worker.start();
		synchronized(this.workers) {
			this.workers.add(worker);
		}
		this.statusLog.logStatus("Added worker " + worker.getName() + " using account " + worker.getUsername());
	}
	
	// Validate a message
	public boolean validate(Message m) {
		synchronized(this.usedPads) {
			if (this.usedPads.contains(m.getPad()) || !m.valid(Secret.SECRET)) {
				return false;
			} else {
				this.usedPads.add(m.getPad());
				return true;
			}
		}
	}
	
	// Testing program
	public static void main(String[] args) throws UnknownHostException {
		/*
		Controller c = new Controller(4000, 12854372, "log/");
		c.start();
		*/
		
		if (args.length == 3) {
			Controller c = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);
			c.start();
		} else {
			System.out.println("Don't forget to use -XmsM and -XmxM options, where M is like 256m, 1g, etc.");
			System.out.println("Usage: port seed log");
			System.out.println("For a good seed, try 12854372 or 813286");
		}
	}
}
