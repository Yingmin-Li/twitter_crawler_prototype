package worker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import message.*;
import common.*;

// Multithreading considerations
// in, out accessed only by ReceiveThread, SendThread respectively
// usedPads accessed by SendThread, ReceiveThread
// inQueue, outQueue accessed by main thread and ReceiveThread, main thread and SendThread respectively
// threadPool, tasks accessed only by main thread
public class Worker {
	private String hostName;
	private int hostPort;
	private boolean connected = false;
	private String username;
	private String password;
	private long crawlCount = 0;
	
	// Speed / thread limits
	private int MAX_CONCURRENCY = 40;
	private int REQUESTS_PER_HOUR = 18000;
	private int SLEEP_INTERVAL = Math.round((float)3600000 / (float)REQUESTS_PER_HOUR);
	
	// Connection details
	private Socket socket;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private volatile boolean sending = false;
	
	// Message validation
	private HashSet<String> usedPads = new HashSet<String>();
	
	// Message queues
	private LinkedList<Integer> inQueue = new LinkedList<Integer>();
	private LinkedList<CrawlResult> outQueue = new LinkedList<CrawlResult>();
		
	// Thread pool
	private ExecutorService threadPool = Executors.newFixedThreadPool(40);
	private LinkedList<CrawlTask> tasks = new LinkedList<CrawlTask>();
	
	// Status log
	private Logger statusLog;
	
	// Constructor
	public Worker(String name, int port, String username, String password) {
		this.hostName = name;
		this.hostPort = port;
		this.username = username;
		this.password = password;
		this.statusLog = new Logger("worker_log.txt");
	}
	
	// Get the count
	public long getCount() { return this.crawlCount; }

	// Connect function
	private void connect() {
		try {
			// Attempt the start the connection
			this.socket = new Socket(this.hostName, this.hostPort);		
			this.socket.setSoTimeout(Timeout.TIMEOUT);
			
			this.out = new ObjectOutputStream(this.socket.getOutputStream());
			this.in = new ObjectInputStream(this.socket.getInputStream());
			
			// Send a register message
			this.out.writeObject(new RegisterMessage(Secret.SECRET, InetAddress.getLocalHost().getHostAddress(), this.username));
			this.out.flush();
			
			// Wait for an acknowledgement
			Object o = this.in.readObject();
			if (!(o instanceof AcknowledgementMessage)) {
				throw new RuntimeException("Not a response message");
			} else if (!this.validate((AcknowledgementMessage)o)) {
				throw new RuntimeException("Invalid message");
			} else {
				this.connected = true;
			}
		} catch (Exception e) {
			try {
				this.statusLog.logError(e.toString());
				if (this.socket != null) {
					this.socket.close();
				}
			} catch (IOException e1) {}
		}		
	}
	
	// Read in message, put ids on queue - receive thread
	public void receiveIds() throws IOException, ClassNotFoundException {
		if (!this.sending) {
			Object o = this.in.readObject();
			if (o instanceof AssignmentMessage && this.validate((Message)o)) {
				AssignmentMessage am = (AssignmentMessage)o;
				int[] ids = am.getIds();
				synchronized(this.inQueue) {
					for (int id : ids) { 
						this.inQueue.add(id); 
					}
				}
				this.statusLog.logStatus("Received " + ids.length + " ids to crawl.");
				this.sending = true;
			}
		}
	}
	
	// Grab results from queue, send out - send thread
	public void sendResults() throws IOException {
		if (this.sending) {
			if (this.outQueue.size() > 0) {	
				synchronized (this.outQueue) {					
					// Clear queue
					CrawlResult[] results = new CrawlResult[this.outQueue.size()];
					for (int i = 0; i < results.length; i ++) { results[i] = this.outQueue.removeFirst(); }
					
					// Send message
					CrawlResultMessage crm = new CrawlResultMessage(Secret.SECRET, results);
					this.out.writeObject(crm);
					this.out.flush();
				}
				
				if (this.inQueue.size() + this.tasks.size() == 0) {
					this.sending = false;
				}
			}			
		}
	}
		
	
	public void start() {
		this.connect();
		
		long beginTime = System.currentTimeMillis();
		
		if (this.connected) {			
			this.statusLog.logStatus("Worker connected!");
			
			// Start receive and send threads			
			this.threadPool.execute(new SocketThread(this));
			int runningCount = 0;
			this.crawlCount = 0;
			
			try {
				while (this.connected) {								
					// Spawn any new crawler tasks necessary
					while (this.inQueue.size() > 0 && runningCount < MAX_CONCURRENCY) {
						synchronized(this.inQueue) {
							int nextId = this.inQueue.removeFirst();
							CrawlTask task = new CrawlTask(nextId, this.username, this.password);
							this.threadPool.execute(task);
							this.tasks.add(task);
							runningCount++;
						}
						Thread.sleep(SLEEP_INTERVAL);
					}
				
					// Loop through current tasks, writing results to outQueue and popping as necessary
					Iterator<CrawlTask> iter = this.tasks.iterator();
					while (iter.hasNext()) {
						CrawlTask task = iter.next();
						if (task.isFinished()) {
							synchronized(this.outQueue) {
								this.outQueue.add(task.getResult());
							}
							iter.remove();
							runningCount--;		
							this.crawlCount++;
							
							// Log statement
							if (this.crawlCount % 1000 == 0 && this.crawlCount > 0) {
								long currentTime = System.currentTimeMillis();
								this.statusLog.logStatus("Crawled " + crawlCount + " in " + ((currentTime - beginTime)/1000) + " seconds.");
							}
						}
					}
					Thread.sleep(500);
				}
				
				// We're done, so clean up
				this.threadPool.shutdownNow();
			} catch (Exception e) {
				this.statusLog.logError("Error in main loop - " + e);
				System.exit(-1);
			}
		} else {
			this.statusLog.logError("Failed to connect.");
		}
	}
	
	public void stop() {
		try {
			this.connected = false;
			this.in.close();
			this.out.close();
		} catch (IOException e) {}
	}
	
	// We don't need to synchronize the worker version - only one thread trades messages
	public boolean validate(Message m) {
		if (this.usedPads.contains(m.getPad()) || !m.valid(Secret.SECRET)) {
			return false;
		} else {
			this.usedPads.add(m.getPad());
			return true;
		}
	}
	
	public boolean isSending() {
		return this.sending;
	}
	
	// Testing program
	public static void main(String[] args) throws UnknownHostException {
		/*
		Worker w = new Worker("localhost", 4000, "ningliang", "i7ahfdhy.");
		w.start();
		*/
		
		if (args.length == 4) {
			while (true) {
				Worker w =  new Worker(args[0], Integer.parseInt(args[1]), args[2], args[3]);
				w.start();
				Runtime.getRuntime().gc();
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) { 
					System.out.println("Interrupt received.");
					break; 
				}
			}
		} else {
			System.out.println("Don't forget to use -XmsM and -XmxM options, where M is like 256m, 1g, etc.");
			System.out.println("Usage: hostname port username password");
		}
	}
}

class SocketThread implements Runnable {
	private Worker worker;
	SocketThread(Worker worker) {
		this.worker = worker;
	}
	
	public void run() {
		while (true) {
			try {
				if (this.worker.isSending()) {
					this.worker.sendResults();
					Thread.sleep(2000);
				} else {
					this.worker.receiveIds();
				}
			} catch (Exception e) {
				System.out.println("Error in socket thread: " + e);
				e.printStackTrace();
				this.worker.stop();
				break;
			}
		}
	}	
}