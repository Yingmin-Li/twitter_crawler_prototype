package controller;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;

import common.*;
import message.*;

public class Registrar implements Runnable {
	private Controller controller;
	private ServerSocket serverSocket;
	private ExecutorService threadPool;

	public Registrar(Controller controller, ServerSocket serverSocket) {
		this.controller = controller;
		this.serverSocket = serverSocket;
		this.threadPool = Executors.newSingleThreadExecutor();
	}
	
	public void run() {
		while (true) {
			try {
				this.threadPool.execute(new Registration(this.controller, this.serverSocket.accept()));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
}

class Registration implements Runnable {
	private Controller controller;
	private Socket socket;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	
	public Registration(Controller controller, Socket socket) throws IOException {
		this.controller = controller;
		this.socket = socket;
		this.socket.setSoTimeout(Timeout.TIMEOUT);
	}
	
	public void run() {
		try {			
			this.out = new ObjectOutputStream(this.socket.getOutputStream());
			this.in = new ObjectInputStream(this.socket.getInputStream());
			
			Object o = this.in.readObject();
			if (!(o instanceof RegisterMessage)) {
				throw new RuntimeException("Not a register message");
			} else if (!this.controller.validate((RegisterMessage)o)) {
				throw new RuntimeException("Not a valid message");
			} else {
				this.out.writeObject(new AcknowledgementMessage(Secret.SECRET));
				this.out.flush();
				
				RegisterMessage m = (RegisterMessage)o;
				WorkerRemote worker = new WorkerRemote(m.getName(), m.getUsername(), this.controller, this.in, this.out);
				this.controller.addWorker(worker);
			} 
		} catch (Exception e) {
			System.out.println(e);
			try {
				this.socket.close();
			} catch (IOException e1) {}
		}
	}
}
