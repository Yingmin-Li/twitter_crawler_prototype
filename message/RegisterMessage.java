package message;

public class RegisterMessage extends Message {
	private static final long serialVersionUID = -4289228394049096903L;
	private String workerName;
	private String username;
	
	public RegisterMessage(String key, String name, String username) {
		super(key);
		this.workerName = name;
		this.username = username;
	}
	
	public String getName() { return this.workerName; }
	public String getUsername() { return this.username; }
}
