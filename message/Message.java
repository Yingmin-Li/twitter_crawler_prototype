package message;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Message implements Serializable {
	private static final long serialVersionUID = 7675288057571122511L;
	private String pad;
	private String signature;
	
	public Message(String key) { 
		this.pad = UUID.randomUUID().toString();
		this.signature = this.calculateSignature(key);
	}
	
	// Check if a message validates given a certain key
	public boolean valid(String key) {		
		return this.signature.equals(this.calculateSignature(key));
	}
	
	// Calculate the signature from a key and the pad
	public String calculateSignature(String key) {
		String retval = "";
		MessageDigest digest;
		try {
			String encrypt = this.pad + key;
			digest = MessageDigest.getInstance("MD5");
			digest.reset();
			digest.update(encrypt.getBytes());
			BigInteger number = new BigInteger(1, digest.digest());
            retval = number.toString(16);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(-1);
		} 	
		return retval;
	}
	
	public String getPad() { return this.pad; }
}
