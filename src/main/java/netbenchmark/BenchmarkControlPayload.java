package netbenchmark;

import java.io.Serializable;
import java.util.Random;

public class BenchmarkControlPayload implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8868160306004573541L;
	public String command;
	
	//public boolean lastpayload;
	//public byte[] payload;
	//public int sizeInMB;
	//public long recTime;
	public int sizeInBytes;
	public long recTime;
	public BenchmarkControlPayload()
	{
		//this.lastpayload = false;
		//this.header = header;
		//this.sizeInMB = sizeInMB;
		//int sizeInBytes = sizeInMB * 1024 * 1024;
		//payload = new byte[sizeInBytes];
		//new Random().nextBytes(payload);
	}
	public void setSizeInBytes(int sizeInMB)
	{
		sizeInBytes =  sizeInMB * 1024 * 1024;
	}
}
