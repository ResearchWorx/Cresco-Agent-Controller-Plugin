package netbenchmark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Random;

public class NetBenchClient {

	
	public NetBenchClient()
	{
		
	}
	
	public String benchmarkThroughput(String hostname, String benchmarkType, int repeat, int sizeInMB)
	{
		String result = null;
		try
		{
			float sendMbps = 0;
			float recMbps = 0;
			
			for(int i = 1; i <= repeat; i++)
			{
				String results = benchmarkThroughput(hostname, benchmarkType, sizeInMB);
				//System.out.println(results);
				String[] sstr = results.split("_");
				float tmpSendMbps = Float.parseFloat(sstr[0]);
				float tmpRecMbps = Float.parseFloat(sstr[1]);
				
				if(i == 1)
				{
					sendMbps = tmpSendMbps;
					recMbps = tmpRecMbps;			
				}
				else
				{
					sendMbps = (sendMbps + tmpSendMbps)/2;
					recMbps = (recMbps + tmpRecMbps)/2;
				}
				Thread.sleep(1000);
			}
			result = String.valueOf(Math.round(sendMbps)) + "," + String.valueOf(Math.round(recMbps));
		}
		catch(Exception ex)
		{
			System.out.println("NetBenchClient : benchmarkThroughput-repeat : Error : " + ex.toString());
		}
		return result;
	}
	public String benchmarkThroughput(String hostname, String benchmarkType, int sizeInMB)
	{
		//System.out.println("Sending control object to server ...");
		String results = null;
		Socket socketToServer = null;
        try
    	{
    		socketToServer = new Socket(hostname, 32005);
        	ObjectOutputStream outputStream = new ObjectOutputStream(socketToServer.getOutputStream());
        	ObjectInputStream inputStream = new ObjectInputStream(socketToServer.getInputStream());
        	
        	//for(int i = 1; i < repeat; i++)
        	//{
        		BenchmarkControlPayload payload = new BenchmarkControlPayload();
        		payload.setSizeInBytes(sizeInMB);
        		payload.command = "benchmark-throughput";
        		outputStream.writeObject(payload);
        		outputStream.flush();
		
        		//System.out.println("Sending benchmark start");
        		outputStream.writeInt(1);
        		outputStream.flush();
		
        		byte[] benchPayload = new byte[payload.sizeInBytes];
        		new Random().nextBytes(benchPayload);
        		outputStream.write(benchPayload);
        		//outStream.flush();
		
        		outputStream.writeInt(-1);
        		outputStream.flush();
		
        		BenchmarkControlPayload o = (BenchmarkControlPayload)inputStream.readObject();
		
        		if(o.command.equals("benchmark-throughput"))
        		{
        			//System.out.println("Server-to-Client benchmark-throughput test");
        			long recElapsedTime = 0;
        			int sizeInBytes = o.sizeInBytes;
        			int startSig = inputStream.readInt();
        			//System.out.println(startSig);
        			if(startSig == 1)
        			{
        				//System.out.println("Server starting benchmark-throughput signal start");
    			
        				int bytesReadCount = 0;
        				//System.out.println(bytesReadCount + " < " + sizeInBytes);
				
        				byte[] b = new byte[1024];
        				//long recStartTime = System.nanoTime();
        				long recStartTime = System.currentTimeMillis();
        				while(bytesReadCount < sizeInBytes)
        				{
					
        					int bytesRead = inputStream.read(b);
        					bytesReadCount = bytesReadCount + bytesRead; 
        					//System.out.println("Server bytesRead: " + bytesRead);
        					//bytes[] b = new bytes[sizeInBytes];
        				}
        				//long recEndTime = System.nanoTime();
        				long recEndTime = System.currentTimeMillis();
        				recElapsedTime = recEndTime - recStartTime;
        				//float sendMbps = 1000000000/recElapsedTime;
        				//float recMbps = ((sizeInBytes/recElapsedTime) * 1000)/(1048576);
        				//float sendMbps = ((sizeInBytes/o.recTime) * 1000)/(1048576);
        				float recMbps = ((sizeInBytes/recElapsedTime) * 1000)/(1024);
        				float sendMbps = ((sizeInBytes/o.recTime) * 1000)/(1024);
        				//float sendMbps = 1000/recElapsedTime;
        				//System.out.println("Mbps = " + sendMbps);
        				results = sendMbps + "," + recMbps;
        				if(inputStream.readInt() == -1)
        				{
        					//System.out.println("benchmark complete!");
        					
        				}
        			}
        		}
        	//}
    		socketToServer.shutdownInput();
        	socketToServer.shutdownOutput();
        	if(socketToServer.isOutputShutdown() && socketToServer.isInputShutdown())
        	{
        		outputStream.close();
                inputStream.close();
                //System.out.println("try and close");
        	}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("NetBenchClient : benchmarkThroughput : Error : " + ex.toString());
    	}
    	finally
        {
        	try {
				socketToServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		return results;
	}
   
    
}
