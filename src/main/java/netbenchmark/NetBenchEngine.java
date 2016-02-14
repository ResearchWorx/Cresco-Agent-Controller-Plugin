package netbenchmark;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

import javax.net.ServerSocketFactory;

import plugincore.PluginEngine;


    public class NetBenchEngine extends Thread {

        private ServerSocket serverSocket;

        public NetBenchEngine() throws IOException {
            serverSocket = ServerSocketFactory.getDefault().createServerSocket(32005);
        }

        
        public void run() {
        	PluginEngine.NetBenchEngineActive = true;
            while (PluginEngine.NetBenchEngineActive) {
                try {
                    final Socket socketToClient = serverSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(socketToClient);
                    clientHandler.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class ClientHandler extends Thread{
        private Socket socket;
        ObjectInputStream inputStream;
        ObjectOutputStream outputStream;
        

        ClientHandler(Socket socket) throws IOException {
            this.socket = socket;
            inputStream = new ObjectInputStream(socket.getInputStream());
            //outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            
        }

        @Override
        public void run() {
        	boolean islastpayload = false;
        	try 
            {
        		
        		//get object first
        		BenchmarkControlPayload o = (BenchmarkControlPayload) inputStream.readObject();
        		//System.out.println("Server rec control message");
    			if(o.command.equals("benchmark-throughput"))
    			{
    				//System.out.println(" starting benchmark-throughput test");
    				long recElapsedTime = 0;
    				int sizeInBytes = o.sizeInBytes;
    				//byte[] b = new byte[sizeInBytes];
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
    	        		//float sendMbps = ((sizeInBytes/recElapsedTime) * 1000)/(1048576);
    	        		//float sendMbps = 1000/recElapsedTime;
    	        		//System.out.println("Mbps = " + sendMbps);
    					if(inputStream.readInt() == -1)
        				{
    						//System.out.println("benchmark complete!");
        				}
    				}
    				
    				//Now bench the other way
        			BenchmarkControlPayload payload = new BenchmarkControlPayload();
    				payload.recTime = recElapsedTime;
    				payload.command = o.command;
    				payload.sizeInBytes = o.sizeInBytes;
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
        			
        			
    			}
        		//if benchmark
        		
        		
        		/*
        		BenchmarkControlPayload payload = new BenchmarkControlPayload();
        		while (!islastpayload) 
        		{
            			long recStartTime = System.nanoTime();
            			System.out.println("Starting waiting on read");
            			//System.out.println(inputStream.readInt());
            			BenchmarkControlPayload o = (BenchmarkControlPayload) inputStream.readObject();
            			long recEndTime = System.nanoTime();
            			
            			//long recEndTime = System.currentTimeMillis();
            			long recElapsedTime = recEndTime - recStartTime;
            			System.out.println("Server Rec: " + recElapsedTime);
                		
            			//System.out.println("server elapsed time: " + recElapsedTime);
            			//islastpayload  = o.lastpayload;
            			//System.out.println("Islastpayload: " + islastpayload);
            			//System.out.println("Server Read object: "+o.header);
                    
            			//payload.recTime = recElapsedTime;
            			outputStream.writeObject(payload);
            			outputStream.flush();
            		}
                 */
                
            }
        	catch (EOFException eof)
        	{
        		//socket.close();
        		System.out.println("Islastpayload ex: " + islastpayload);
                
        	}
        	catch (Exception e) 
            {
                System.out.println("NetBenchEngine : ClientHandler : Error " + e.toString());
            }
        	finally
        	{
        		try {
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	
        	}
            
            //System.out.println("Exiting server..");
        }
    
    } 
    

