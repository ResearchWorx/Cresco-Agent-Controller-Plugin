package netdiscoveryIPv4;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;


public class DiscoveryEngine implements Runnable 
{
	//private DatagramSocket socket;
	private Gson gson;
	public DiscoveryEngine()
	{
		gson = new Gson();
	}
	  
	public void shutdown()
	{
		//socket.close();
	}
	
	  public void run() {
	    try {
	      //Keep a socket open to listen to all the UDP trafic that is destined for this port
	      //socket = new DatagramSocket(32005, InetAddress.getByName("0.0.0.0"));
	      //socket = new DatagramSocket(32005, Inet4Address.getByName("0.0.0.0"));
	    	//socket = new DatagramSocket(null);
	    	
		  Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
	 	  while (interfaces.hasMoreElements()) {
	 	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
	 	   new Thread(new DiscoveryEngineWorkerIPv4(networkInterface)).start();
	 	  }
	        
	      PluginEngine.DiscoveryActive = true;
	      
	    } 
	    catch (Exception ex) 
	    {
	    	System.out.println(ex.toString());
	    }
	  }
	  
	  public static DiscoveryEngine getInstance() {
	    return DiscoveryThreadHolder.INSTANCE;
	  }

	  private static class DiscoveryThreadHolder {

	    private static final DiscoveryEngine INSTANCE = new DiscoveryEngine();
	  }

	  class DiscoveryEngineWorkerIPv4 implements Runnable 
	  {
		  private NetworkInterface networkInterface;
		  private DatagramSocket socket;
		  private DatagramSocket sendsocket;
		  
		    public DiscoveryEngineWorkerIPv4(NetworkInterface networkInterface)
		    {
		    	
		    	this.networkInterface = networkInterface;
		    	
		    }
		    public void shutdown()
			{
				socket.close();
				sendsocket.close();
			}
		    public void run() 
		    {
		    	try
		    	{
		    		//if (!networkInterface.isLoopback() && networkInterface.isUp())
		    		if (!networkInterface.getDisplayName().startsWith("docker") && !networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast()  && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		{
		    			
		  	 	    	 for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) 
		  	 	    	 {
		  	 	        	  if(interfaceAddress.getAddress() instanceof Inet4Address)
		  	 	              {
		  	 	        		 SocketAddress sa = new InetSocketAddress(interfaceAddress.getBroadcast(),32005);
		  	 	        		 SocketAddress ssa = new InetSocketAddress(interfaceAddress.getAddress(),32005);
		  	 	        		 socket = new DatagramSocket(null);
		  	 	        		 socket.bind(sa);
					        	 socket.setBroadcast(true);
			 	        		 
					        	 sendsocket = new DatagramSocket(null);
		  	 	        		 sendsocket.bind(ssa);
					        	 
					        	 //System.out.println(interfaceAddress.getAddress().getHostAddress());
					        	 while (PluginEngine.isActive) 
					   	      {
					   	    	  //System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");
					   		        
					   	        //Receive a packet
					   	        byte[] recvBuf = new byte[15000];
					   	        DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
					   	        socket.receive(packet);

					   	        //Packet received
					   	        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
					   	        //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

					   	        //See if the packet holds the right command (message)
					   	        String message = new String(packet.getData()).trim();
					   	        if (message.equals("DISCOVER_FUIFSERVER_REQUEST")) 
					   	        {
					   	          //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
					   	          //MsgEventType
					   	          //MsgEventType msgType, String msgRegion, String msgAgent, String msgPlugin, String msgBody
					   	          MsgEvent me = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Broadcast discovery response.");
					   	          me.setParam("clientip", packet.getAddress().getHostAddress());
					   	       // convert java object to JSON format,
					   	      	// and returned as JSON formatted string
					   	      	  String json = gson.toJson(me);
					   	          //byte[] sendData = "DISCOVER_FUIFSERVER_RESPONSE".getBytes();
					   	          byte[] sendData = json.getBytes();
					   	          //Send a response
					   	          DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(), packet.getPort());
					   	          sendsocket.send(sendPacket);
					   	          
					   	          //peer with remote
					   	          PluginEngine.processPeer(packet.getAddress().getHostAddress(),"dummy-value");
			  	 		          //peer with remote
					   	          
					   	          //System.out.println(getClass().getName() + ">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
					   	        }
					   	      }
			 	        		 
		  	 	              }
		  	 	    	 }
		  	 	    	
		    		}
		    		
		    	}
		    	catch(Exception ex)
		    	{
		    		System.out.println("DiscoveryEngineIPv4 : DiscoveryEngineWorkerIPv4 : Run Error " + ex.toString());
		    	}
		    }
	  }

	  
}