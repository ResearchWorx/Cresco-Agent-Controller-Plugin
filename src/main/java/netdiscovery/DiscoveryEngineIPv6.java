package netdiscovery;

import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;


public class DiscoveryEngineIPv6 implements Runnable 
{
	//private MulticastSocket socket;
	private Gson gson;
	public DiscoveryEngineIPv6()
	{
		gson = new Gson();
	}
	  
	public void shutdown()
	{
		//socket.close();
	}
	
	  public void run() {
	    try {
	     
	    	Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
	 	  while (interfaces.hasMoreElements()) 
	 	  {
	 	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
	 	    new Thread(new DiscoveryEngineWorkerIPv6(networkInterface)).start();
	      }
	 	  
	 	 PluginEngine.DiscoveryActiveIPv6 = true;
	     
	 	  
	    } 
	    catch (Exception ex) 
	    {
	    	System.out.println("DiscoveryEngineIPv6 : Run Error " + ex.toString());
	    }
	  }

	  
	  public static DiscoveryEngineIPv6 getInstance() {
	    return DiscoveryThreadHolder.INSTANCE;
	  }
	  
	  private static class DiscoveryThreadHolder {

	    private static final DiscoveryEngineIPv6 INSTANCE = new DiscoveryEngineIPv6();
	  }

	  class DiscoveryEngineWorkerIPv6 implements Runnable 
	  {
		  private NetworkInterface networkInterface;
		  private MulticastSocket socket;
		  private String networkInterfaceName;
		    public DiscoveryEngineWorkerIPv6(NetworkInterface networkInterface)
		    {
		    	this.networkInterfaceName = networkInterface.getDisplayName();
		    	this.networkInterface = networkInterface;
		    	
		    }
		    public void shutdown()
			{
				socket.close();
			}
		    public void run() 
		    {
		    	try
		    	{
		    		
		    		//if (!networkInterface.getDisplayName().startsWith("docker") && !networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast()  && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		//if (!networkInterface.getDisplayName().startsWith("docker") && !networkInterface.getDisplayName().startsWith("veth"))
		    		//if (!networkInterface.getDisplayName().startsWith("veth"))
		    		//if (networkInterface.getDisplayName().startsWith("em1") && !networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		//if (networkInterface.getDisplayName().startsWith("docker") && !networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
					{
		    			System.out.println("DiscoveryEngineWorkerIPv6 : Init " + this.networkInterfaceName);
				    	boolean isIPv6Bound = true;
			  	 	    
		  	 	    	SocketAddress sa = new InetSocketAddress("[::]",32005);
		  	 	        socket = new MulticastSocket(null);
		  	 	        socket.bind(sa);
		  	 	        System.out.println("IPv6 Bound to interface : " + networkInterfaceName + " address: [::]");
		  	 	        			
			  			 
		  	 	    	
		  	 	    	if(isIPv6Bound)
		  	 	    	{
		  	 	    		SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),32005);
		  	 	    	    socket.joinGroup(saj, networkInterface);
		  	 	    	    SocketAddress saj2 = new InetSocketAddress(Inet6Address.getByName("ff02::1:c"),32005);
		  	 	    		socket.joinGroup(saj2, networkInterface);
		  	 	    		//System.out.println(getClass().getName() + ">>> Bind2");
		  	 			    //SocketAddress saj3 = new InetSocketAddress(Inet6Address.getByName("ff01::1:c"),32005);
		  	 	    		//socket.joinGroup(saj3, networkInterface);
		  	 	    		//System.out.println(getClass().getName() + ">>> Bind1");
		  	 			    
		  	 	    		
		  	 	    		
		  	 	    		while (PluginEngine.isActive) 
		  	 	    		{
		  	 		    	  //System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");
		  	 			        
		  	 		        //Receive a packet
		  	 		        byte[] recvBuf = new byte[15000];
		  	 		        DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
		  	 		        
		  	 		        socket.receive(packet);
		  	 		        //check if this is a local address
		  	 		        System.out.println("saj " + saj);
		  	 		        System.out.println("saj2 " + saj2);
		  	 		        System.out.println("incoming " + packet.getSocketAddress());
		  	 		        System.out.println("Thread = " + Thread.currentThread().toString());
		  	 		        
		  	 		        
		  	 		        if(!PluginEngine.isLocal(packet.getAddress().getHostAddress()))
		  	 		        {
		  	 		         
		  	 		        //Packet received
		  	 		        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
		  	 		        //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

		  	 		        //See if the packet holds the right command (message)
		  	 		        String message = new String(packet.getData()).trim();
		  	 		        //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
		  	 		        MsgEvent rme = null;
		    		  		try
		    		  		{
		    		  			rme = gson.fromJson(message, MsgEvent.class);
		    		  		}
		    		  		catch(Exception ex)
		    		  		{
		    		  			System.out.println(getClass().getName() + " fail to marshal discovery");
		    		  		}
		  	 		        //if (message.equals("DISCOVER_FUIFSERVER_REQUEST")) 
		    		  	    if (rme!=null) 
		  	 		        {
		    		  	      System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
			  	 		        
		  	 		          //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
		  	 		          //MsgEventType
		  	 		          //MsgEventType msgType, String msgRegion, String msgAgent, String msgPlugin, String msgBody
		  	 		          MsgEvent me = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Broadcast discovery response.");
		  	 		          me.setParam("clientip", packet.getAddress().getHostAddress());

		  	 		      	// convert java object to JSON format,
		  	 		      	// and returned as JSON formatted string
		  	 		      	  String json = gson.toJson(me);
		  	 		          //byte[] sendData = "DISCOVER_FUIFSERVER_RESPONSE".getBytes();
		  	 		       try{
		  	 		    	  
		  	 		    	  byte[] sendData = json.getBytes();
		  	 		    	  InetAddress returnAddr = packet.getAddress();
		  	 		    	  int returnPort = packet.getLength();
			  	 		      //Send a response
		  	 		          //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(), packet.getPort());
		  	 		    	  DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
	  	 		              socket.send(sendPacket);
	  	 		              
		  	 		       	  }
		  	 		          catch(Exception ex)
		  	 		          {
		  	 		        	System.out.println("DE Sending remote " + getClass().getName() + " interface= " + networkInterfaceName + " >>> Socket Error ERROR " + ex.toString());
		  	 		        	System.out.println("Thread = " + Thread.currentThread().toString());
			  	 		          
		  	 		          }
		  	 		          // process peer
		  	 		    try{
		  	 		    	
		  	 		       	   //String hsAddr = packet.getAddress().getHostAddress();
		  	 		          //PluginEngine.processPeer(hsAddr, "dummy-value");
		  	 		 }
	  	 		          catch(Exception ex)
	  	 		          {
	  	 		        	System.out.println("DE Process Peer " + getClass().getName() + " interface= " + networkInterfaceName + " >>> PEER ERROR " + ex.toString());
	  	 		        	System.out.println("Thread = " + Thread.currentThread().toString());
		  	 		         
	  	 		          }
		  	 		          // process peer
		  	 		          
		  	 		          //System.out.println(getClass().getName() + ">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
		  	 		        }
		  	 	    		}
		  	 	    		
		  	 	    		else
		  	 	    		{
		  	 	    			//local address
		  	 	    			//System.out.println("Local Address")
		  	 	    		}
		  	 		      }
		  	 	    	}
		  	 	    	
		    		}
		    		
		    	}
		    	catch(Exception ex)
		    	{
		    		System.out.println("DiscoveryEngineIPv6 : DiscoveryEngineWorkerIPv6 : Interface = "+ networkInterfaceName + " : Run Error " + ex.toString());
		    		System.out.println("Thread = " + Thread.currentThread().toString());
  	 		        
		    	}
		    }
	  }

	  
}