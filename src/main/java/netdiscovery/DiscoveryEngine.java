package netdiscovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import netdiscovery.DiscoveryClientWorkerIPv6.IPv6Responder;
import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;


public class DiscoveryEngine implements Runnable 
{
	//private MulticastSocket socket;
	
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
	     
	    	Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
	 	  while (interfaces.hasMoreElements()) 
	 	  {
	 	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
	 	    new Thread(new DiscoveryEngineWorker(networkInterface)).start();
	      }
	 	  
	 	 PluginEngine.DiscoveryActive = true;
	     
	 	  
	    } 
	    catch (Exception ex) 
	    {
	    	System.out.println("DiscoveryEngineIPv6 : Run Error " + ex.toString());
	    }
	  }

	  
	  public static DiscoveryEngine getInstance() {
	    return DiscoveryThreadHolder.INSTANCE;
	  }
	  
	  private static class DiscoveryThreadHolder {

	    private static final DiscoveryEngine INSTANCE = new DiscoveryEngine();
	  }

	  class DiscoveryEngineWorker implements Runnable 
	  {
		  private NetworkInterface networkInterface;
		  private MulticastSocket socket;
		  
		  public DiscoveryEngineWorker(NetworkInterface networkInterface)
		    {
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
		    		/*
		    		System.out.println("DiscoveryEngineWorkerIPv6 : Check " + this.networkInterfaceName);
		    		System.out.println("isLoopBack: " +networkInterface.isLoopback());
		    		System.out.println("isUP: " + networkInterface.isUp());
		    		System.out.println("supportsMulticast: " + networkInterface.supportsMulticast());
		    		System.out.println("isPointToPoint: " + networkInterface.isPointToPoint());
		    		System.out.println("isVirtual: " + networkInterface.isVirtual());
		    		*/
		    		//if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
				    {
		    			System.out.println("DiscoveryEngineWorkerIPv6 : Init " + networkInterface.getDisplayName());
		    			SocketAddress sa = null;
		    			if(PluginEngine.isIPv6)
		    			{
		    				sa = new InetSocketAddress("[::]",32005);
		    			}
		    			else
		    			{
		    				sa = new InetSocketAddress("0.0.0.0",32005);
		    			}
		  	 	        socket = new MulticastSocket(null);
		  	 	        socket.bind(sa);
		  	 	        System.out.println("Bound to interface : " + networkInterface.getDisplayName() + " address: [::]");
		  	 	        			
			  			 
		  	 	    	
		  	 	        if(PluginEngine.isIPv6)
		    			{
		  	 	        	//find to network and site multicast addresses
		    				//SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),32005);
		  	 	    	    //socket.joinGroup(saj, networkInterface);
		  	 	    	    SocketAddress saj2 = new InetSocketAddress(Inet6Address.getByName("ff02::1:c"),32005);
		  	 	    		socket.joinGroup(saj2, networkInterface);
		    			}
		  	 	    		
		  	 	    		while (PluginEngine.isActive) 
		  	 	    		{
		  	 		    	  //System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");
		  	 			        
		  	 		        //Receive a packet
		  	 		        byte[] recvBuf = new byte[15000];
		  	 		        DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
		  	 		        
		  	 		        socket.receive(packet); //rec broadcast packet, could be IPv6 or IPv4
		  	 		        new Thread(new DiscoveryResponder(socket,packet,networkInterface)).start();
		  	 		        
		  	 	    		}
					}	
		    	}
		    	catch(Exception ex)
		    	{
		    		System.out.println("DiscoveryEngineIPv6 : DiscoveryEngineWorkerIPv6 : Interface = "+ networkInterface.getDisplayName() + " : Run Error " + ex.toString());
		    		System.out.println("Thread = " + Thread.currentThread().toString());
  	 		        
		    	}
		    }
	  }

	  private boolean sendDiscovery(DatagramPacket sendPacket) throws IOException
	  {
		  boolean isSent = false;

		     //System.out.println(getClass().getName() + "3 " + Thread.currentThread().getId());
		      DatagramSocket sendSocket = null;
		      //try
		      //{
		    	  sendSocket = new DatagramSocket();
		    	  
		    	  //SocketAddress sa = new InetSocketAddress(returnAddr,returnPort);
		    	  //sendSocket.connect(sa);
		      	  sendSocket.send(sendPacket);
		      	  sendSocket.close();
		      	  isSent = true;
		      //}
		   	  //catch(Exception ex)
		      //{
		   		  //eat error
	  			//System.out.println(getClass().getName() + " DiscoveryEngine sendDiscovery Error : " + ex.getMessage());
	  		  //}
		    
		  return isSent;
	  }
	  
	  private Inet6Address getInet6AddressByName(String host) throws UnknownHostException, SecurityException
	  {
	      for(InetAddress addr : InetAddress.getAllByName(host))
	      {
	          if(addr instanceof Inet6Address)
	              return (Inet6Address)addr;
	      }
	      throw new UnknownHostException("No IPv6 address found for " + host);
	  }
	  
	  public class DiscoveryResponder implements Runnable {

			DatagramSocket socket = null;
		    DatagramPacket packet = null;
		    NetworkInterface networkInterface = null;
		    Gson gson;
			

		    public DiscoveryResponder(DatagramSocket socket, DatagramPacket packet, NetworkInterface networkInterface) {
		        this.socket = socket;
		        this.packet = packet;
		        this.networkInterface = networkInterface;
		        
		        gson = new Gson();
		    }

		    public void run() 
		    {
		    	InetAddress returnAddr = packet.getAddress();
		    	boolean isGlobal = !returnAddr.isSiteLocalAddress() && !returnAddr.isLinkLocalAddress();
	  			
	 		    String remoteAddress = returnAddr.getHostAddress();
		          if(remoteAddress.contains("%"))
		          {
		        	  String[] remoteScope = remoteAddress.split("%");
		        	  remoteAddress = remoteScope[0];
		          }
		    	if((!PluginEngine.isLocal(remoteAddress)) && (isGlobal))
	 		        {
	 		         
	 		        //Packet received
	 		        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
	 		        //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

	 		        //See if the packet holds the right command (message)
	 		        String message = new String(packet.getData()).trim();
	 		        
	 		       MsgEvent rme = null;
	 		       DatagramPacket sendPacket = null;
	 		       //
	 		      DatagramSocket sendSocket = null;
	    			SocketAddress sab = null;
	    			SocketAddress sac = null;
	    			
	 		       
	 		       //
	 		       
		  		try
		  		{
		  		    //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
	 		        //check that the message can be marshaled into a MsgEvent
		  			//System.out.println(getClass().getName() + "0.0 " + Thread.currentThread().getId());
		  			try
			  		{
		  				rme = gson.fromJson(message, MsgEvent.class);
			  		}
		  			catch(Exception ex)
			  		{
			  			System.out.println(getClass().getName() + " fail to marshal discovery " + ex.getMessage());
			  		}
		  			//System.out.println(getClass().getName() + "0.1 " + Thread.currentThread().getId());
		  			if (rme!=null) 
	 		        {
		  			  
	 		          
	 		         //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
			  			
	 		          
	 		          MsgEvent me = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Broadcast discovery response.");
	 		          me.setParam("dst_region",rme.getParam("src_region"));
	 		          me.setParam("dst_agent",rme.getParam("src_agent"));
	 		          me.setParam("src_region",PluginEngine.region);
	 		          me.setParam("src_agent",PluginEngine.agent);
	 		          me.setParam("dst_ip", remoteAddress);
	 		          me.setParam("dst_port", String.valueOf(packet.getPort()));
	 		          //PluginEngine.discoveryResponse.offer(me);
	 		          //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress() +  " Sent to broker. " + PluginEngine.discoveryResponse.size());
	 		         //System.out.println(getClass().getName() + "2 " + Thread.currentThread().getId());
			  			
	 		          String json = gson.toJson(me);
					  byte[] sendData = json.getBytes();
		 		      //returnAddr = InetAddress.getByName(me.getParam("dst_ip"));
		 		      int returnPort = Integer.parseInt(me.getParam("dst_port"));
	  	 		      //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
		 		      sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
		 		      //socket.send(sendPacket);
		 		      
		 		      boolean isSent = false;
		 		      for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) 
		 		      {
		 		    	  InetAddress outAddr = interfaceAddress.getAddress();
		 		    	  boolean isGlobalLocal = !outAddr.isSiteLocalAddress() && !outAddr.isLinkLocalAddress();
		 		    	 if((outAddr instanceof Inet6Address) && isGlobalLocal)
			        	 {
		 		    		if(!isSent)
		 		    		{
		 		    			//DatagramSocket sendSocket = new DatagramSocket();
		 		    			/*
		 		    			DatagramSocket sendSocket = new DatagramSocket(null);
		 		    			SocketAddress sab = new InetSocketAddress(outAddr,0);
		 		    			sendSocket.bind(sab);
		 		    			SocketAddress sac = new InetSocketAddress(returnAddr,returnPort);
		 		    			sendSocket.connect(sac);
		 		    			sendSocket.send(sendPacket);
		 		    			sendSocket.close();
		 		    			isSent = true;
		 		    			*/
		 		    			
		 		    			sendSocket = new DatagramSocket(null);
		 		    			sab = new InetSocketAddress(outAddr,0);
		 		    			sendSocket.bind(sab);
		 		    			sac = new InetSocketAddress(returnAddr,returnPort);
		 		    			sendSocket.connect(sac);
		 		    			sendSocket.send(sendPacket);
		 		    			sendSocket.close();
		 		    			isSent = true;
		 		    			
		 		    			
		 		    		}
				 		     
			        	 }
		 		    	 
		 		      }
		 		          
		 		     
		 		      
		 		      /*
	  	 		   else {
	  	                // we're connected
	  	                packetAddress = p.getAddress();
	  	                if (packetAddress == null) {
	  	                    p.setAddress(connectedAddress);
	  	                    p.setPort(connectedPort);
	  	                } else if ((!packetAddress.equals(connectedAddress)) ||
	  	                           p.getPort() != connectedPort) {
	  	                    throw new IllegalArgumentException("connected address " +
	  	                                                       "and packet address" +
	  	                                                       " differ");
	  	                }
	  	 		      */
	 		        }
	  	 		      /*
	  	 		      boolean isSent = false;
	  	 		      int failCount = 1;
	  	 		      while(!isSent)
	  	 		      {
	  	 		    	if(sendDiscovery(sendPacket)) //try to send message
	  	 		    	{
	  	 		    		isSent = true;
	  	 		    	}
	  	 		    	else
	  	 		    	{
	  	 		    		if(failCount == 5)
	  	 		    		{
	  	 		    			isSent = true;
	  	 		    			//System.out.println("Giving up on responding to host " + returnAddr.getHostAddress());
	  	 		    		}
	  	 		    		failCount++;
	  	 		    	}
	  	 		    	Thread.sleep(1000);
	  	 		      }
	  	 		      */
	  	 		      
	 		        //}  
		  			
		  			//sendDiscovery(sendPacket);
		  		}
		  		catch(Exception ex)
		  		{
		  			/*
		  			System.out.println(getClass().getName() + " Discovery Respond Failed 0: " + ex.getMessage());
		  			System.out.println(getClass().getName() + " Discovery Respond Failed 1: " + ex.getLocalizedMessage() );
		  			for(StackTraceElement se : ex.getStackTrace())
		  			{
		  				System.out.println(getClass().getName() + "Method : " + se.getMethodName());
		  				System.out.println(getClass().getName() + "Class : " + se.getClassName());
		  				System.out.println(getClass().getName() + "Line : " + se.getLineNumber());
		  			}
		  			*/
		  			
		  			/*
		  			 DatagramSocket sendSocket = null;
		 		    SocketAddress sab = null;
		 		    SocketAddress sac = null;
		 		    			
		  			 */
		  			
		  			ex.printStackTrace(System.out);
		  			System.out.println("IncomingInterface : " + networkInterface.getDisplayName());
		  			boolean isIPv6 = false;
		  			if(returnAddr instanceof Inet6Address)
		        	{
		  				isIPv6 = true;
		        	}
		  			System.out.println("Remote Address : "  + packet.getAddress().getHostAddress() + " Global: " + isGlobal + " isIPv6: " + isIPv6);
		  			InetSocketAddress sabs = (InetSocketAddress)sab;
		  			InetSocketAddress sacs = (InetSocketAddress)sac;
		  			boolean sab6 = false;
		  			if(sabs.getAddress() instanceof Inet6Address)
		        	{
		  				sab6 = true;
		        	}
		  			boolean sac6 = false;
		  			if(sacs.getAddress() instanceof Inet6Address)
		        	{
		  				sac6 = true;
		        	}
		  			System.out.println("sab : " + sabs.getAddress().getHostAddress() + " port:" + sabs.getPort() + " isIPv6: " + sab6); 
		  			System.out.println("sac : " + sacs.getAddress().getHostAddress() + " port:" + sacs.getPort() + " isIPv6: " + sac6); 
		  			System.out.println("send socket connected: " + sendSocket.isConnected());
		  			System.out.println("send socket closed: " + sendSocket.isClosed());
		  			System.out.println("send socket bound: " + sendSocket.isBound());
		  			
		  			
		  			//Inet6Address addr = new Inet6Address(str);
		  			
		  			//ex.getStackTrace()
			  		
		  		}
	 		        
	 		      
	 	   
		    	
	 		        }}
		}
		
	  
}