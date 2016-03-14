package netdiscovery;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
		  	 		        DatagramPacket recPacket = new DatagramPacket(recvBuf, recvBuf.length);
		  	 		        
		  	 		        
		  	 		     synchronized (socket) {
		  	 		        socket.receive(recPacket); //rec broadcast packet, could be IPv6 or IPv4
		  	 		     }
		  	 		     
		  	 		     synchronized (socket) {
		  	 		    	 	DatagramPacket sendPacket = sendPacket(recPacket);
		  	 		    	 	if(sendPacket != null)
		  	 		    	 	{
		  	 		    	 		socket.send(sendPacket);
		  	 		    	 		PluginEngine.responds++;
			  	 		        
		  	 		    	 	}
		  	 		     }
		  	 		        //sendSucka(socket,packet);
		  	 		        //new Thread(new DiscoveryResponder(packet)).start();
		  	 		        
		  	 	    		}
					}	
		    	}
		    	catch(Exception ex)
		    	{
		    		System.out.println("DiscoveryEngineIPv6 : DiscoveryEngineWorkerIPv6 Run : Interface = "+ networkInterface.getDisplayName() + " : Run Error " + ex.toString());
		    		System.out.println("Thread = " + Thread.currentThread().toString());
  	 		        
		    	}
		    }
		  	private String stripIPv6Address(InetAddress address)
		  	{
		  		
		  		String sAddress = null;
		  		try
		  		{
		  			sAddress = address.getHostAddress();
			        if(sAddress.contains("%"))
			        {
			        	  String[] aScope = sAddress.split("%");
			        	  sAddress = aScope[0];
			          }
		  		}
		  		catch(Exception ex)
		  		{
		  			System.out.println("DiscoveryEngine : stripIPv6Address Error " + ex.getMessage());
		  		}
		  		return sAddress;
		  		
		  	}
		  	private String getSourceAddress(Map<String,String> intAddr, String remoteAddress)
		  	{
		  		String sAddress = null;
		  		try
		  		{
		  			for (Entry<String, String> entry : intAddr.entrySet())
		  			{
		  				String cdirAddress = entry.getKey() + "/" + entry.getValue();
		  				CIDRUtils cutil = new CIDRUtils(cdirAddress);
		  				if(cutil.isInRange(remoteAddress))
		  				{
		  					sAddress = entry.getKey();
		  				}
		  			}
		  			
		  		}
		  		catch(Exception ex)
		  		{
		  			System.out.println("DiscoveryEngine : getSourceAddress Error " + ex.getMessage());
		  		}
		  		return sAddress;
		  		
		  	}
		  	private Map<String,String> getInterfaceAddresses()
		  	{
		  		Map<String, String> intAddr = null;
		  		try
		  		{
		  			intAddr = new HashMap<String,String>();
		  			for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) 
		  		    {
		  				Short aPrefix = interfaceAddress.getNetworkPrefixLength();
		  				String hostAddress = stripIPv6Address(interfaceAddress.getAddress());
		  				intAddr.put(hostAddress, aPrefix.toString());
		  		    }
		  		}
		  		catch(Exception ex)
		  		{
		  			System.out.println("DiscoveryEngine : stripIPv6Address Error " + ex.getMessage());
		  		}
		  		return intAddr;
		  		
		  	}
		  	private synchronized DatagramPacket sendPacket(DatagramPacket packet) 
		    {
		    	synchronized (packet) 
		    	{
		    		Map<String,String> intAddr = getInterfaceAddresses();
		    		InetAddress returnAddr = packet.getAddress();
		    	
		    		String remoteAddress = stripIPv6Address(returnAddr);
		    		boolean isGlobal = !returnAddr.isSiteLocalAddress() && !returnAddr.isLinkLocalAddress();
		    		String sourceAddress = getSourceAddress(intAddr,remoteAddress); //determine send address
		    		
		    		//if((!PluginEngine.isLocal(remoteAddress)) && (isGlobal))
		    		if(!(intAddr.containsKey(remoteAddress)) && (isGlobal) && (sourceAddress != null))
			 		{
		    		//Packet received
	 		        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
	 		        //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

	 		        //See if the packet holds the right command (message)
	 		        String message = new String(packet.getData()).trim();
	 		        
	 		        MsgEvent rme = null;
	 		       //DatagramPacket sendPacket = null;
	 		      
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
	 		          me.setParam("agent_count", String.valueOf(PluginEngine.reachableAgents().size()));
	 		          
	 		          
	 		          //PluginEngine.discoveryResponse.offer(me);
	 		          //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress() +  " Sent to broker. " + PluginEngine.discoveryResponse.size());
	 		         //System.out.println(getClass().getName() + "2 " + Thread.currentThread().getId());
			  			
	 		          String json = gson.toJson(me);
					  byte[] sendData = json.getBytes();
		 		      //returnAddr = InetAddress.getByName(me.getParam("dst_ip"));
		 		      int returnPort = Integer.parseInt(me.getParam("dst_port"));
	  	 		      //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
		 		      packet.setData(sendData);
		 		      packet.setLength(sendData.length);
		 		      packet.setAddress(returnAddr);
		 		      packet.setPort(returnPort);
		 		      //sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
		 		      //socket.send(sendPacket);
		 		      
		 		      		//DatagramSocket sendSocket = new DatagramSocket();
		 		      		//sendSocket.send(sendPacket);
		 		    		//sendSocket.disconnect();
		 		    		//sendSocket.close();
		 		    		
		 		      }
		 		     
		  		}
		  		catch(Exception ex)
		  		{
		  			
		  			ex.printStackTrace(System.out);
		  			
		  		}
	 		        
	 		      
	 	   
		    	
	 		        }
		    		else
		    		{
		    			packet = null;
		    		}
		    	return packet;
			      
		    	}
		    }
		    
	  }
	  	
	  public class DiscoveryResponder implements Runnable {

			DatagramPacket packet = null;
		    Gson gson;
			

		    public DiscoveryResponder(DatagramPacket packet) {
		        this.packet = packet;
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
		 		      
		 		      		DatagramSocket sendSocket = new DatagramSocket();
		 		      		sendSocket.send(sendPacket);
		 		    		//sendSocket.disconnect();
		 		    		sendSocket.close();
		 		    		
		 		      }
		 		     
		  		}
		  		catch(Exception ex)
		  		{
		  			
		  			ex.printStackTrace(System.out);
		  			
		  		}
	 		        
	 		      
	 	   
		    	
	 		        }}
		
	  }
		
	  
}