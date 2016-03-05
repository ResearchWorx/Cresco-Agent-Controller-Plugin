package netdiscovery;

import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;

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
		  private String networkInterfaceName;
		    public DiscoveryEngineWorker(NetworkInterface networkInterface)
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
		  	 	        System.out.println("Bound to interface : " + networkInterfaceName + " address: [::]");
		  	 	        			
			  			 
		  	 	    	
		  	 	        if(PluginEngine.isIPv6)
		    			{
		  	 	        	//find to network and site multicast addresses
		    				SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),32005);
		  	 	    	    socket.joinGroup(saj, networkInterface);
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
		  	 		        
		  	 		        if(!PluginEngine.isLocal(packet.getAddress().getHostAddress()))
		  	 		        {
		  	 		         
		  	 		        //Packet received
		  	 		        System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
		  	 		        //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

		  	 		        //See if the packet holds the right command (message)
		  	 		        String message = new String(packet.getData()).trim();
		  	 		        
		  	 		        MsgEvent rme = null;
		    		  		try
		    		  		{
		    		  			//check that the message can be marshaled into a MsgEvent
		    		  			rme = gson.fromJson(message, MsgEvent.class);
		    		  		}
		    		  		catch(Exception ex)
		    		  		{
		    		  			System.out.println(getClass().getName() + " fail to marshal discovery");
		    		  		}
		  	 		        if (rme!=null) 
		  	 		        {
		    		  	    
		  	 		          String remoteAddress = packet.getAddress().getHostAddress();
	    		        	  if(remoteAddress.contains("%"))
	    		        	  {
	    		        			 String[] remoteScope = remoteAddress.split("%");
	    		        			 remoteAddress = remoteScope[0];
	    		        	  }
		  	 		          MsgEvent me = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Broadcast discovery response.");
		  	 		          me.setParam("dst_region",rme.getParam("src_region"));
		  	 		          me.setParam("dst_agent",rme.getParam("src_agent"));
				 		      me.setParam("src_region",PluginEngine.region);
				 		      me.setParam("src_agent",PluginEngine.agent);
				 		      me.setParam("dst_ip", remoteAddress);
		  	 		          me.setParam("dst_port", String.valueOf(packet.getPort()));
		  	 		          PluginEngine.discoveryResponse.offer(me);
		  	 		          //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress() +  " Sent to broker. " + PluginEngine.discoveryResponse.size());
			  	 		      
		  	 		        }
		  	 	    		}
		  	 	    		else
		  	 	    		{
		  	 	    			//local address do nothing
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