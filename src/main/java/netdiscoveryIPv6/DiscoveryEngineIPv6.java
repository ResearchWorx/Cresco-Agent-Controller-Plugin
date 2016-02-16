package netdiscoveryIPv6;

import java.net.DatagramPacket;
import java.net.Inet6Address;
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
		    public DiscoveryEngineWorkerIPv6(NetworkInterface networkInterface)
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
		    		if (!networkInterface.isLoopback() && networkInterface.isUp() && networkInterface.supportsMulticast()  && !networkInterface.isPointToPoint() && !networkInterface.isVirtual())
		    		{
		    			System.out.println("Trying interface: " + networkInterface.getDisplayName());
		    			boolean isIPv6Bound = false;
			  	 	    
		  	 	    	 for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) 
		  	 	    	 {
		  	 	        	  if(interfaceAddress.getAddress() instanceof Inet6Address)
		  	 	              {
		  	 	        		  if(!interfaceAddress.getAddress().isLinkLocalAddress())
		  	 	        		  {
		  	 	        			isIPv6Bound = true;
		  	 	        			SocketAddress sa = new InetSocketAddress(interfaceAddress.getAddress().getHostAddress(),32005);
		  	 	        			socket = new MulticastSocket(null);
		  	 	        			socket.bind(sa);
			  			     	  }
		  	 	              }
		  	 	    	 }
		  	 	    	
		  	 	    	if(isIPv6Bound)
		  	 	    	{
		  	 	    		SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),32005);
		  	 	    		socket.joinGroup(saj, networkInterface);
		  	 	    	
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
		  	 		          socket.send(sendPacket);
		  	 		          
		  	 		          // process peer
		  	 		          PluginEngine.processPeer(packet.getAddress().getHostAddress(), "dummy-value");
		  	 		          // process peer
		  	 		          
		  	 		          //System.out.println(getClass().getName() + ">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
		  	 		        }
		  	 		      }
		  	 	    	}
		  	 	    	
		    		}
		    		
		    	}
		    	catch(Exception ex)
		    	{
		    		System.out.println("DiscoveryEngineIPv6 : DiscoveryEngineWorkerIPv6 : Run Error " + ex.toString());
		    	}
		    }
	  }

	  
}