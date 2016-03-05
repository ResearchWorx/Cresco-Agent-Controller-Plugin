package netdiscovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;

public class DiscoveryClientWorkerIPv6 
{
private DatagramSocket c;
//private MulticastSocket c;
private Gson gson;
public Timer timer;
public int discoveryTimeout;
public String multiCastNetwork;

	public DiscoveryClientWorkerIPv6(int discoveryTimeout, String multiCastNetwork)
	{
		gson = new Gson();
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
		this.discoveryTimeout = discoveryTimeout;
		this.multiCastNetwork = multiCastNetwork;
	    
	}
	
class StopListnerTask extends TimerTask {
		
	    public void run() 
	    {
	    	//System.out.println("CODY: Closing Shop!");
	    	c.close();
	    	timer.cancel();
	    }
	  }

	public List<MsgEvent> discover()
	{
		List<MsgEvent> discoveryList = null;
	// Find the server using UDP broadcast
	try {
		discoveryList = new ArrayList<MsgEvent>();
		
	  // Broadcast the message over all the network interfaces
	  Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
	  while (interfaces.hasMoreElements()) {
	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();

	    //if (networkInterface.isLoopback() || !networkInterface.isUp()) {
	    if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
		continue; // Don't want to broadcast to the loopback interface
	    }
	    
	    if(networkInterface.supportsMulticast())
	    {
	    	 for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
	         {
	          try {
	        	  
	        	  //if((interfaceAddress.getAddress() instanceof Inet6Address) && !interfaceAddress.getAddress().isLinkLocalAddress())
	        	  if((interfaceAddress.getAddress() instanceof Inet6Address))
	        	  {  
	        		  
	        		 //c = new MulticastSocket(null);
	        		 c = new DatagramSocket(null);
	        		 //c.setReuseAddress(true);
	        		  //System.out.println("prebind1");
	        		 String hostAddress = interfaceAddress.getAddress().getHostAddress();
	        		 if(hostAddress.contains("%"))
	        		 {
	        			 String[] hostScope = hostAddress.split("%");
	        			 hostAddress = hostScope[0];
	        		 }
	        		 SocketAddress sa = new InetSocketAddress(hostAddress,0);
		        	  //System.out.println("prebind2");
		        		
		        	 c.bind(sa);
		        	  //System.out.println("prebind3");
		        		
		        	 //start timer to clost discovery
		        	  timer = new Timer();
		        	  timer.schedule(new StopListnerTask(), discoveryTimeout);
		        	 
		        	  MsgEvent sme = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Discovery request.");
		        	  sme.setParam("broadcast_ip",multiCastNetwork);
		 		      sme.setParam("src_region",PluginEngine.region);
		 		      sme.setParam("src_agent",PluginEngine.agent);
		 		      
		 		      
  	 		          //me.setParam("clientip", packet.getAddress().getHostAddress());

  	 		      	// convert java object to JSON format,
  	 		      	// and returned as JSON formatted string
  	 		      	  String sendJson = gson.toJson(sme);

  	 		         byte[] sendData = sendJson.getBytes();
  	 		           
		          DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName(multiCastNetwork), 32005);
	      	     c.send(sendPacket);
	      	     System.out.println(getClass().getName() + ">>> Request packet sent to: " + multiCastNetwork +  ": from : " + interfaceAddress.getAddress().getHostAddress());
	      	    
	      	  while(!c.isClosed())
	    	  {
	      		  
	      		  try
	    		  {
	    			  byte[] recvBuf = new byte[15000];
	    			  DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
	    			  c.receive(receivePacket);

	    			  //We have a response
	    			  System.out.println(getClass().getName() + ">>> Broadcast response from server: " + receivePacket.getAddress().getHostAddress());

	    			  //Check if the message is correct
	    			  //System.out.println(new String(receivePacket.getData()));
	    	  
	    			  String json = new String(receivePacket.getData()).trim();
	    			  //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
	    		  		try
	    		  		{
	    		  			MsgEvent me = gson.fromJson(json, MsgEvent.class);
	    		  			if(me != null)
	    		  			{
	    		  				//System.out.println("RESPONCE: " + me.getParamsString());
		    		  			
	    		  				 String remoteAddress = receivePacket.getAddress().getHostAddress();
	    		        		 if(remoteAddress.contains("%"))
	    		        		 {
	    		        			 String[] remoteScope = hostAddress.split("%");
	    		        			 remoteAddress = remoteScope[0];
	    		        		 }
	    		        		//System.out.println("Client IP = " + me.getParam("clientip") + " Remote IP= " + receivePacket.getAddress().getHostAddress());
	    		        		//System.out.println("Client IP = " + remoteAddress  + " Remote IP= " + me.getParam("dst_ip"));
		    		  				//if(!me.getParam("dst_ip").equals(remoteAddress))
	    		  				//{
	    		  					//System.out.println("SAME HOST");
	    		  					//System.out.println(me.getParamsString() + receivePacket.getAddress().getHostAddress());
	    		  					//me.setParam("dst_ip", remoteAddress);
	    		  					//me.setParam("src_ip", remoteAddress);
	    		        		    me.setParam("dst_ip", remoteAddress);
	    		        		    me.setParam("dst_region", me.getParam("src_region"));
	    		        		    me.setParam("dst_agent", me.getParam("src_agent"));
	    		        		    
	    		  					discoveryList.add(me);
	    		  				//}
	    		  			}
	    		  		}
	    		  		catch(Exception ex)
	    		  		{
	    		  			System.out.println("DiscoveryClientWorker in loop 0" + ex.toString());
	    		  		}
	    		  	}
	    		  	catch(SocketException ex)
	    		  	{
	    		  		//eat message.. this should happen
	    		  		//System.out.println("should eat exception");
	    		  	}
	    		    catch(Exception ex)
	    		  	{
	    		  		System.out.println("in loop 1" + ex.toString());
	    		  	}
	    		  
	    	  	}
	    		  //Close the port!
	              
	              }
	      	  }
	          catch (IOException ie)
	          {
	        	  //eat exception
	        	  //System.out.println("DiscoveryClientWorkerIPv6 : getDiscoveryMap IO Error : " + ie.toString());
	          }
	      	  catch (Exception e) 
	      	  {
	      		  System.out.println("DiscoveryClientWorkerIPv6 : getDiscoveryMap Error : " + e.toString());
	      	  }	
	         
	         }
	    }
	    
	    
	    	    
	    }
      

	  //Wait for a response
	  
	  //c.close();
	  //System.out.println("CODY : Dicsicer Client Worker Engned!");
	  }
	} 
	catch (Exception ex) 
	{
	  System.out.println("while not closed: " + ex.toString());
	}
	return discoveryList;
}
}
