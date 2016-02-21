package netdiscoveryIPv6;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import com.google.gson.Gson;

import shared.MsgEvent;

public class DiscoveryClientWorkerIPv6 
{
private DatagramSocket c;
//private MulticastSocket c;
private Gson gson;
public Timer timer;
public int discoveryTimeout;

	public DiscoveryClientWorkerIPv6(int discoveryTimeout)
	{
		gson = new Gson();
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
		this.discoveryTimeout = discoveryTimeout;
	    
	}
	
class StopListnerTask extends TimerTask {
		
	    public void run() 
	    {
	    	//System.out.println("CODY: Closing Shop!");
	    	c.close();
	    	timer.cancel();
	    }
	  }

	public Map<String,String> getDiscoveryMap()
	{
		Map<String,String> dhm = null;
		try
		{
			Map<String, ArrayList<String>> tmphm = new HashMap<String,ArrayList<String>>();
			dhm = new HashMap<String,String>();
			List<MsgEvent> dlist = discover();
			for(MsgEvent me : dlist)
			{
				String agentpath = me.getMsgRegion() + "_" + me.getMsgAgent();
				String ippath = me.getParam("clientip") + "_" + me.getParam("serverip");
				//Benchmark connection here.. will have to do self-terminating code at some point
				/*
				String netBenchResult = nbc.benchmarkThroughput(me.getParam("serverip"),"benchmark-throughput",3,1);
		    	if(netBenchResult != null)
		    	{
		    		ippath = ippath + "_" + netBenchResult;
		    	}
				*/
				//
				if(!tmphm.containsKey(agentpath))
				{
					ArrayList<String> newList = new ArrayList<String>();
					newList.add(ippath);
					tmphm.put(agentpath, newList);
				}
				else
				{
					ArrayList<String> newList = tmphm.get(agentpath);
					if(!newList.contains(ippath))
					{
						newList.add(ippath);
						tmphm.put(agentpath, newList);
					}
					
				}
			}
			
			if(!tmphm.isEmpty())
			{
				StringBuilder keylist = new StringBuilder();
				for(Entry<String, ArrayList<String>> entry : tmphm.entrySet()) 
				{
					String agentpath = entry.getKey();
					keylist.append(agentpath + ",");
					ArrayList<String> ipList = entry.getValue();
					//System.out.println(agentpath);
					StringBuilder sb = new StringBuilder();
		        
					for(String ip : ipList)
					{
						//System.out.println(ip);
						sb.append(ip + ",");
					}
					String ips = sb.substring(0,sb.length() -1);
					dhm.put(agentpath, ips);
					// do what you have to do here
					// In your case, an other loop.
				}
				
				String keyfound = keylist.substring(0, keylist.length() -1);
				dhm.put("discoveredagents", keyfound);
			}
			
			
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClient : getDiscovery : " + ex.toString());
		}
		
		return dhm;
	}
	
	public List<MsgEvent> discover()
	{
		List<MsgEvent> discoveryList = null;
	// Find the server using UDP broadcast
	try {
		discoveryList = new ArrayList<MsgEvent>();
		
	  // Broadcast the message over all the network interfaces
	  Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
	  while (interfaces.hasMoreElements()) {
	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();

	    if (networkInterface.isLoopback() || !networkInterface.isUp()) {
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
		        		
		        	 SocketAddress sa = new InetSocketAddress(interfaceAddress.getAddress(),0);
		        	  //System.out.println("prebind2");
		        		
		        	 c.bind(sa);
		        	  //System.out.println("prebind3");
		        		
		        	 //start timer to clost discovery
		        	  timer = new Timer();
		        	  timer.schedule(new StopListnerTask(), discoveryTimeout);
		        	 
		        	 byte[] sendData = "DISCOVER_FUIFSERVER_REQUEST".getBytes();
	          
	      	    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName("255.255.255.255"), 32005);
	      	    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName("ff05::1:c"), 32005);
	      	    //DatagramPacket sendPacket2 = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName("ff02::1:c"), 32005);
	      	    //DatagramPacket sendPacket3 = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName("ff01::1:c"), 32005);
	      	   
	      	    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName("ff02::1:c"), 32005);
	      	    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, GROUP, PORT);
	      	    c.send(sendPacket);
	      	    //System.out.println(getClass().getName() + ">>> Request packet sent to: 255.255.255.255 (DEFAULT)");
	      	   System.out.println(getClass().getName() + ">>> Request packet sent to: ff05::1:c : from : " + interfaceAddress.getAddress());
	      	   /*
	      	  System.out.println("prebind0 " + interfaceAddress.getAddress());
        	  System.out.println("locallink " + interfaceAddress.getAddress().isLinkLocalAddress());
    		  System.out.println("global " + interfaceAddress.getAddress().isMCGlobal());
    		  System.out.println("linklocal " + interfaceAddress.getAddress().isMCLinkLocal());
    		  System.out.println("isNodelocal " + interfaceAddress.getAddress().isMCNodeLocal());
    		  System.out.println("multicast " + interfaceAddress.getAddress().isMulticastAddress());
    		  System.out.println("site " + interfaceAddress.getAddress().isSiteLocalAddress());
    		  System.out.println("any " + interfaceAddress.getAddress().isAnyLocalAddress());
    		  System.out.println("loop " + interfaceAddress.getAddress().isLoopbackAddress());
    		  */
	      	  
    		  
	      	  while(!c.isClosed())
	    	  {
	      		  
	      		  try
	    		  {
	    			  byte[] recvBuf = new byte[15000];
	    			  DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
	    			  c.receive(receivePacket);

	    			  //We have a response
	    			  //System.out.println(getClass().getName() + ">>> Broadcast response from server: " + receivePacket.getAddress().getHostAddress());

	    			  //Check if the message is correct
	    			  //System.out.println(new String(receivePacket.getData()));
	    	  
	    			  String json = new String(receivePacket.getData()).trim();
	    			  //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
	    		  		try
	    		  		{
	    		  			MsgEvent me = gson.fromJson(json, MsgEvent.class);
	    		  			if(me != null)
	    		  			{
	    		  				if(!me.getParam("clientip").equals(receivePacket.getAddress().getHostAddress()))
	    		  				{
	    		  					//System.out.println("SAME HOST");
	    		  					//System.out.println(me.getParamsString() + receivePacket.getAddress().getHostAddress());
	    		  					me.setParam("serverip", receivePacket.getAddress().getHostAddress());
	    		  					discoveryList.add(me);
	    		  				}
	    		  			}
	    		  		}
	    		  		catch(Exception ex)
	    		  		{
	    		  			System.out.println("in loop 0" + ex.toString());
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
