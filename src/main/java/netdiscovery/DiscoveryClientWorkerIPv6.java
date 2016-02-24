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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
	      	    //System.out.println(getClass().getName() + ">>> Request packet sent to: " + multiCastNetwork +  ": from : " + interfaceAddress.getAddress().getHostAddress());
	      	    
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
	    		  				//System.out.println("RESPONCE: " + me.getParamsString());
		    		  			
	    		  				 String remoteAddress = receivePacket.getAddress().getHostAddress();
	    		        		 if(remoteAddress.contains("%"))
	    		        		 {
	    		        			 String[] remoteScope = hostAddress.split("%");
	    		        			 remoteAddress = remoteScope[0];
	    		        		 }
	    		        		//System.out.println("Client IP = " + me.getParam("clientip") + " Remote IP= " + receivePacket.getAddress().getHostAddress());
	    		        		System.out.println("Client IP = " + remoteAddress  + " Remote IP= " + me.getParam("dst_ip"));
		    		  				//if(!me.getParam("dst_ip").equals(remoteAddress))
	    		  				//{
	    		  					//System.out.println("SAME HOST");
	    		  					//System.out.println(me.getParamsString() + receivePacket.getAddress().getHostAddress());
	    		  					//me.setParam("dst_ip", remoteAddress);
	    		  					//me.setParam("src_ip", remoteAddress);
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
