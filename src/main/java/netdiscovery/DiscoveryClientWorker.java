package netdiscovery;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
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

import netbenchmark.NetBenchClient;
import shared.MsgEvent;

public class DiscoveryClientWorker 
{
private DatagramSocket c;
private Gson gson;
public Timer timer;

	public DiscoveryClientWorker(int discoveryTimeout)
	{
		gson = new Gson();
		timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
	    timer.schedule(new StopListnerTask(), discoveryTimeout);
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
		NetBenchClient nbc = new NetBenchClient();
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
				String netBenchResult = nbc.benchmarkThroughput(me.getParam("serverip"),"benchmark-throughput",3,1);
		    	if(netBenchResult != null)
		    	{
		    		ippath = ippath + "_" + netBenchResult;
		    	}
				
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
	  //Open a random port to send the package
	  c = new DatagramSocket();
	  c.setBroadcast(true);

	  byte[] sendData = "DISCOVER_FUIFSERVER_REQUEST".getBytes();

	  //Try the 255.255.255.255 first
	  try {
	    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName("255.255.255.255"), 32005);
	    c.send(sendPacket);
	    //System.out.println(getClass().getName() + ">>> Request packet sent to: 255.255.255.255 (DEFAULT)");
	  } catch (Exception e) {
	  }

	  // Broadcast the message over all the network interfaces
	  Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
	  while (interfaces.hasMoreElements()) {
	    NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();

	    if (networkInterface.isLoopback() || !networkInterface.isUp()) {
	      continue; // Don't want to broadcast to the loopback interface
	    }

	    for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
	      InetAddress broadcast = interfaceAddress.getBroadcast();
	      if (broadcast == null) {
	        continue;
	      }

	      // Send the broadcast package!
	      try {
	        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, broadcast, 32005);
	        c.send(sendPacket);
	      } catch (Exception e) {
	      }

	      //System.out.println(getClass().getName() + ">>> Request packet sent to: " + broadcast.getHostAddress() + "; Interface: " + networkInterface.getDisplayName());
	    }
	  }

	  //System.out.println(getClass().getName() + ">>> Done looping over all network interfaces. Now waiting for a reply!");

	  //Wait for a response
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
		  	}
		    catch(Exception ex)
		  	{
		  		System.out.println("in loop 1" + ex.toString());
		  	}
		  
	  	}
		  //Close the port!
	  //c.close();
	  //System.out.println("CODY : Dicsicer Client Worker Engned!");
	} 
	catch (Exception ex) 
	{
	  System.out.println("while not closed: " + ex.toString());
	}
	return discoveryList;
}
}
