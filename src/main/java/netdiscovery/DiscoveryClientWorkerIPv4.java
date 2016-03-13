package netdiscovery;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Timer;
import java.util.TimerTask;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;

public class DiscoveryClientWorkerIPv4 
{
private DatagramSocket c;
private Gson gson;
public Timer timer;

	public DiscoveryClientWorkerIPv4(int discoveryTimeout)
	{
		gson = new Gson();
		timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
	    timer.schedule(new StopListnerTask(), discoveryTimeout);
	}
	
	public class IPv4Responder implements Runnable {

		DatagramSocket socket = null;
	    DatagramPacket packet = null;
	    String hostAddress = null;
	    Gson gson;
		

	    public IPv4Responder(DatagramSocket socket, DatagramPacket packet, String hostAddress) {
	        this.socket = socket;
	        this.packet = packet;
	        this.hostAddress = hostAddress;
	        gson = new Gson();
	    }

	    public void run() 
	    {
	    	
	    	//We have a response
			  //System.out.println(getClass().getName() + ">>> Broadcast response from server: " + receivePacket.getAddress().getHostAddress());

			  //Check if the message is correct
			  //System.out.println(new String(receivePacket.getData()));
	  
			  String json = new String(packet.getData()).trim();
			  //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
		  		try
		  		{
		  			MsgEvent me = gson.fromJson(json, MsgEvent.class);
		  			if(me != null)
		  			{
		  				 //System.out.println("RESPONCE: " + me.getParamsString());
  		  			
		  				 //if(!me.getParam("dst_ip").equals(receivePacket.getAddress().getHostAddress()))
		  				 //{
		  					//System.out.println("SAME HOST");
		  					//System.out.println(me.getParamsString() + receivePacket.getAddress().getHostAddress());
		  					//me.setParam("serverip", receivePacket.getAddress().getHostAddress());
		  					me.setParam("dst_ip", packet.getAddress().getHostAddress());
		  					me.setParam("dst_region", me.getParam("src_region"));
		        		    me.setParam("dst_agent", me.getParam("src_agent"));
		        		    PluginEngine.incomingCanidateBrokers.offer(me);
		  					//discoveryList.add(me);
		  					//System.out.println(getClass().getName() + ">>> Added server: " + receivePacket.getAddress().getHostAddress() + " to broker list");

		  			}
		  		}
		  		catch(Exception ex)
		  		{
		  			System.out.println("in loop 0" + ex.toString());
		  		}
	    	
	    	
	    }
	}
	
	
class StopListnerTask extends TimerTask {
		
	    public void run() 
	    {
	    	//System.out.println("CODY: Closing Shop!");
	    	c.close();
	    	timer.cancel();
	    }
	  }
	
	public void discover()
	{
	
		// Find the server using UDP broadcast
	try {
	  //Open a random port to send the package
	  c = new DatagramSocket();
	  c.setBroadcast(true);

	  //byte[] sendData = "DISCOVER_FUIFSERVER_REQUEST".getBytes();
	  MsgEvent sme = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Discovery request.");
	  sme.setParam("broadcast_ip","255.255.255.0");
	  sme.setParam("src_region",PluginEngine.region);
	  sme.setParam("src_agent",PluginEngine.agent);
	  
	  String sendJson = gson.toJson(sme);
	  byte[] sendData = sendJson.getBytes();
	  
	  //Try the 255.255.255.255 first
	  try {
	    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName("255.255.255.255"), 32005);
	    c.send(sendPacket);
	    //System.out.println(getClass().getName() + ">>> Request packet sent to: 255.255.255.255 (DEFAULT)");
	  } catch (Exception e) {
	  }

	  // Broadcast the message over all the network interfaces
	  Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
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
	      if(interfaceAddress.getAddress() instanceof Inet6Address)
          {
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
	
}
}
