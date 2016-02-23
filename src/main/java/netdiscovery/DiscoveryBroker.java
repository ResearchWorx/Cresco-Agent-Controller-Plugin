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
import java.net.SocketException;
import java.util.Enumeration;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;


public class DiscoveryBroker implements Runnable 
{
	//private MulticastSocket socket;
	private DatagramSocket socket;
	private Gson gson;
	public DiscoveryBroker() throws SocketException
	{
		gson = new Gson();
		socket = new DatagramSocket();
	}
	  
	public void shutdown()
	{
		socket.close();
	}
	
	  public void run() 
	  {
		PluginEngine.DiscoveryBrokerActive = true;
	    while(PluginEngine.DiscoveryBrokerActive)
	    {
		  try 
		  {
			  MsgEvent dr = PluginEngine.discoveryResponse.poll();
			  if(dr != null)
			  {
				  System.out.println(getClass().getName() + ">>>Broker Discovery packet received from " +  dr.getParam("clientip"));
  	 		      
				  String json = gson.toJson(dr);
				  byte[] sendData = json.getBytes();
	 		      InetAddress returnAddr = InetAddress.getByName(dr.getParam("clientip"));
	 		      int returnPort = Integer.parseInt(dr.getParam("clientport"));
  	 		      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
	 		      socket.send(sendPacket);
	 		     System.out.println(getClass().getName() + ">>>Broker Discovery packet sent to " +  dr.getParam("clientip"));
 	 		  }
			  else
			  {
				  Thread.sleep(1000);
			  }
			  
		  } 
		  catch (Exception ex) 
		  {
			  System.out.println("DiscoveryEngineIPv6 : Run Error " + ex.toString());
		  }
	    }
	  }
	  
	  
}