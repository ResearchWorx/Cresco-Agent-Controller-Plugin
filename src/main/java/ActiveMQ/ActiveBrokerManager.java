package ActiveMQ;

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


public class ActiveBrokerManager implements Runnable 
{
	//private MulticastSocket socket;
	
	public ActiveBrokerManager() throws SocketException
	{
	
	}
	  
	public void shutdown()
	{
	
	}
	
	  public void run() 
	  {
		PluginEngine.ActiveBrokerManagerActive = true;
	    while(PluginEngine.ActiveBrokerManagerActive)
	    {
		  try 
		  {
			  MsgEvent dr = PluginEngine.incomingCanidateBrokers.poll();
			  if(dr != null)
			  {
				  
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