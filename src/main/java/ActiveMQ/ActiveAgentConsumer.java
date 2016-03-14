package ActiveMQ;


import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import plugincore.PluginEngine;
import shared.MsgEvent;

import java.sql.Timestamp;


public class ActiveAgentConsumer implements Runnable
{
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	
	public ActiveAgentConsumer(String RXQueueName, String URI)
	{
		try
		{
			
			//ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://default?group=test)?reconnectDelay=1000&maxReconnectAttempts=30&useExponentialBackOff=false");
			//ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
			//conn = factory.createConnection();
			conn = (ActiveMQConnection) new    ActiveMQConnectionFactory(URI).createConnection();
			
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			//this.RXqueue = sess.createQueue(RXQueueName);
			this.RXqueue = sess.createQueue(RXQueueName);
			
			//Queue TXqueue = sess.createQueue(TXQueueName);
		}
		catch(Exception ex)
		{
			System.out.println("ActiveConsumer Init " + ex.toString());
		}
		
	}

	@Override
	public void run() 
	{
		Gson gson = new Gson();
		// TODO Auto-generated method stub
		//new Thread(new Sender(sess, TXqueue, RXQueueName)).start();
		try
		{
				PluginEngine.ConsumerThreadActive = true;
			
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (PluginEngine.ConsumerThreadActive) 
			{
				System.out.println("WAITING FOR MESSAGE!!!!");
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) 
				{
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
					//count++;
					//if(count++ == 10)
					//{
					//System.out.println("");
					if (me.getMsgBody().toLowerCase().equals("ping")) {
						String pingAgent = me.getParam("src_region") + "_" + me.getParam("src_agent");
						System.out.println("Sending to Agent [" + pingAgent + "]");
						MsgEvent sme = new MsgEvent(me.getMsgType(), PluginEngine.region, PluginEngine.agent, PluginEngine.plugin, "pong");
						sme.setParam("src_region", me.getParam("dst_region"));
						sme.setParam("src_agent", me.getParam("dst_agent"));
						sme.setParam("dst_region", me.getParam("src_region"));
						sme.setParam("dst_agent", me.getParam("src_agent"));
						PluginEngine.ap.sendMessage(sme);
					} else {
						java.util.Date date = new java.util.Date();
						System.out.println("\n[" + new Timestamp(date.getTime()) + "] " + me.getParam("src_region") + "_" + me.getParam("src_agent") + " sent a message.");
						System.out.print("Name of Agent to message: ");
					}
					
				}
			}
			System.out.println("Cleaning up ActiveConsumer");
			sess.close();
			conn.cleanup();
			conn.close();
		}
		catch(Exception ex)
		{
			System.out.println("Activeconsumer Run : " + ex.toString());
			//javax.jms.JMSException: java.io.EOFException
			PluginEngine.shutdown();
		}

	}
	

}