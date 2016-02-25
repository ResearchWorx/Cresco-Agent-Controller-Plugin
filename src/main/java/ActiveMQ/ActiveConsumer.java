package ActiveMQ;


import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import plugincore.PluginEngine;


public class ActiveConsumer implements Runnable
{
	private Queue RXqueue; 
	private Session sess;
	
	public ActiveConsumer(String RXQueueName, String URI)
	{
		try
		{
			//ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://default?group=test)?reconnectDelay=1000&maxReconnectAttempts=30&useExponentialBackOff=false");
			ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
			Connection conn = factory.createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
		// TODO Auto-generated method stub
		//new Thread(new Sender(sess, TXqueue, RXQueueName)).start();
		try
		{
			PluginEngine.ConsumerThreadActive = true;
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (PluginEngine.ConsumerThreadActive) 
			{
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) 
				{
					System.out.println("");
					System.out.println(msg.getText());
					System.out.println("");
				}
			}
		}
		catch(Exception ex)
		{
			System.out.println("Activeconsumer Run : " + ex.toString());
		}

	}
	

}