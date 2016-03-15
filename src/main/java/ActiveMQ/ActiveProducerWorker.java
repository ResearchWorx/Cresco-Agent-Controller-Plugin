package ActiveMQ;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.google.gson.Gson;

import shared.MsgEvent;


public class ActiveProducerWorker
{

	public boolean ActiveProducer;
	private Session sess;
	private ActiveMQConnection  conn;
	private Destination destination;
	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	
public ActiveProducerWorker(String TXQueueName, String URI) 
{
	try
	{
		queueName = TXQueueName;
		gson = new Gson();
		//ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
		conn = (ActiveMQConnection) new ActiveMQConnectionFactory(URI).createConnection();
		//conn = factory.createConnection();
		conn.start();
		this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		//this.TXqueue = sess.createQueue(TXQueueName);
		destination = sess.createQueue(TXQueueName);
		producer = this.sess.createProducer(destination);
		producer.setTimeToLive(3000L);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        isActive = true;
	}
	catch (JMSException e)
	{
		System.out.println("ActiveConsumer JMS " + e.getMessage());
	}
	catch(Exception ex)
	{
		System.out.println("ActiveConsumer Init " + ex.toString());
	}
}

public boolean shutdown()
{
	boolean isShutdown = false;
    try {
    	producer.close();
    	sess.close();
		conn.cleanup();
		conn.close();
        System.out.println("Destroyed ActiveProducerTask [" + queueName + "]");
		System.out.print("Name of Agent to message [q to quit]: ");
        isShutdown = true;
    } catch (JMSException jmse) {
		jmse.printStackTrace();
		System.out.println(jmse.getMessage());
        System.out.println(jmse.getLinkedException().getMessage());
    }
    return isShutdown;
    
	
}
public boolean sendMessage(MsgEvent se) {
	boolean isSent = false;
    try {
    	String sendJson = gson.toJson(se);

    	producer.send(this.sess.createTextMessage(sendJson));
        
    } catch (JMSException jmse) {
        System.out.println(jmse.getErrorCode());
    }
    return isSent;
}
}