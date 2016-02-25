package ActiveMQ;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;


public class ActiveProducer implements Runnable
{
	public boolean ActiveProducer;
	//private Queue TXqueue;
	private Destination destination; 
	private Session sess;
	private String URI;

public ActiveProducer(String TXQueueName, String URI) 
{
	try
	{
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
		Connection conn = factory.createConnection();
		conn.start();
		this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		//this.TXqueue = sess.createQueue(TXQueueName);
		//this.TXqueue = sess.cr createQueue(TXQueueName);
		// Create the destination (Topic or Queue)
        destination = sess.createTopic(TXQueueName);

        // Create a MessageProducer from the Session to the Topic or Queue
        
		
		this.URI = URI;
		
	}
	catch(Exception ex)
	{
		System.out.println("ActiveConsumer Init " + ex.toString());
	}
}

	
@Override
public void run() {
    try {
    	ActiveProducer = true;
        //MessageProducer producer = this.sess.createProducer(TXqueue);
        MessageProducer producer = sess.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        
        while (ActiveProducer) 
        {
        	TextMessage message = sess.createTextMessage("from " + URI + " to ");
            //producer.send(this.sess.createTextMessage("from " + URI + " to " + TXqueue.getQueueName()));
        	producer.send(message);
            //Thread.sleep(5000);
        }
    } catch (JMSException jmse) {
        System.out.println(jmse.getErrorCode());
    } //catch (InterruptedException ie) {
      //  System.out.println(ie.getMessage());
    //}
}
}