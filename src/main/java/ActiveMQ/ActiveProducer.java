package ActiveMQ;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;


public class ActiveProducer implements Runnable
{

	private Queue TXqueue; 
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
		this.TXqueue = sess.createQueue(TXQueueName);
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
        MessageProducer producer = this.sess.createProducer(TXqueue);
        while (true) 
        {
            producer.send(this.sess.createTextMessage("from " + URI + " to " + TXqueue.getQueueName()));
            Thread.sleep(5000);
        }
    } catch (JMSException jmse) {
        System.out.println(jmse.getErrorCode());
    } catch (InterruptedException ie) {
        System.out.println(ie.getMessage());
    }
}
}