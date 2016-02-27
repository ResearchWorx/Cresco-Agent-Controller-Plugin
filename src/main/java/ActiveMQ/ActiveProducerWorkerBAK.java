package ActiveMQ;

import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;


public class ActiveProducerWorkerBAK implements Runnable
{

	public boolean ActiveProducer;
	private Queue TXqueue; 
	private Session sess;
	private String URI;
	private ActiveMQConnection  conn;
	private Destination destination;
	private boolean isActiveMessage = false;
	private boolean isActiveProducer = false;
	

	class ClearProducerTask extends TimerTask 
			{
				
			    public void run() 
			    {
			    	if(isActiveMessage)
			    	{
			    		isActiveMessage = false;
			    	}
			    	else
			    	{
			    		isActiveProducer = false;
			    	}
			    }
			  }


	
public ActiveProducerWorkerBAK(String TXQueueName, String URI) 
{
	try
	{
		//ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
		conn = (ActiveMQConnection) new    ActiveMQConnectionFactory(URI).createConnection();
		//conn = factory.createConnection();
		conn.start();
		this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		//this.TXqueue = sess.createQueue(TXQueueName);
		destination = sess.createQueue(TXQueueName);
		
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
        MessageProducer producer = this.sess.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        
        System.out.println("Started Producer Thread :" + Thread.currentThread());
        
        while (isActiveProducer) 
        {
            producer.send(this.sess.createTextMessage("from " + URI + " to " ));
            //Thread.sleep(5000);
            
        }
        System.out.println("CODY 0 Ended Producer Thread :" + Thread.currentThread());
        sess.close();
        conn.destroyDestination((ActiveMQDestination) destination);
        conn.cleanup();
        conn.doCleanup(true);
        conn.stop();
        System.out.println("CODY 1 Ended Producer Thread :" + Thread.currentThread());
    } catch (JMSException jmse) {
        System.out.println(jmse.getErrorCode());
    } //catch (InterruptedException ie) {
      //  System.out.println(ie.getMessage());
    //}
}
}