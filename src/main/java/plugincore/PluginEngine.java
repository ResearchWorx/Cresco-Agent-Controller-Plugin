package plugincore;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.*;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import shared.MsgEvent;

import javax.jms.*;
import javax.jms.Queue;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PluginEngine {
    
	public static boolean clientDiscoveryActive = false;
	public static boolean DiscoveryActive = false;
	public static boolean isActive = false;
	public static boolean NetBenchEngineActive = false;
	
	public static String region = "reg";
	public static String agent = "agent";
	public static String plugin = "pl";
	
	public String getName()
	{
		return "Name";
				
	}
	public String getVersion()
	{
		return "Name";
				
	}
	public void msgIn(MsgEvent command)
	{
		
	}
	public void shutdown()
	{
		
	}
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue,ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region,String agent, String plugin)  
	{
		return true;
	}
	public static void main(String[] args) throws Exception {
    	
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	rootLogger.setLevel(Level.toLevel("debug"));
    	
    	
        if (args.length < 2) {
            System.out.println("Usage: [RXqueue] [TXqueue]");
            return;
        }
        
        //boolean isBroker = Boolean.parseBoolean(args[0]);
        //boolean isAgent = Boolean.parseBoolean(args[1]);
        String RXQueueName = args[0];
        String TXQueueName = args[1];
        
        //java.net.BindException: Address already in use
        //61616
        //if(portAvailable(61616))
        if(portAvailable(1099))
        {
        BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setBrokerName(args[0]);
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://localhost:0"));
        connector.setDiscoveryUri(new URI("multicast://default?group=test"));
        broker.addConnector(connector);
        broker.start();
        }
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://default?group=test)?reconnectDelay=1000&maxReconnectAttempts=30&useExponentialBackOff=false");
        Connection conn = factory.createConnection();
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue RXqueue = sess.createQueue(RXQueueName);
        Queue TXqueue = sess.createQueue(TXQueueName);

        new Thread(new Sender(sess, TXqueue, RXQueueName)).start();

        MessageConsumer consumer = sess.createConsumer(RXqueue);
        while (true) {
            TextMessage msg = (TextMessage) consumer.receive(1000);
            if (msg != null) {
                System.out.println("");
                System.out.println(msg.getText());
                System.out.println("");
            }
        }
        
    }

    private static class Sender implements Runnable {
        private Session sess;
        private Queue queue;
        private String name;
        public Sender(Session sess, Queue queue, String name) {
            this.sess = sess;
            this.queue = queue;
            this.name = name;
        }
        @Override
        public void run() {
            try {
                MessageProducer producer = this.sess.createProducer(queue);
                while (true) {
                    producer.send(this.sess.createTextMessage("from " + this.name + " to " + queue.getQueueName()));
                    Thread.sleep(5000);
                }
            } catch (JMSException jmse) {
                System.out.println(jmse.getErrorCode());
            } catch (InterruptedException ie) {
                System.out.println(ie.getMessage());
            }
        }
    }
    
    public static boolean portAvailable(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
}