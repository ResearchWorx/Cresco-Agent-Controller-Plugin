package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class ActiveAgentConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;

	public ActiveAgentConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		logger = new CLogger(ActiveAgentConsumer.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
		logger.debug("Queue: {}", RXQueueName);
		logger.trace("RXQueue=" + RXQueueName + " URI=" + URI + " brokerUserNameAgent=" + brokerUserNameAgent + " brokerPasswordAgent=" + brokerPasswordAgent);
		this.plugin = plugin;
		int retryCount = 10;
		while (((conn == null) || !conn.isStarted()) && retryCount-- > 0) {
			try {
				conn = (ActiveMQConnection) new ActiveMQConnectionFactory(brokerUserNameAgent, brokerPasswordAgent, URI).createConnection();
				conn.start();
				sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
				RXqueue = sess.createQueue(RXQueueName);
				break;
			} catch (JMSException je) {
				try {
				    logger.error("JMSException: {}", je.getMessage());
					logger.debug("brokerUserNameAgent={}, brokerPasswordAgent={},URI={}", brokerUserNameAgent, brokerPasswordAgent, URI);
                    logger.error("Retrying initialization");
					Thread.sleep(500);
				} catch (InterruptedException e) {
					logger.trace("Initialization was interrupted");
				}
			} catch (Exception ex) {
				logger.error("Constructor: {}", ex.getMessage());
				break;
			}
		}
	}

	@Override
	public void run() {
		logger.info("Queue: {}", RXqueue);
		Gson gson = new Gson();
		try {
			plugin.setConsumerThreadActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (plugin.isConsumerThreadActive()) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {
					logger.debug("Incoming Queue: {}", RXqueue);
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
					plugin.msgIn(me);
					logger.debug("Message: {}", me.getParams().toString());
				}
                else {
					logger.trace("Queue Details: {}", RXqueue);
					logger.trace("isStarted=" + conn.isStarted() + " isClosed=" + conn.isClosed() + " ClientId=" + conn.getInitializedClientID());
					logger.trace("brokerName=" + conn.getBrokerName() + " username=" + conn.getConnectionInfo().getUserName() + " password=" + conn.getConnectionInfo().getPassword());
                }
			}
			logger.debug("Cleaning up");
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Shutdown complete");
		} catch (JMSException e) {
			plugin.setConsumerThreadActive(false);
		} catch (Exception ex) {
			logger.error("Run: {}", ex.getMessage());
			plugin.setConsumerThreadActive(false);
		}
	}
}