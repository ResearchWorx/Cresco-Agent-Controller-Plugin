package com.researchworx.cresco.controller.communication;

import javax.jms.*;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class ActiveAgentConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;

	public ActiveAgentConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		logger.debug("Agent Consumer initialized : Queue : " + RXQueueName);
		logger.trace("RXQueue=" + RXQueueName + " URI=" + URI + " brokerUserNameAgent=" + brokerUserNameAgent + " brokerPasswordAgent=" + brokerPasswordAgent);
		this.plugin = plugin;
		try {
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent,brokerPasswordAgent,URI).createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.RXqueue = sess.createQueue(RXQueueName);
		} catch(Exception ex) {
			plugin.getLogger().error("Init {}", ex.getMessage());
		}
	}

	@Override
	public void run() {
		logger.info("Agent Consumer started : Queue : " + RXqueue);
		Gson gson = new Gson();
		try {
			plugin.setConsumerThreadActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (plugin.isConsumerThreadActive()) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {
					logger.debug("Incoming Queue : " + RXqueue);
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
					plugin.msgIn(me);
					logger.debug("Message : "  + me.getParams().toString());
				}
                else {
					logger.trace("Agent Consumer Connection Details: Queue : " + RXqueue);
					logger.trace("isStarted=" + conn.isStarted() + " isClosed=" + conn.isClosed() + " ClientId=" + conn.getInitializedClientID());
					logger.trace("brokerName=" + conn.getBrokerName() + " username=" + conn.getConnectionInfo().getUserName() + " password=" + conn.getConnectionInfo().getPassword());
                }
			}
			logger.debug("Cleaning up ActiveAgentConsumer");
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Agent Consumer has shutdown");
		} catch (JMSException e) {
			plugin.setConsumerThreadActive(false);
		} catch (Exception ex) {
			logger.error("Run {}", ex.getMessage());
			plugin.setConsumerThreadActive(false);
		}
	}
}