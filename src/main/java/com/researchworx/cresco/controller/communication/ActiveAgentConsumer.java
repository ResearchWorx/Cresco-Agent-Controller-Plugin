package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.security.SecureRandom;

public class ActiveAgentConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue;
	private Session sess;
	private ActiveMQConnection conn;
	private ActiveMQSslConnectionFactory connf;


	public ActiveAgentConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) throws JMSException {
		//logger = new CLogger(ActiveAgentConsumer.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), Logger.INFO);
		this.logger = new CLogger(ActiveAgentConsumer.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
		logger.debug("Queue: {}", RXQueueName);
		logger.trace("RXQueue=" + RXQueueName + " URI=" + URI + " brokerUserNameAgent=" + brokerUserNameAgent + " brokerPasswordAgent=" + brokerPasswordAgent);
		this.plugin = plugin;
		int retryCount = 10;

		connf = new ActiveMQSslConnectionFactory(URI);
		connf.setKeyAndTrustManagers(plugin.getCertificateManager().getKeyManagers(),plugin.getCertificateManager().getTrustManagers(), new SecureRandom());
		conn = (ActiveMQConnection) connf.createConnection();
		conn.start();
		sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		RXqueue = sess.createQueue(RXQueueName);

	}

	@Override
	public void run() {
		logger.trace("Queue: {}", RXqueue);
		Gson gson = new Gson();
		try {
			plugin.setConsumerThreadActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (plugin.isConsumerThreadActive()) {
				TextMessage msg = (TextMessage) consumer.receive();
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
			logger.error("JMS Error : " + e.getMessage());
		} catch (Exception ex) {
			logger.error("Run: {}", ex.getMessage());
			plugin.setConsumerThreadActive(false);
		}
	}
}