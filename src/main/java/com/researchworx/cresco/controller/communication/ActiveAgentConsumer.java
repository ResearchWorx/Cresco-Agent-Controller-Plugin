package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.commons.net.telnet.EchoOptionHandler;

import javax.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
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
					MsgEvent me = null;
					try {
						String msgPayload = new String(msg.getText());
						me = gson.fromJson(msgPayload,MsgEvent.class);
						//me = gson.fromJson(msg.getText(), MsgEvent.class);
						//me = new Gson().fromJson(msg.getText(), MsgEvent.class);
						if(me != null) {
							logger.debug("Message: {}", me.getParams().toString());
							plugin.msgIn(me);
						} else {
							logger.error("non-MsgEvent message found!");
						}
					} catch(Exception ex) {
						logger.error("MsgEvent Error  : " +  ex.getMessage());
						StringWriter errors = new StringWriter();
						ex.printStackTrace(new PrintWriter(errors));
						logger.error("MsgEvent Error Stack : " + errors.toString());

					}
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
		}
		catch (JMSException e) {
			//TODO Fix this dirty hack
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));

			if(!e.getMessage().equals("java.lang.InterruptedException")) {
				plugin.setConsumerThreadActive(false);
				logger.error("JMS Error : " + e.getMessage());
				logger.error("JMS Error : ", errors.toString());
			} else {
				logger.error("JMS Error java.lang.InterruptedException : ", errors.toString());
			}
		} catch (Exception ex) {
			logger.error("Run: {}", ex.getMessage());
			StringWriter errors = new StringWriter();
			ex.printStackTrace(new PrintWriter(errors));
			logger.error("Run Stack: {}", errors.toString());
			//return errors.toString();
			try {
				//self distruct
				Thread.sleep(5000);
				System.exit(0);
			} catch(Exception exx) {

			}
			plugin.setConsumerThreadActive(false);
		}

	}
}