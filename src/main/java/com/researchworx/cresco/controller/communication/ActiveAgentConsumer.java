package com.researchworx.cresco.controller.communication;

import javax.jms.*;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveAgentConsumer implements Runnable {
	private Launcher plugin;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	private static final Logger logger = LoggerFactory.getLogger(ActiveAgentConsumer.class);

	public ActiveAgentConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		logger.debug("Agent Consumer initialized : Queue : " + RXQueueName);
        logger.trace("RXQueue=" + RXQueueName + " URI=" + URI + " brokerUserNameAgent=" + brokerPasswordAgent + " brokerPasswordAgent=" + brokerPasswordAgent);
		this.plugin = plugin;
		try {
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent,brokerPasswordAgent,URI).createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.RXqueue = sess.createQueue(RXQueueName);
		} catch(Exception ex) {
			logger.error("Init {}", ex.getMessage());
		}
	}

	@Override
	public void run() {
		logger.info("Agent Consumer started : Queue : " + RXqueue);
		Gson gson = new Gson();
		try {
			this.plugin.setConsumerThreadActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (this.plugin.isConsumerThreadActive()) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
					this.plugin.msgIn(me);
					logger.debug("Incoming Queue : " + RXqueue + " Message : "  + me.getParams().toString());
					/*
					if (me.getMsgBody().toLowerCase().equals("ping")) {
						String pingAgent = me.getParam("src_region") + "_" + me.getParam("src_agent");
						logger.info("Sending to Agent [{}]", pingAgent);
						MsgEvent sme = new MsgEvent(me.getMsgType(), PluginEngine.region, PluginEngine.agent, PluginEngine.plugin, "pong");
						sme.setParam("src_region", me.getParam("dst_region"));
						sme.setParam("src_agent", me.getParam("dst_agent"));
						sme.setParam("dst_region", me.getParam("src_region"));
						sme.setParam("dst_agent", me.getParam("src_agent"));
						PluginEngine.ap.sendMessage(sme);
					} else {
						logger.debug("[{}] {}_{} sent a message.", new Timestamp(new Date().getTime()), me.getParam("src_region"), me.getParam("src_agent"));
						System.out.print("Name of Agent to message [q to quit]: ");
					}
					*/
				}
                else {
                    logger.trace("Agent Consumer Connection Details:");
                    logger.trace("isStarted=" + conn.isStarted() + " isClosed=" + conn.isClosed() + " ClientId=" + conn.getInitializedClientID());
                    logger.trace("brokerName=" + conn.getBrokerName() + " username=" + conn.getConnectionInfo().getUserName() + " password=" + conn.getConnectionInfo().getPassword());
                }


			}
			logger.debug("Cleaning up ActiveAgentConsumer");
			this.sess.close();
			this.conn.cleanup();
			this.conn.close();
			logger.debug("Agent Consumer has shutdown");
		} catch (JMSException e) {
			this.plugin.setConsumerThreadActive(false);
		} catch (Exception ex) {
			logger.error("Run {}", ex.getMessage());
			this.plugin.setConsumerThreadActive(false);
		}
	}
}