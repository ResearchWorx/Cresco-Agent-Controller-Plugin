package com.researchworx.cresco.controller.communication;

import javax.jms.*;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveRegionConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	//private static final Logger logger = LoggerFactory.getLogger(ActiveRegionConsumer.class);
	
	public ActiveRegionConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		logger.debug("Region Consumer initialized");
		this.plugin = plugin;
		try {
			//conn = (ActiveMQConnection)new ActiveMQConnectionFactory(URI).createConnection();
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
		logger.info("Region Consumer started");
		Gson gson = new Gson();
		try {
			this.plugin.setConsumerThreadRegionActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (this.plugin.isConsumerThreadRegionActive()) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
					this.plugin.msgIn(me);
					logger.debug("Incoming Message Region: " + me.getParams().toString());

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
						logger.trace("[{}] {}_{} sent a message.", new Timestamp(new Date().getTime()), me.getParam("src_region"), me.getParam("src_agent"));
						System.out.print("Name of Agent to message [q to quit]: ");
					}
					*/
				}
			}
			logger.debug("Cleaning up ActiveRegionConsumer");
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Region Consumer has shutdown");
		} catch (Exception ex) {
			logger.error("Run {}", ex.toString());
			this.plugin.setConsumerThreadRegionActive(false);
		}
	}
}