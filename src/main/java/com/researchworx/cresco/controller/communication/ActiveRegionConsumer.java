package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ActiveRegionConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	
	public ActiveRegionConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.logger = new CLogger(ActiveRegionConsumer.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		logger.debug("Initializing");
		this.plugin = plugin;
		try {
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent,brokerPasswordAgent,URI).createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			//todo Figure out if this does anything consumer.excl
			//this.RXqueue = sess.createQueue(RXQueueName + "?consumer.exclusive=true");
			this.RXqueue = sess.createQueue(RXQueueName + "?consumer.prefetchSize=10000");

			//this.RXqueue = sess.createQueue(RXQueueName);

		} catch(Exception ex) {
			logger.error("Init {}", ex.getMessage());
		}
	}

	@Override
	public void run() {
		logger.info("Starting");
		Gson gson = new Gson();
		try {
			this.plugin.setConsumerThreadRegionActive(true);
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (this.plugin.isConsumerThreadRegionActive()) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {

					MsgEvent me = null;
					try {
						me = gson.fromJson(msg.getText(), MsgEvent.class);
					} catch(Exception ex) {
						logger.error("Invalid MsgEvent Format : " + msg.getText());
					}
					if(me != null) {
					me.setParam("dst_agent",plugin.getAgent());
                    me.setParam("dst_plugin",plugin.getPluginID());

                    if(me.getParam("src_agent") == null) {
						//logger.error("src_agent = null: " + me.getParams().toString());
					}
                    else if(!me.getParam("src_agent").equals(plugin.getAgent())) {
						//logger.info("region message: " + me.getParams().toString());
					}

					logger.debug("Incoming Message Region: " + me.getParams().toString());

					this.plugin.msgIn(me);

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
					*/ }
				}
			}
			logger.debug("Cleaning up");
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Shutdown");
		} catch (Exception ex) {
			logger.error("ActiveRegionConsumer().run() " + ex.toString());
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			logger.error(sw.toString()); //
			this.plugin.setConsumerThreadRegionActive(false);
		}
	}
}