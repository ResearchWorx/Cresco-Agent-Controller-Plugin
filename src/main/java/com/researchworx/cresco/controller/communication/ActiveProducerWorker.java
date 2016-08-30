package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class ActiveProducerWorker {
	private CLogger logger;
	private Session sess;
	private ActiveMQConnection  conn;
	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	
	public ActiveProducerWorker(Launcher plugin, String TXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent)  {
		this.logger = new CLogger(ActiveProducerWorker.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		try {
			queueName = TXQueueName;
			gson = new Gson();
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent, brokerPasswordAgent, URI).createConnection();
			conn.start();
			sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = sess.createQueue(TXQueueName);
			producer = sess.createProducer(destination);
			producer.setTimeToLive(300000L);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			isActive = true;
			logger.debug("Initialized", queueName);
		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
		}
	}
//BDB\em{?}
	public boolean shutdown() {
		boolean isShutdown = false;
		try {
			producer.close();
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Producer Worker [{}] has shutdown", queueName);
			isShutdown = true;
		} catch (JMSException jmse) {
			logger.error(jmse.getMessage());
			logger.error(jmse.getLinkedException().getMessage());
		}
		return isShutdown;


	}
	public boolean sendMessage(MsgEvent se) {
		try {
			producer.send(sess.createTextMessage(gson.toJson(se)));
			logger.trace("sendMessage to : " + queueName + " message:" + se.getParams());
			return true;
		} catch (JMSException jmse) {
			logger.error("sendMessage: {} : {}", se.getParams(), jmse.getMessage());
			return false;
		}
	}
}