package com.researchworx.cresco.controller.communication;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.researchworx.cresco.library.messaging.MsgEvent;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import shared.MsgEvent;

public class ActiveProducerWorker {
	private static final Logger logger = LoggerFactory.getLogger(ActiveProducerWorker.class);

	private Session sess;
	private ActiveMQConnection  conn;
	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	
	public ActiveProducerWorker(String TXQueueName, String URI)  {
		try {
			queueName = TXQueueName;
			gson = new Gson();
			conn = (ActiveMQConnection) new ActiveMQConnectionFactory(URI).createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = sess.createQueue(TXQueueName);
			producer = this.sess.createProducer(destination);
			producer.setTimeToLive(3000L);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			isActive = true;
			logger.debug("Producer Worker [{}] initialized", queueName);
		} catch (Exception e) {
			logger.error("Constructor {}", e.getMessage());
		}
	}

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
			String sendJson = gson.toJson(se);

			producer.send(this.sess.createTextMessage(sendJson));
			return true;
		} catch (JMSException jmse) {
			System.out.println("ActiveProducerWorker : sendMessage : " + se.getParams() + " : " + jmse.getMessage());
			//System.out.println(jmse.getErrorCode());
			return false;
		}
	}
}