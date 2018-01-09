package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;

import javax.jms.*;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ActiveProducerWorker {
	private String producerWorkerName;
	private CLogger logger;
	private Session sess;
	private ActiveMQConnection  conn;
	private ActiveMQSslConnectionFactory connf;

	private MessageProducer producer;
	private Gson gson;
	public boolean isActive;
	private String queueName;
	private Destination destination;
	
	public ActiveProducerWorker(Launcher plugin, String TXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent)  {
		this.logger = new CLogger(ActiveProducerWorker.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.producerWorkerName = UUID.randomUUID().toString();
		try {
			queueName = TXQueueName;
			gson = new Gson();
			//conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent, brokerPasswordAgent, URI).createConnection();
			connf = new ActiveMQSslConnectionFactory(URI);
			connf.setKeyAndTrustManagers(plugin.getCertificateManager().getKeyManagers(),plugin.getCertificateManager().getTrustManagers(), new SecureRandom());
			conn = (ActiveMQConnection) connf.createConnection();
			conn.start();
			sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			destination = sess.createQueue(TXQueueName);
			producer = sess.createProducer(destination);
			producer.setTimeToLive(300000L);
			//producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
			int pri = 0;

			/*
			CONFIG,
        	DISCOVER,
        	ERROR,
        	EXEC,
        	GC,
        	INFO,
        	KPI,
        	LOG,
        	WATCHDOG;
			 */

			String type = se.getMsgType().toString();

			switch (type) {
				case "CONFIG":  pri = 10;
					break;
				case "EXEC":  pri = 10;
					break;
				case "WATCHDOG":  pri = 7;
					break;
				case "KPI":  pri = 0;
					break;
				default: pri = 4;
					break;
			}

			//producer.send(message, DeliveryMode.NON_PERSISTENT, 3, 0);
			producer.send(sess.createTextMessage(gson.toJson(se)), DeliveryMode.NON_PERSISTENT, pri, 0);
			producer.send(sess.createTextMessage(gson.toJson(se)));
			logger.trace("sendMessage to : {} : from : {}", queueName, producerWorkerName);
			return true;
		} catch (JMSException jmse) {
			logger.error("sendMessage: jmse {} : {}", se.getParams(), jmse.getMessage());
			return false;
		}
	}
}