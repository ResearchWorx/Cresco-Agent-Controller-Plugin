package com.researchworx.cresco.controller.communication;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;

public class ActiveRegionConsumer implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	private ActiveMQSslConnectionFactory connf;
	
	public ActiveRegionConsumer(Launcher plugin, String RXQueueName, String URI, String brokerUserNameAgent, String brokerPasswordAgent) {
		this.logger = new CLogger(ActiveRegionConsumer.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		logger.debug("Initializing Pre");
		this.plugin = plugin;
		try {
			//conn = (ActiveMQConnection)new ActiveMQConnectionFactory(brokerUserNameAgent,brokerPasswordAgent,URI).createConnection();
			connf = new ActiveMQSslConnectionFactory(URI);
			connf.setKeyAndTrustManagers(plugin.getCertificateManager().getKeyManagers(),plugin.getCertificateManager().getTrustManagers(), new SecureRandom());
			conn = (ActiveMQConnection) connf.createConnection();

			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			//might not need this
			this.RXqueue = sess.createQueue(RXQueueName + "?consumer.exclusive=true");
			//this.RXqueue = sess.createQueue(RXQueueName + "?consumer.prefetchSize=1");
			//this.RXqueue = sess.createQueue(RXQueueName);

			logger.debug("Initializing Post");


		} catch(Exception ex) {
			logger.error("Init {}", ex.getMessage());
		}
	}

	@Override
	public void run() {
		logger.info("Initialized");
		Gson gson = new Gson();
		try {

			this.plugin.setConsumerThreadRegionActive(true);

			MessageConsumer consumer = sess.createConsumer(RXqueue);


			while (this.plugin.isConsumerThreadRegionActive()) {

				TextMessage msg = (TextMessage) consumer.receive();
				if (msg != null) {

					MsgEvent me = null;
					try {
						me = gson.fromJson(msg.getText(), MsgEvent.class);
					} catch(Exception ex) {
						logger.error("Invalid MsgEvent Format : " + msg.getText());
					}
					if(me != null) {

					//logger.debug("Incoming Message Region: " + me.getParams().toString());

					this.plugin.msgIn(me);
					}
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
		logger.error("ActiveResion Comsumer Finished");
	}
}