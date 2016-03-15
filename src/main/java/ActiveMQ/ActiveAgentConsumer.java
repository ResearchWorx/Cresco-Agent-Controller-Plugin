package ActiveMQ;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;
import shared.MsgEvent;

import java.util.Date;
import java.sql.Timestamp;

public class ActiveAgentConsumer implements Runnable {
	private Queue RXqueue; 
	private Session sess;
	private ActiveMQConnection conn;
	private static final Logger logger = LoggerFactory.getLogger(ActiveAgentConsumer.class);

	public ActiveAgentConsumer(String RXQueueName, String URI) {
		logger.debug("Agent Consumer initialized");
		try {
			conn = (ActiveMQConnection)new ActiveMQConnectionFactory(URI).createConnection();
			conn.start();
			this.sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.RXqueue = sess.createQueue(RXQueueName);
		} catch(Exception ex) {
			logger.error("Init {}", ex.getMessage());
		}
	}

	@Override
	public void run() {
		logger.info("Agent Consumer started");
		Gson gson = new Gson();
		try {
			PluginEngine.ConsumerThreadActive = true;
			MessageConsumer consumer = sess.createConsumer(RXqueue);
			while (PluginEngine.ConsumerThreadActive) {
				TextMessage msg = (TextMessage) consumer.receive(1000);
				if (msg != null) {
					MsgEvent me = gson.fromJson(msg.getText(), MsgEvent.class);
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
				}
			}
			logger.debug("Cleaning up ActiveAgentConsumer");
			sess.close();
			conn.cleanup();
			conn.close();
			logger.debug("Agent Consumer has shutdown");
		} catch (Exception ex) {
			logger.error("Run {}", ex.getMessage());
			PluginEngine.ConsumerThreadActive = false;
		}
	}
}