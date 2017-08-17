package com.tsdb.opsmx;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

/**
 * Proof of Concept
 * 
 * This OpenTSDB Plugin publishes data to a opsmx UDP server
 * 
 * make sure that you have 2 new settings in your opentsdb.conf:
 * tsd.plugin.skyline.host = Your opsmx host 
 * tsd.plugin.skyline.port = Your opsmx port
 * 
 */

public class OpsmxPublisher extends RTPublisher {

	
	private static final Logger LOG = LoggerFactory.getLogger(OpsmxPublisher.class);
	private DatagramSocket udpSocket;
	private MessagePack msgpack = new MessagePack();
	private int opsmxPort;
	private String[] opsmxIa_host;
	private List<String> opsmxIp;
	private static String ops=null;

	@Message
	public static class MetricDataPoint {
		public String name;
		public long timestamp;
		public double value;
		public Map<String, String> tags;
	}

	public void initialize(final TSDB tsdb) {
		LOG.info("Init opsmxPublisher");
        RtconfigManger rts=new RtconfigManger();
        ops=rts.getOpsName();
		opsmxPort = tsdb.getConfig().getInt("tsd.plugin."+ops+".port");

		opsmxIa_host = tsdb.getConfig().getString("tsd.plugin."+ops+".host").split(",");

		opsmxIp = new LinkedList<String>();
		for (int index = 0; index < opsmxIa_host.length; index++) {
			opsmxIp.add(opsmxIa_host[index]);
		}

		try {
			udpSocket = new DatagramSocket();
			LOG.info("Socket connected");
		} catch (SocketException e) {
			LOG.error("SocketException in "+ops+"Publisher initialize");
			LOG.info("SocketException in "+ops+"Publisher initialize");
		}
	}

	public Deferred<Object> shutdown() {
		return new Deferred<Object>();
	}

	public String version() {
		return "2.0.1";
	}

	public void collectStats(final StatsCollector collector) {
		LOG.info("collectStats "+ops+"Publisher");
	}

	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value,
			final Map<String, String> tags, final byte[] tsuid) {
		MetricDataPoint mdp = new MetricDataPoint();
		mdp.name = metric;
		mdp.value = value;
		mdp.timestamp = timestamp;
		mdp.tags = tags;

		System.out.println(""+ops+"Publisher.publishDataPoint() metric=" + metric);
		System.out.println(""+ops+"Publisher.publishDataPoint() value=" + value);
		System.out.println(""+ops+"Publisher.publishDataPoint()tags.host=" + tags.get("host"));

		// for (Map.Entry<String, String> entry : tags.entrySet()) {
		// String tagKey = entry.getKey();
		// String tagValue = entry.getValue();
		//
		// if (!tagKey.equals("host")) {
		// mdp.name = mdp.name + "." + tagValue;
		// }
		// }

		try {
			byte[] data = msgpack.write(mdp);
			for (int index = 0; index < opsmxIa_host.length; index++) {
			  DatagramPacket packet = new DatagramPacket(data, data.length, InetAddress.getByName(opsmxIp.get(index)),
						opsmxPort);
				udpSocket.send(packet);
				System.out.println("Data sending ...");
			}
		} catch (IOException e) {
			LOG.error("IOException in "+ops+"Publisher send" +e);
		}
		return new Deferred<Object>();
	}

	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value,
			final Map<String, String> tags, final byte[] tsuid) {

		MetricDataPoint mdp = new MetricDataPoint();
		mdp.name = metric;
		mdp.value = value;
		mdp.timestamp = timestamp;
		mdp.tags = tags;

		// for (Map.Entry<String, String> entry : tags.entrySet()) {
		// String tagKey = entry.getKey();
		// String tagValue = entry.getValue();
		//
		// if (!tagKey.equals("host")) {
		// mdp.name = mdp.name + "." + tagValue;
		// }
		// }
		try {
			byte[] data = msgpack.write(mdp);

			for (int index = 0; index < opsmxIa_host.length; index++) {
				DatagramPacket packet = new DatagramPacket(data, data.length, InetAddress.getByName(opsmxIp.get(index)),
						opsmxPort);
				udpSocket.send(packet);
				System.out.println("Data sending ...");
			}
		} catch (IOException e) {
			LOG.error("IOException in "+ops+"Publisher send" +e);
			LOG.info("SocketException in "+ops+"Publisher initialize");
		}
		return new Deferred<Object>();
	}

	public Deferred<Object> publishAnnotation(Annotation annotation) {
		return new Deferred<Object>();
	}

}
