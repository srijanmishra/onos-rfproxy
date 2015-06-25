package org.routeflow.rfproxy.IPC.JeroMQIPC;

import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.routeflow.rfproxy.IPC.IPC.IPCMessage;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageFactory;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageProcessor;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageService;
import org.routeflow.rfproxy.IPC.Tools.fields;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;

import org.slf4j.Logger;

public class JeroMQIPCMessageService extends IPCMessageService implements fields {
	
	public static String INTERNAL_SEND_CHANNEL="inproc://sender";
	public static String INTERNAL_PUBLISH_CHANNEL="inproc://channeler";
	private Logger logger;
	private String address;
	private Context ctx;
	private Socket sender;
	private final Lock ready = new ReentrantLock(true);

	/**
	 * Creates and starts an IPC message service using JeroMQ.
	 * 
	 * @param address
	 *            the address and port of the server in the format
	 *            address:port
	 * @param id
	 *            the ID of this IPC service user
	 */
	public JeroMQIPCMessageService(String address, String id, boolean bind, Logger log) {
		this.set_id(id);
		this.address = address;
		ready.lock();
		this.logger = log;
		this.ctx = ZMQ.context(1);
		this.sender = this.ctx.socket(ZMQ.PAIR);

		this.sender.bind(INTERNAL_SEND_CHANNEL); //#TODO: Check for failure

		/** Creates and run a new thread for capture new messages */
		Thread thread = new Thread("mainWorker") {
			public void run() {
				mainWorker(address, bind);
			}
		};

		thread.start();

		try {
			thread.join();
		} catch (InterruptedException e) {
			this.logger.debug("mainWorker Thread creation error!");
		}
	}

	/**
	 * Listen to messages. Empty messages are built using the factory, populated
	 * based on the received data and sent to processing by the processor. The
	 * method can be blocking or not.
	 * 
	 * @param channelId
	 *            the channel to listen to messages on
	 * 
	 * @param factory
	 *            the message factory
	 * 
	 * @param processor
	 *            the message processor
	 * 
	 * @param block
	 *            true if blocking behavior is desired, false otherwise
	 */
	public void listen(final String channelId, final IPCMessageFactory factory,
			final IPCMessageProcessor processor, boolean block) {
		
		/** Creates and run a new thread for capture new messages */
		Thread thread = new Thread("subWorker") {
			public void run() {
				subWorker(channelId, factory, processor);
			}
		};

		thread.start();

		if (block) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				this.logger.debug("subWorker Thread creation error!");
			}
		}
	};

	/**
	 * Send a message to another user on a channel.
	 * 
	 * @param channelId
	 *            the channel to send the message to
	 * 
	 * @param to
	 *            the user to send the message to
	 * 
	 * @param msg
	 *            the message
	 * 
	 * @return true if the message was sent, false otherwise
	 */
	public boolean send(String channelId, String to, IPCMessage msg) {

		String type = String.valueOf(msg.get_type());

		String payload = msg.to_bson();

		this.sender.send(to, ZMQ.SNDMORE);
        this.sender.send(channelId, ZMQ.SNDMORE);
        this.sender.send(type, ZMQ.SNDMORE);
        this.sender.send(payload);

		return true;
	};

	/** 
	 * This worker manages the external socket. It proxies messages to send out 
	 * the external socket, and external messages are proxied into the internal pub-sub 
	 * system. We need to do this extra layer because each socket in ZMQ must be
	 * created and managed by exactly one thread. Since the usage model of IPC is that
	 * the main thread sends messages, but listening threads handle incoming messages,
	 * the actual external socket must be managed by yet its own thread. 
	 * @param address
	 *        	  the address and port of the server in the format
	 *            address:port
	 */
	public void mainWorker(String serverAddress, boolean bind) {
		Socket external = this.ctx.socket(ZMQ.ROUTER);
		external.setIdentity(this.id.getBytes());

		if (bind) {
			external.bind(serverAddress); // #TODO: Check for failure
		} else {
			external.connect(serverAddress);
			external.setRouterMandatory(true);
		}

		Socket mailbox = this.ctx.socket(ZMQ.PAIR);
		mailbox.connect(INTERNAL_SEND_CHANNEL);

		Socket publisher = this.ctx.socket(ZMQ.PUB);
		publisher.bind(INTERNAL_PUBLISH_CHANNEL);

		this.ready.unlock();

		Poller poller = new Poller(2);
		poller.register(external, Poller.POLLIN);
		poller.register(mailbox, Poller.POLLIN);

		while (true) {
			
			if (poller.pollin(0)) {
				String addressFrame = external.recvStr();
				String channelFrame = external.recvStr();

				publisher.send(channelFrame, ZMQ.SNDMORE);
				publisher.send(addressFrame, ZMQ.SNDMORE);
			}

			if (poller.pollin(1)) {
				do {
					String address = mailbox.recvStr();
					String channel = mailbox.recvStr();
					String type = mailbox.recvStr();
					String payload = mailbox.recvStr();

					boolean retry = false;
					for (int attempt = bind ? 1 : 30; attempt > 0; --attempt) {
						external.send(address, ZMQ.SNDMORE);
                   		external.send(channel, ZMQ.SNDMORE);
                    	external.send(type, ZMQ.SNDMORE);
                    	external.send(payload);
                    }
				} while(false);

			}
		}
	}

	public void subWorker(final String channelId, final IPCMessageFactory factory,
			final IPCMessageProcessor processor) {
		this.ready.unlock();

		Socket subscriber = this.ctx.socket(ZMQ.SUB);
		subscriber.subscribe(channelId.getBytes());
		subscriber.connect(INTERNAL_PUBLISH_CHANNEL);

		while(true) {
			String address = subscriber.recvStr();
			String channel = subscriber.recvStr();
			String type = subscriber.recvStr();
			String payload = subscriber.recvStr();
			if (type.length() == 1) {
				IPCMessage msg = factory.buildForType(Integer.parseInt(type));
				msg.from_bson(payload);

				processor.process(address, this.id, channelId, msg);
			}
		}
	}

	public String get_id() {
		return this.id;
	}

	public void set_id(String id) {
		this.id = id;
	}
}