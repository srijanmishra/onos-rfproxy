package org.routeflow.rfproxy.IPC.IPC;

import com.mongodb.DBObject;

/** Class for a message transmitted through the IPC */
public abstract class IPCMessage {

	/**
	 * Get the type of the message.
	 * 
	 * @return the type of the message
	 */
	public abstract int get_type();

	/**
	 * Sets the fields of this message to those given in the BSON data.
	 * 
	 * @param data the BSON data from which to load.
	 */
	public abstract void from_bson(DBObject data);

	/**
	 * Creates a BSON representation of this message.
	 * 
     * @return the binary representation of the message in BSON
	 */
	public abstract DBObject to_bson();

	/**
	 * Get a string representation of the message.
	 * 
	 * @return the string representation of the message
	 */
	public abstract String str();
}
