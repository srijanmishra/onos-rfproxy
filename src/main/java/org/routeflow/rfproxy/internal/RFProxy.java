package org.routeflow.rfproxy.internal;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.net.*; 
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.io.*;

import org.onosproject.net.PortNumber;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.packet.*;
import org.routeflow.rfproxy.IPC.IPC.IPCMessage;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageProcessor;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageService;
import org.routeflow.rfproxy.IPC.JeroMQIPC.JeroMQIPCMessageService;
import org.routeflow.rfproxy.IPC.Tools.*;
import org.routeflow.rfproxy.RFProtocol.*;

import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.DeviceId;

import org.onosproject.core.CoreService;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;

import org.onosproject.net.flow.*;

import org.openflow.protocol.factory.BasicFactory;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFType;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate = true)
/* Main Class. */
public class RFProxy {
	/* Log Service. */
	protected static Logger logger = LoggerFactory.getLogger(RFProxy.class);

	/* Association Table. */
	AssociationTable table = new AssociationTable();

	/* IPC's Definition. */
	private String address = defs.MONGO_ADDRESS;
	private String database_name = defs.MONGO_DB_NAME;
	private String consumer_id = defs.RFPROXY_ID;
	private String listen_queue = defs.RFSERVER_RFPROXY_CHANNEL;
	private JeroMQIPCMessageService ipc;
	private RFProtocolFactory factory;
	private RFProtocolProcessor processor;
	private BasicFactory basicFactory;
	private ApplicationId appId;
	/* SAL modules, required for listening to packets, manage switches and program (install/remove) flows */
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected PacketService packetService;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	private DeviceService switchMgr;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    private FlowRuleService programmer;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ComponentConfigService cfgService;

    @Activate
    public void activate() {
        cfgService.registerProperties(RFProxy.class);
        appId = coreService.registerApplication("org.routeflow.onos-rfproxy");

        //packetService.addProcessor(processor, PacketProcessor.ADVISOR_MAX + 2);
        //readComponentConfiguration(context);
        //requestPackests();
        start();

        logger.info("Started with Application ID {}", appId.id());
    }

	/**
	 * private method to process a RouteMod message received
	 * from the ipc
	 * The method will create a flow and set its actions, 
	 * matches and options according to the information
	 * in the message.
	 * @param msg the received message
	 */
    private void process_flow_mod(RouteMod msg){
    	if(msg.get_mod() != 0 && msg.get_mod() != 1) {
    		this.logger.error("Unrecognised RouteMod Type (type: " + msg.get_type() + ")");
    		return ;
    	}

		int priority = 1;
		int makeTemp = 0;

    	//this.logger.info("Processing flow mod");
    	/* Create flow and set default options.
    	 	This is done to avoid additional checks for existing options*/
    	//Flow flow = new Flow(msg.get_matches(), msg.get_actions());
        //flow.setHardTimeout((short) 0);       
        //flow.setPriority((short) 1); //DEFAULT_IPSWITCH_PRIORITY
        //flow.setIdleTimeout((short) 0);

        // Override the predefined options for the packet options
		for(Option o : msg.get_options()){
			if(o.get_type() == OptionType.RFOT_PRIORITY) {
				priority = o.get_value();
			}
	        else if (o.get_type() == OptionType.RFOT_IDLE_TIMEOUT) {
				makeTemp = o.get_value();
			}
	        else if(o.get_type() == OptionType.RFOT_HARD_TIMEOUT) {
				makeTemp = o.get_value();
			}
		}

		// create the node to implement the flow in
    	//Node node = NodeCreator.createOFNode(msg.get_id());

    	DeviceId node = DeviceId.deviceId(String.valueOf(msg.get_id()));

		FlowRule flowRule = DefaultFlowRule.builder()
				.forDevice(node)
				.withSelector(msg.get_matches())
				.withTreatment(msg.get_actions())
				.withPriority(priority)
				.withFlag(ForwardingObjective.Flag.VERSATILE)
				.fromApp(appId)
				.makeTemporary(makeTemp)
				.build();

		if(msg.get_mod() == 0) { // RouteMod ADD
    		// add the flow
    		this.logger.info("Adding flow");

			this.programmer.applyFlowRules(flowRule);

			this.logger.info("Installed {} in node id {}, node {}", flowRule, msg.get_id(), node);
		}
    	else if(msg.get_mod() == 1){ // RouteMod DEL
    		// remove the flow
    		this.programmer.removeFlowRules(flowRule);
    		
    		this.logger.info("Removed {} in node {}", flow, node);
		}    		    			
    }

    /**
     * Send a packet to a switch
     * @param sw_id the id of the switch to send the message to
     * @param port the port of the switch to send the message to
     * @param inPkt the packet with data to send
     */
	private void send_packet(Long sw_id, short port, InboundPacket inPkt) {
		OFPacketOut packetOut = (OFPacketOut) this.basicFactory.getMessage(OFType.PACKET_OUT);
		
		// Set Output action so that packets are forwarded to the specified port
		TrafficTreatment.Builder actions = DefaultTrafficTreatment.builder();
		actions.setOutput(PortNumber.portNumber((long) port));

		// Build the packet to send using data from the received inPkt
		ByteBuffer packetData = inPkt.unparsed();
		packetOut.setPacketData(packetData.array());
		 
		packetOut.setBufferId(OFPacketOut.BUFFER_ID_NONE);

		// Sending data to nodes
		DeviceId node = DeviceId.deviceId(String.valueOf(sw_id));

		DefaultOutboundPacket toSend = new DefaultOutboundPacket(node, actions.build(), packetData);
		packetService.emit(toSend);
		
	}

	/*******************************************************************
	 ****************************   Listeners   ************************
	 *******************************************************************/

    /**
     * Method implemented by the IInventoryListener interface
     * When a new switch is added by the controller, the notifyNode methd 
     * of every listener is executed.
     * The method will only answer to updates of the type remove, since
     * RouteFlow does not require datapathUp events
     * @param node that connected or disconnected
     * @param type the type of the Update
     * @param propMap a map with the properties of the node
     */
    public void notifyNode(DeviceId node, UpdateType type,
            Map<String, Property> propMap){

    		if(type == UpdateType.REMOVED){
    			// Get id of the datapath
				long dp_id = (long) node.toString(); // Get dp id

    			// Delete from the association table
				this.table.delete_dp(dp_id);

				/* Create and send DatapathDown message to inform RF-S
						of the event */ 
				DatapathDown msg = new DatapathDown(defs.RFPROXY_ID, dp_id);
				this.ipc.send(defs.RFSERVER_RFPROXY_CHANNEL, defs.RFSERVER_ID, msg);

				logger.info("Datapath down (dp_id={})", dp_id);
    		}
    }

    /**
     * Method implemented by the IInventoryListener interface
     * When a new switch port is added by the controller, the notifyNodeConnector
     * method of every listener is executed.
     * The method will only answer to updates of the type ADDED, since
     * RouteFlow does not require datapathPortDown events
     * @param nodeConnector the connector that identifies the port and switch
     * @param type the type of the Update
     * @param propMap a map with the properties of the nodeConnector
     */
    public void notifyNodeConnector(NodeConnector nodeConnector,
            UpdateType type, Map<String, Property> propMap){

			if(type == UpdateType.ADDED){
				// Get the port and id of the switch
				long dp_id = (long) nodeConnector.getNode().getID(); // Get dp id
				short port = Short.parseShort(nodeConnector.getID().toString());

				if (port > 0) {

					/* Create and send DatapathPortRegister message to inform RF-S 
						of the event */
					DatapathPortRegister msg = new DatapathPortRegister(defs.RFPROXY_ID, dp_id, port);
					this.ipc.send(defs.RFSERVER_RFPROXY_CHANNEL, defs.RFSERVER_ID, msg);

					logger.info("Registering datapath port (ct_id={}, dp_id={}, dp_port={})",
									defs.RFPROXY_ID, dp_id, port);
				}
    		}
    }

    /**
     * Method implemented by the IListenDataPacket interface
     * When a new Packet is received by the controller, the 
     * receiveDataPacket method of every listener is executed
     * This method will process the data packet and execute
     * an action according to the type of the packet, and
     * from where it comes from.
     * @param inPkt the received packet
     * @return if the packet will be ignored or consumed
     */
	public PacketResult receiveDataPacket(RawPacket inPkt){
		NodeConnector in = inPkt.getIncomingNodeConnector();

		// Get ethernet type using the match
		OFMatch match = new OFMatch();
		match.loadFromPacket(inPkt.getPacketData(), 
			(short) Integer.parseInt(in.getID().toString()));
		Short ethernetType = match.getDataLayerType();

		// If packet is of type LLDP or has a type lower than 0, ignore
		if (ethernetType == EtherTypes.LLDP.shortValue() || ethernetType < 0)
			return PacketResult.IGNORED;

		// Decode the RawPacket
		Packet packet = this.dataPacketService.decodeDataPacket(inPkt);

		// Convert the node from which the packet came into a RF DataPath
		DP from = new DP(Long.valueOf(in.getNode().getID().toString()).longValue() , 
						Integer.parseInt(in.getID().toString()));

		if(ethernetType == defs.RF_ETH_PROTO){ // Received Mapping packet
			//process packet
			byte[] data = inPkt.getPacketData();
			byte[] buf = new byte[8];

			buf[0] = data[14];
			buf[1] = data[15];
			buf[2] = data[16];
			buf[3] = data[17];
			buf[4] = data[18];
			buf[5] = data[19];
			buf[6] = data[20];
			buf[7] = data[21];

			ByteBuffer bb = ByteBuffer.wrap(buf);

			bb.order(ByteOrder.LITTLE_ENDIAN);

			// get the virtual id and port
			long vm_id = bb.getLong();
			int vm_port = data[22];

			String desc = "vm_id=" + Long.toString(vm_id) + " vm_port="
					+ Integer.toString(vm_port) + " vs_id="
					+ Long.toString(from.getDp_id()) + " vs_port="
					+ from.getDp_port();

			this.logger.debug("Received mapping packet ({})", desc);

			/* Create and send VirtualPlaneMap message to inform RF-S 
				of the mapping */
			VirtualPlaneMap vpm = new VirtualPlaneMap(vm_id, vm_port, from.getDp_id(), from.getDp_port());
			this.ipc.send(defs.RFSERVER_RFPROXY_CHANNEL, defs.RFSERVER_ID, vpm);
		}

		else if (from.getDp_id() == defs.RFVS_DPID){ // Received Packet from RFVS
			/* Translate the virtual switch to its corresponding datapath
				 using the association table */

			VS vs = new VS(from.getDp_id(), from.getDp_port());
			DP dp = table.vs_port_to_dp_port(vs);

			// Send packet to the corresponding datapath or debug an unmapped port
			if (dp != null) 
				this.send_packet(dp.getDp_id(), (short) dp.getDp_port(),
					inPkt);
			else
				this.logger.debug("Unmapped RFVS port (vs_id=" + dp.getDp_id() + 
					", vs_port=" + (short) dp.getDp_port() + ")");
		}
		else { // Received Packet from a Datapath
			/* Translate the datapath to its corresponding virtual switch
				 using the association table */
				 
			DP dp = new DP(from.getDp_id(), from.getDp_port());
			VS vs = table.dp_port_to_vs_port(dp);

			// Send packet to the corresponding virtual switch or debug an unmapped port
			if (vs != null) 
				this.send_packet(vs.getVs_id(), (short) vs.getVs_port(),
						inPkt);
			else
				this.logger.debug("Unmapped datapath port (dp_id=" + dp.getDp_id() + 
					", dp_port=" + (short) dp.getDp_port() + ")");
		}

		// If an action is correctly executed, don't consume the packet
		return PacketResult.IGNORED;
	}


	/*******************************************************************
	 **********************   System Initialization   ******************
	 *******************************************************************/

    /**
     * Function called by dependency manager after "init ()" is called and after
     * the services provided by the class are registered in the service registry
     */
	public void start() {
		this.logger.info("Activating RFProxy");
		
		// Create IPC
 		this.ipc = new JeroMQIPCMessageService(
			this.address, this.database_name, this.consumer_id, this.logger);

		// Initialize Message Factory
 		this.factory = new RFProtocolFactory();
 		this.basicFactory = new BasicFactory();

		// Initialize Message-In Processor
 		this.processor = new RFProtocolProcessor(this.ipc);

		// Start IPC message listening
		this.ipc.listen(this.listen_queue, this.factory, this.processor, false);

	}

    /*******************************************************************
     ************************   RFProcessor Class   ********************
     *******************************************************************/

    /**
     * Class responsable for processing messages received from the IPC
     */
    public class RFProtocolProcessor extends IPCMessageProcessor implements
            messagesTypes {
        private IPCMessageService ipc;
        private Logger logger;
 
        public RFProtocolProcessor(IPCMessageService ipc) {
            this.ipc = ipc;
            this.logger = LoggerFactory.getLogger(RFProtocolProcessor.class);
            this.logger.debug("Started RFProtocolProcessor");
        }
 
        @Override
        public boolean process(String from, String to, String channel,
                IPCMessage msg) {

            int type = msg.get_type();

            if (type == messagesTypes.RouteMod) // received a RouteMod Message
            	process_flow_mod((RouteMod) msg);
            else if (type == messagesTypes.DataPlaneMap) { // received a DataPlaneMap Message
                DataPlaneMap messageDataPlaneMap = (DataPlaneMap) msg;
 
                // Update the Association Table
                DP dp_in = new DP(messageDataPlaneMap.get_dp_id(),
                        messageDataPlaneMap.get_dp_port());
                VS vs_in = new VS(messageDataPlaneMap.get_vs_id(),
                        messageDataPlaneMap.get_vs_port());

                table.update_dp_port(dp_in, vs_in);
            }
 
            return false;
        }
    }
}
