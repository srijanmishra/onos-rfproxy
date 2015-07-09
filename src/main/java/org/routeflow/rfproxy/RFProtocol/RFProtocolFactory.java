package org.routeflow.rfproxy.RFProtocol;

import org.routeflow.rfproxy.IPC.IPC.IPCMessage;
import org.routeflow.rfproxy.IPC.IPC.IPCMessageFactory;
import org.routeflow.rfproxy.IPC.Tools.messagesTypes;

public class RFProtocolFactory extends IPCMessageFactory implements messagesTypes{

	@Override
	public IPCMessage buildForType(int type) {
		if(type == messagesTypes.RouteMod){
			return new RouteMod();
		}
		if(type == messagesTypes.DatapathDown){
			return new DatapathDown();
		}
		if(type == messagesTypes.DatapathPortRegister){
			return new DatapathPortRegister();
		}
		if(type == messagesTypes.DataPlaneMap){
			return new DataPlaneMap();
		}
		if(type == messagesTypes.PortConfig){
			return new PortConfig();
		}
		if(type == messagesTypes.PortRegister){
			return new PortRegister();
		}
		if(type == messagesTypes.VirtualPlaneMap){
			return new VirtualPlaneMap();
		}
		return null;
	}

}


