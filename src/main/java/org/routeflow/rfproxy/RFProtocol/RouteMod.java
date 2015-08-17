package org.routeflow.rfproxy.RFProtocol;

import org.routeflow.rfproxy.IPC.IPC.IPCMessage;
import org.routeflow.rfproxy.IPC.Tools.fields;
import org.routeflow.rfproxy.IPC.Tools.messagesTypes;
import org.routeflow.rfproxy.IPC.Tools.Option;
import org.routeflow.rfproxy.IPC.Tools.OptionType;

import org.onosproject.net.flow.criteria.*;
import org.onosproject.net.PortNumber;
import org.onlab.packet.IpPrefix;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.net.flow.*;
import org.onosproject.net.flow.instructions.*;

import java.util.List;
import java.util.ArrayList;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class RouteMod extends IPCMessage implements fields, messagesTypes {
    private int mod;
    private long id;
    private TrafficTreatment.Builder actions;
    private TrafficSelector match;
    private List<Option> options;

    public RouteMod(){}

    public RouteMod(int m, long mod_id, TrafficTreatment.Builder acts, 
                            DefaultTrafficSelector mtchs, List<Option> opts){
        this.set_mod(m);
        this.set_id(mod_id);
        this.set_actions(acts);
        this.set_matches(mtchs);
        this.set_options(opts);
    }

    public long get_id(){
        return this.id;
    }

    public void set_id(long mod_id){
        this.id = mod_id;
    }

    public int get_mod(){
        return this.mod;
    }

    public void set_mod(int m){
        this.mod = m;
    }

    public TrafficTreatment.Builder get_actions(){
        return this.actions;
    }

    public void set_actions(TrafficTreatment.Builder acts){
        this.actions = acts;
    }

    public TrafficSelector get_matches(){
        return this.match;
    }

    public void set_matches(DefaultTrafficSelector mtchs){
        this.match = mtchs;
    }

    public List<Option> get_options(){
        return this.options;
    }

    public void set_options(List<Option> opts){
        this.options = opts;
    }

    public DBObject to_bson() {
        DBObject data = new BasicDBObject();

        data.put("mod", String.valueOf(this.mod));
        data.put("id", String.valueOf(this.id));
        data.put("match", get_matches());
        data.put("actions", get_actions());
        data.put("options", get_options());

        return data;
    }

    public void from_bson(DBObject data) {
        DBObject content = (DBObject) data.get(fields.CONTENT_FIELD);

        this.actions = DefaultTrafficTreatment.builder();
        this.options = new ArrayList<Option>();
        this.match = DefaultTrafficSelector.emptySelector();

        this.set_mod(Integer.parseInt(content.get("mod").toString()));
        this.set_id(Long.parseLong(content.get("id").toString()));

        List<DBObject> aux = (List<DBObject>) content.get("actions");

        for(DBObject obj : aux)
            db_to_action(obj);

        aux = (List<DBObject>) content.get("options");

        for(DBObject obj : aux)
            db_to_option(obj);

        aux = (List<DBObject>) content.get("matches");
        
        for(DBObject obj : aux)
            db_to_matchEntry(obj);
    }

    private void db_to_matchEntry(DBObject o){
        int type = Integer.parseInt(o.get("type").toString());

        //RFMT_VLAN = 255      # Match incoming VLAN (Unimplemented)
        if(type == 1){ //Match IPv4 Destination

            byte[] value = (byte[]) o.get("value");
            byte[] ipvalue = new byte[4];
            byte[] mask = new byte[4];

            for(int i=0; i < 4; i++)
                ipvalue[i] = value[i];

            for(int j=0; j < 4; j++)
                mask[j] = value[j+4];

            IpPrefix address = IpPrefix.valueOf(IpAddress.Version.INET, ipvalue, IpPrefix.MAX_INET_MASK_LENGTH);

            this.match = DefaultTrafficSelector.builder()
                    .matchIPDst(address)
                    .build();
        }
        else if (type == 2){ //Match IPv6 Destination
            byte[] value = (byte[]) o.get("value");
            byte[] ipvalue = new byte[16];

            for(int i=0; i < 16; i++)
                ipvalue[i] = value[i];

                IpPrefix address = IpPrefix.valueOf(IpAddress.Version.INET6, ipvalue, IpPrefix.MAX_INET6_MASK_LENGTH);
                this.match = DefaultTrafficSelector.builder()
                        .matchIPDst(address)
                        .build();
        }
        else if (type == 3){ //Match Ethernet Destination
            byte[] value = (byte[]) o.get("value");
            this.match = DefaultTrafficSelector.builder()
                    .matchEthDst(MacAddress.valueOf(value))
                    .build();
        /*else if (type == 4){ //Match MPLS label_in
            this.match.setField(MatchType.
        }*/
        }
        else if (type == 5){ //Match Ethernet type
            short val = new BigInteger(((byte[]) o.get("value"))).shortValue();

            this.match = DefaultTrafficSelector.builder()
                    .matchEthType(val)
                    .build();
        }
        /*else if (type == 6){ //Match Network Protocol
        //    this.match.setField(MatchType.NW_PROTO, ((byte[]) o.get("value"))[0]);
        }*/
        else if (type == 7){ //Match Transport Layer Src Port
            short val = new BigInteger(((byte[]) o.get("value"))).shortValue();

            this.match = DefaultTrafficSelector.builder()
                    .matchTcpSrc(val)
                    .build();
        }
        else if (type == 8){ //Match Transport Layer Dest Port
            short port = new BigInteger(((byte[]) o.get("value"))).shortValue();

            this.match = DefaultTrafficSelector.builder()
                    .matchTcpDst(port)
                    .build();
        }
        else if(type == 254){ //Match incoming port      
            short port = new BigInteger(((byte[]) o.get("value"))).shortValue();

            PortNumber nc = PortNumber.portNumber(port);
            this.match = DefaultTrafficSelector.builder()
                    .matchInPort(nc)
                    .build();
        }
    }


    private void db_to_action(DBObject o){
        int type = Integer.parseInt(o.get("type").toString());

        if(type == 1){
            short port = new BigInteger(((byte[]) o.get("value"))).shortValue();

            PortNumber nc = PortNumber.portNumber(port);

            Instructions.OutputInstruction ac = Instructions.createOutput(nc);
            this.actions.add(ac);

        }
        else if (type == 2){
            byte[] val = (byte[]) o.get("value");

            this.actions.setEthSrc(MacAddress.valueOf(val));
        }
        else if(type == 3){
            byte[] val = (byte[]) o.get("value");

            this.actions.setEthDst(MacAddress.valueOf(val));
        }
    }

    private void db_to_option(DBObject o){
        int type = Integer.parseInt(o.get("type").toString());
        byte[] bytes = (byte[]) o.get("value");
        short val = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getShort();

        if(type == OptionType.RFOT_PRIORITY)
            this.options.add(new Option(OptionType.RFOT_PRIORITY, val));
        else if (type == OptionType.RFOT_IDLE_TIMEOUT)
            this.options.add(new Option(OptionType.RFOT_IDLE_TIMEOUT, val));
        else if(type == OptionType.RFOT_HARD_TIMEOUT)
            this.options.add(new Option(OptionType.RFOT_HARD_TIMEOUT, val));

    }  

    public int get_type() {
        return messagesTypes.RouteMod;
    };

    public String str(){
        return "RouteMod\n mod: " + String.valueOf(this.mod) + "\nid: " + String.valueOf(this.id);
    }

}
