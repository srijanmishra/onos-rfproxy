package org.routeflow.rfproxy.IPC.Tools;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.routeflow.rfproxy.IPC.Tools.DP;
import org.routeflow.rfproxy.IPC.Tools.VS;


public class AssociationTable {
	private Map<DP, VS> assoc_table;

	public AssociationTable() {
		assoc_table = new HashMap<DP, VS>();
	}

	public void update_dp_port(DP dp, VS vs) {
		this.assoc_table.put(dp, vs);
	}

	public VS dp_port_to_vs_port(DP dp) {
		if (this.assoc_table.containsKey(dp)) {
			return this.assoc_table.get(dp);
		} else {
			return null;
		}
	}

	public DP vs_port_to_dp_port(VS vs) {
		for (DP dp : this.assoc_table.keySet()) {
		  if (this.assoc_table.get(dp).equals(vs))
		    return dp;
		}

		return null;
	}

	public void delete_dp(long dp_id) {
		Iterator<DP> i = this.assoc_table.keySet().iterator();

		while (i.hasNext()) {
			DP dp = i.next();

			if (dp.getDp_id() == dp_id) 
				this.assoc_table.remove(dp);
		}
	}
}
