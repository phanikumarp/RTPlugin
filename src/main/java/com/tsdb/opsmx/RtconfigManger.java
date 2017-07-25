package com.tsdb.opsmx;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;



public class RtconfigManger {

private static final Logger logger = Logger.getLogger(RtconfigManger.class);
	
	private static final String CONFIG_FILE = "config/rtplugin.json";
	private String opsName;
	
	public String getOpsName() {
		return opsName;
	}

	public void setOpsName(String opsName) {
		this.opsName = opsName;
	}

	JSONArray json = null;
	public RtconfigManger() {
		try {
			setDBValues();
		} catch (Exception ex) {

		}
	}

	private void setDBValues() throws ConfigurationException {

		try(InputStream inputstream = new FileInputStream(CONFIG_FILE); 
				InputStreamReader reader = new InputStreamReader(inputstream)) {
			JSONObject data = (JSONObject) JSONValue.parseWithException(reader);
			 json = (JSONArray) ((JSONObject) data).get("rtagent");
			 
			for (int i = 0; i < json.size(); i++) {
				JSONObject obj = (JSONObject) json.get(i);
				setOpsName((String) obj.get("ops_name"));
			    
				
			}
		}catch (Exception e) {
			logger.error("unable to find the configuration file parameters due to this Exception::"+e);
			throw new ConfigurationException(
					"'KairoDB_details' could not be found in the 'apachesettings.json' configuration file");
		}
	}

	
}
