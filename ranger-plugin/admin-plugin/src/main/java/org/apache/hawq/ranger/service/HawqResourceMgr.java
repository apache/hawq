/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.service;

import org.apache.hawq.ranger.model.HawqResource;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.log4j.Logger;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.Collections;

public abstract class HawqResourceMgr {

	private static final Logger LOG = Logger.getLogger(HawqResourceMgr.class);
	
    public static List<String> getHawqResources(String serviceName, String serviceType, Map<String, String> configs,
                                                ResourceLookupContext context) throws Exception {
      	final String userInput = context.getUserInput();
        HawqResource hawqResource = HawqResource.valueOf(context.getResourceName().toUpperCase());
        final Map<String, List<String>> resources = context.getResources();
        
        List<String> result = null;
        if (serviceName != null && userInput != null) {
	        try {
	        	
	        		if(LOG.isDebugEnabled()) {
					LOG.debug("==> HawqResourceMgr.getHawqResources() UserInput: "+ userInput  + " configs: " + configs);
				}
	        		final HawqClient hawqClient =new HawqClient(serviceName, configs);
		        Callable<List<String>> callableObj = null;
		        
	            if(hawqClient != null) {
	             	    switch (hawqResource) {
	            	        case DATABASE:
	          	    	        callableObj = new Callable<List<String>>() {
	            	    	        	    @Override
	            	    	        	    public List<String> call() throws SQLException{
	              	    	        	    return hawqClient.getDatabaseList(userInput);
	            	    	        	    }
	            	    	        };
	            	    	        break;
	            	        case TABLESPACE:
	        	    	            callableObj = new Callable<List<String>>() {
	        	    	        	        @Override
	        	    	        	        public List<String> call() throws SQLException{
	          	    	        	        return hawqClient.getTablespaceList(userInput);
	        	    	        	        }
	        	    	            };
	        	    	            break;
	            	        case PROTOCOL:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException{
		      	    	        	        return hawqClient.getProtocolList(userInput);
		    	    	        	        }
		    	    	            };
		    	    	            break;
	            	        case SCHEMA:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException{
		      	    	        	        return hawqClient.getSchemaList(userInput, resources);
		    	    	        	        }
		    	    	            };
		    	    	            break;
	            	        case LANGUAGE:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException{
		      	    	        	        return hawqClient.getLanguageList(userInput, resources);
		    	    	        	        }
		    	    	            };
		    	    	            break;
	            	        case TABLE:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException{
		      	    	        	        return hawqClient.getTableList(userInput, resources);
		    	    	        	        }
		    	    	            };
		    	    	            break;
	            	        case SEQUENCE:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException {
		      	    	        	        return hawqClient.getSequenceList(userInput, resources);
		    	    	        	        }
		    	    	            };
		    	    	            break;
	            	        case FUNCTION:
		    	    	            callableObj = new Callable<List<String>>() {
		    	    	        	        @Override
		    	    	        	        public List<String> call() throws SQLException{
		      	    	        	        return hawqClient.getFunctionList(userInput, resources);
		    	    	        	        }
		    	    	            };
		    	    	            break;
					    default:
					        throw new IllegalArgumentException("Resource requested does not exist.");
		        		}
		        }
		        
		        if (callableObj != null) {
					synchronized (hawqClient) {
						result = TimedEventUtil.timedTask(callableObj, 5,
								TimeUnit.SECONDS);
					}
				} else {
					LOG.error("Could not initiate at timedTask");
				}
		      
		
		        Collections.sort(result);
	        } catch (Exception e) {
				LOG.error("Unable to get hive resources.", e);
				throw e;
	        }
        }
        return result;
    }
}
