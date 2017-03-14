"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import urllib2, base64
import json

from optparse import OptionParser
from rangerrest import RangerRestHelper


def foo_callback(option, opt, value, parser):
  setattr(parser.values, option.dest, value.split(','))

def option_parser():
    '''option parser'''
    parser = OptionParser()
    parser.remove_option('-h')
    parser.add_option('-?', '--help', action='help')
    parser.add_option('-h', '--host', dest="host", help='host of the ranger server', \
                      default='localhost')
    parser.add_option('-p', '--port', dest="port", \
                      help='port of the ranger server', type='int', default=6080)
    parser.add_option('-U', '--rangeruser', dest="rangerusername", default='admin', \
                      help='ranger username')
    parser.add_option('-w', '--rangerpassword', dest="rangerpassword", \
                      default='admin', help='ranger password')
    parser.add_option('-d', '--detelepolicy', dest="deletedpolicyname",\
                      default= '', help='delete a policy in ranger')
    parser.add_option('-a', '--addpolicy', dest="newpolicyfilename", \
                      default = '', help='add a policy in ranger by json file')
    return parser

def create_policy(policy_json_file_name, rangerhelper):
    if policy_json_file_name != '':
        jsonfile = open(policy_json_file_name, "r")
        json_decode=json.load(jsonfile)
        policyname = json_decode['name']
        #print json_decode
        response, is_success = rangerhelper.create_policy(json.dumps(json_decode))
        
        # is there is a duplicate policy error, we try to update policy.
        if is_success == False:
            #get duplicate policy name
            policy_start_pos = response.find("policy-name=[")
            response = response[policy_start_pos+13:]
            policy_end_pos = response.find("], service=[")
            dup_policy_name = response[0:policy_end_pos]
            
            #get dupulicate policy and add privilege item.
            service_name = 'hawq'
            print dup_policy_name;
            response, is_success = rangerhelper.get_policy(service_name, dup_policy_name);
            response_dict = json.load(response)
            for new_policy_item in json_decode['policyItems']:
                response_dict["policyItems"].append(new_policy_item)
            for new_policy_item in json_decode['denyPolicyItems']:
                response_dict["denyPolicyItems"].append(new_policy_item)
            for new_policy_item in json_decode['allowExceptions']:
                response_dict["allowExceptions"].append(new_policy_item)
            for new_policy_item in json_decode['denyExceptions']:
                response_dict["denyExceptions"].append(new_policy_item)
                
            rangerhelper.update_policy(service_name, dup_policy_name, \
                                    json.dumps(response_dict));
        return policyname

def delete_policy(delete_policy_name, rangerhelper):
    rangerhelper.delete_policy("hawq", delete_policy_name);
    
    
if __name__ == '__main__':
    #parse argument
    parser = option_parser()
    (options, args) = parser.parse_args()
    rangeruser = options.rangerusername
    rangerpasswd= options.rangerpassword
    host = options.host
    port = str(options.port)
    new_policy_json_file_name = options.newpolicyfilename
    delete_policy_name = options.deletedpolicyname
    
    #init rangerresthelper
    helper = RangerRestHelper(host, port, rangeruser, rangerpasswd);
    
    if new_policy_json_file_name != "":
        policyname = create_policy(new_policy_json_file_name, helper)
        print "policy {} created".format(policyname)
        
    if delete_policy_name != "":
        delete_policy(delete_policy_name, helper)
        print "policy {} deleted".format(delete_policy_name)
