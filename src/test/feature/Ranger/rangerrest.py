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

class RangerRestHelper(object):
    def __init__(self, host, port, user, passwd):
        self.host = host
        self.port = port
        self.user = user
        self.passwd= passwd
        
    def _send_request(self, url, method, jsondata=''):
        try:
            request = urllib2.Request(url)  
            if method != "DELETE":
                request.add_header("Content-Type",'application/json')  
            request.add_header("Accept", 'application/json')
            #request.add_header("Content-Type",'application/json')
            base64string = base64.b64encode('%s:%s' % (self.user, self.passwd))
            request.add_header("Authorization", "Basic %s" % base64string) 
            request.get_method = lambda: method 
            if jsondata != '':
                request.add_data(jsondata)   
            ret = urllib2.urlopen(request)
            return ret, True
        except urllib2.HTTPError, e:
            if e.code == 400 and e.reason == "Bad Request":
                error_message = e.read()
                print error_message
                return error_message, False
            return "HTTPError", False
    
    def get_policy(self, service_name, policy_name):
        url = 'http://' + self.host + ':' + self.port + '/service/public/v2/api/service/' + \
            service_name +'/policy/' + policy_name
        return self._send_request(url, 'GET')
    
    def update_policy(self, service_name, policy_name, policy_info):
        url = 'http://' + self.host + ':' + self.port + '/service/public/v2/api/service/' + \
            service_name +'/policy/' + policy_name
        return self._send_request(url, 'PUT', policy_info)
    
    def create_policy(self, policy_info):
        url = 'http://' + self.host + ':' + self.port + '/service/public/v2/api/policy'
        return self._send_request(url, 'POST', policy_info)
    
    def delete_policy(self, service_name, policy_name):
        
        url = 'http://' + self.host + ':' + self.port + \
              '/service/public/v2/api/policy?servicename=' + \
              service_name +'&policyname=' + policy_name
        return self._send_request(url, 'DELETE')
    
    def get_user(self):
        url = 'http://' + self.host + ':' + self.port + '/service/users'
        return self._send_request(url, 'GET')
    
    # create secure user may not work currently
    def create_secure_user(self, user_info):
        url = 'http://' + self.host+ ':' + self.port + '/service/xusers/secure/users'
        return self._send_request(url, 'POST', user_info)
    
    def create_user_without_login(self, user_info):
        url = 'http://' + self.host+ ':' + self.port + '/service/xusers/users'
        return self._send_request(url, 'POST', user_info)

    def delete_user(self, user):
        url = 'http://' + self.host+ ':' + self.port + '/service/xusers/users'
        return self._send_request(url, 'DELETE')
