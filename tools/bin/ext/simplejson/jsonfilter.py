# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import simplejson
import cgi

class JSONFilter(object):
    def __init__(self, app, mime_type='text/x-json'):
        self.app = app
        self.mime_type = mime_type

    def __call__(self, environ, start_response):
        # Read JSON POST input to jsonfilter.json if matching mime type
        response = {'status': '200 OK', 'headers': []}
        def json_start_response(status, headers):
            response['status'] = status
            response['headers'].extend(headers)
        environ['jsonfilter.mime_type'] = self.mime_type
        if environ.get('REQUEST_METHOD', '') == 'POST':
            if environ.get('CONTENT_TYPE', '') == self.mime_type:
                args = [_ for _ in [environ.get('CONTENT_LENGTH')] if _]
                data = environ['wsgi.input'].read(*map(int, args))
                environ['jsonfilter.json'] = simplejson.loads(data)
        res = simplejson.dumps(self.app(environ, json_start_response))
        jsonp = cgi.parse_qs(environ.get('QUERY_STRING', '')).get('jsonp')
        if jsonp:
            content_type = 'text/javascript'
            res = ''.join(jsonp + ['(', res, ')'])
        elif 'Opera' in environ.get('HTTP_USER_AGENT', ''):
            # Opera has bunk XMLHttpRequest support for most mime types
            content_type = 'text/plain'
        else:
            content_type = self.mime_type
        headers = [
            ('Content-type', content_type),
            ('Content-length', len(res)),
        ]
        headers.extend(response['headers'])
        start_response(response['status'], headers)
        return [res]

def factory(app, global_conf, **kw):
    return JSONFilter(app, **kw)
