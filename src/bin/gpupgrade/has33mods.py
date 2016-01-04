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
#

import pg8000

def has33mods(masterdb):
    masterdb.execute('''select
        (select 1 from pg_class where relnamespace = 11 and 
         relname = 'kk') is not null as hasaocs,
        (select 1 from pg_attribute where attname = 'compresstype' and
         attrelid = 'pg_appendonly'::regclass) is not null as aocol,
        (select 1 from pg_class where relnamespace = 11 and
         relname = 'gp_configuration_history') is not null as hasgpconfhis;''')
    return masterdb.iterate_dict().next()

if __name__ == '__main__':
    conn = pg8000.Connection(user='swm', host='localhost', port=12000,
                             database='postgres')
    dets = has33mods(conn)
    print dets
