#!/bin/bash

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

if [ -z "${NAMENODE}" ]; then
  export NAMENODE=${HOSTNAME}
fi

if [ ! -f /etc/profile.d/hadoop.sh ]; then
  echo '#!/bin/bash' | sudo tee /etc/profile.d/hadoop.sh
  echo "export NAMENODE=${NAMENODE}" | sudo tee -a /etc/profile.d/hadoop.sh
  sudo chmod a+x /etc/profile.d/hadoop.sh
fi

sudo chmod 777 /etc/hadoop/conf/core-site.xml
sudo sed "s/@hdfs.namenode@/$NAMENODE/g" -i /etc/hadoop/conf/core-site.xml

sudo start-hdfs.sh
sudo sysctl -p

exec "$@"
