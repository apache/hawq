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
all: dblp rig joost 

dblp: dblp.xml dblp.dtd

dblp.xml: dblp.xml.gz
	gunzip -c $< > $@

dblp.xml.gz:
	wget http://www.cs.washington.edu/research/xmldatasets/data/dblp/dblp.xml.gz

dblp.dtd:
	wget http://www.cs.washington.edu/research/xmldatasets/data/dblp/dblp.dtd

rig: rig.xml

rig.xml: WITSML_Data_Schema_v1311.zip
	unzip -p WITSML_Data_Schema_v1311.zip XML_Examples/rig.xml | dos2unix > rig.xml

WITSML_Data_Schema_v1311.zip:
	wget http://w3.energistics.org/schema/WITSML_Data_Schema_v1311.zip

joost: joost.jar

joost.jar: joost-0.9.1-bin.zip
	unzip -p joost-0.9.1-bin.zip joost-0.9.1/joost.jar > joost.jar

joost-0.9.1-bin.zip:
	wget http://sourceforge.net/projects/joost/files/joost/0.9.1/joost-0.9.1-bin.zip/download

clean:
	-rm -f [^R]*.xml *.jar *.zip *.dtd *.gz
