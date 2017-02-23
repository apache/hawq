# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The Postgres make files exploit features of GNU make that other makes
# do not have.  Because it is a common mistake for users to try to build
# Postgres with a different make, we have this make file that does nothing
# but tell the user to use GNU make.

# If the user were using GNU make now, this file would not get used because
# GNU make uses a make file named "GNUmakefile" in preference to "Makefile"
# if it exists.  Postgres is shipped with a "GNUmakefile".

all install clean check installcheck:
	@echo "You must use GNU make to use Postgres.  It may be installed"
	@echo "on your system with the name 'gmake'."
