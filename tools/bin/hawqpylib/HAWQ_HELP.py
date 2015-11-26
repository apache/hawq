#!/usr/bin/env python
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

COMMON_HELP = """ 
usage: hawq <command> [<object>] [options]
            [--version]

The most commonly used hawq "commands" are:
   start         Start hawq service.
   stop          Stop hawq service.
   init          Init hawq service.
   restart       Restart hawq service.
   activate      Activate hawq standby master as master.
   version       Show hawq version information.
   config        Set hawq GUC values.
   state         Show hawq cluster status.
   filespace     Create hawq filespaces.
   extract       Extract table's metadata into a YAML formatted file.
   load          Load data into hawq.
   scp           Copies files between multiple hosts at once.
   ssh           Provides ssh access to multiple hosts at once.
   ssh-exkeys    Exchanges SSH public keys between hosts.
   check         Verifies and validates HAWQ settings.

See 'hawq <command> help' for more information on a specific command.
"""

START_HELP = """
usage: hawq start <object> [--options]

The "objects" are:
   cluster         Start hawq cluster.
   master          Start hawq master.
   segment         Start local segment node.
   standby         Start hawq standby.
   allsegments     Start all segments.

The "options" are:
   -l --logdir        Sets log dir of management tools.
   -q --quiet         Run in quiet mode.
   -v --verbose       Displays detailed status, progress and error messages output by the utility.
   -t --timeout       Sets timeout value in seconds, default is 60 seconds.
   -m --masteronly    Start hawq in masteronly mode.
   -R --restrict      Start hawq in restrict mode.
   -U --special-mode   Start hawq in [upgrade/maintenance] mode.

See 'hawq --help' for more information on other commands.
"""

STOP_HELP = """
usage: hawq stop <object> [--options]

The "objects" are:
   cluster         Stop hawq cluster.
   master          Stop hawq master.
   segment         Stop local segment node.
   standby         Stop hawq standby.
   allsegments     Stop all segments.

The "options" are:
   -a --prompt     Do not ask before execution.
   -l --logdir     Sets log dir of management tools.
   -q --quiet      Run in quiet mode.
   -v --verbose    Displays detailed status, progress and error messages output by the utility.
   -t --timeout    Sets timeout value in seconds, default is 60 seconds.
   -M --mode       Stop with mode [smart|fast|immediate]
   -u --reload     Reload GUC values without restart hawq cluster.

See 'hawq --help' for more information on other commands.
"""

INIT_HELP = """
usage: hawq init <object> [--options]

The "objects" are:
   cluster         Init hawq cluster.
   master          Init hawq master.
   segment         Init local segment node.
   standby         Init hawq standby.

The "options" are:
   -a --prompt          Do not ask before execution.
   -l --logdir          Sets log dir of management tools.
   -q --quiet           Run in quiet mode.
   -v --verbose         Displays detailed status, progress and error messages output by the utility.
   -t --timeout         Sets timeout value in seconds, default is 60 seconds.
   -n --no-update       Resync standby with master, but do not update system catalog tables.
   --locale             Sets the locale name.
   --lc-collate         Sets the string sort order.
   --lc-ctype           Sets character classification.
   --lc-messages        Sets the language in which messages are displayed.
   --lc-monetary        Sets the locale to use for formatting monetary amounts.
   --lc-numeric         Sets the locale to use for formatting numbers.
   --lc-time            Sets the locale to use for formatting dates and times.
   --vsegment_number    Sets the virtual segments number per node.
   --max_connections    Sets the max_connections for formatting hawq database.
   --shared_buffers     Sets the shared_buffers for initializing hawq.

See 'hawq --help' for more information on other commands.
"""

RESTART_HELP = """
usage: hawq restart <object> [--options]

The "objects" are:
   cluster         Restart hawq cluster.
   master          Restart hawq master.
   segment         Restart local segment node.
   standby         Restart hawq standby.
   allsegments     Restart all segments.

The "options" are:
   -a --prompt        Do not ask before execution.
   -l --logdir        Sets log dir of management tools.
   -q --quiet         Run in quiet mode.
   -v --verbose       Displays detailed status, progress and error messages output by the utility.
   -t --timeout       Sets timeout value in seconds, default is 60 seconds.
   -M --mode          Stop with mode [smart|fast|immediate]
   -u --reload        Reload GUC values without restart hawq cluster.
   -m --masteronly    Start HAWQ in master-only mode.
   -R --restrict      Start HAWQ in restrict mode.
   -U --special-mode   Start HAWQ in [upgrade/maintenance] mode.

See 'hawq --help' for more information on other commands.
"""

ACTIVE_HELP = """
usage: hawq activate standby

The "options" are:
   -q --quiet      Run in quiet mode.
   -v --verbose    Displays detailed status, progress and error messages output by the utility.
   -l --logdir     Sets log dir of management tools.
   -t --timeout    Sets timeout value in seconds, default is 60 seconds.
   -M --mode       Stop with mode [smart|fast|immediate]

See 'hawq --help' for more information on other commands.
"""

CONFIG_HELP = """
usage: hawq config [--options]

The "options" are:
   -c --change         Changes a configuration parameter setting.
   -s --show           Shows the value for a specified configuration parameter.
   -l --list           Lists all configuration parameters.
   -q --quiet          Run in quiet mode.
   -v --verbose        Displays detailed status.
   -r --remove         HAWQ GUC name to be removed.
   --skipvalidation    Skip the system validation checks.

See 'hawq --help' for more information on other commands.
"""

STATE_HELP = """
usage: hawq state [--options]

The "options" are:
   -b                  Show brief status of cluster.
   -l --logdir         Sets log dir of management tools.
   -q --quiet          Run in quiet mode.
   -v --verbose        Displays detailed status, progress and error messages output by the utility.
   -d --datadir        Specify HAWQ master data directory.
   --hawqhome          Specify HAWQ install directory.

See 'hawq --help' for more information on other commands.
"""
