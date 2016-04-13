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

class HostType:
    GPCHECK_HOSTTYPE_UNDEFINED  = 0
    GPCHECK_HOSTTYPE_APPLIANCE = 1
    GPCHECK_HOSTTYPE_GENERIC_LINUX = 2
    GPCHECK_HOSTTYPE_GENERIC_SOLARIS = 3

def hosttype_str(type):
    if type == HostType.GPCHECK_HOSTTYPE_APPLIANCE:
        return "GPDB Appliance"
    elif type == HostType.GPCHECK_HOSTTYPE_GENERIC_LINUX:
        return "Generic Linux Cluster"
    elif type == HostType.GPCHECK_HOSTTYPE_GENERIC_SOLARIS:
        return "Generic Solaris Cluster"
    else:
        return "Undetected Platform"

class omreport:
    def __init__(self):
        self.biossetup = dict()     # key value pairs
        self.biossetup_errormsg = None

        self.bootorder = list()     # list of Devices in order of boot
        self.bootorder_errormsg = None

        self.remoteaccess = dict()  # key value pairs
        self.remoteaccess_errormsg = None

        self.vdisks = list()        # list of dicts, 1 for each virtual disk
        self.vdisks_errormsg = None

        self.controller = dict()    # key value pairs
        self.controller_errormsg = None

        self.omversion = None
        self.omversion_errormsg = None

        self.bios = dict()          # key value pairs
        self.bios_errormsg = None

        self.alerts = list()        # list of alerts... each alert is a dictionary of key value pairs
        self.alerts_errormsg = None

class chkconfig:
    def __init__(self):
        self.services = dict() # hash of services, each entry is hash of run levels and boolean value
        self.xinetd = dict()   # hash of services, value is boolean
        self.errormsg = None

class grubconf:
    def __init__(self):
        self.serial_declaration = False
        self.terminal_declaration = False
        self.ttyS1_declaration = False
        self.errormsg = None

    def __str__(self):
        return "serial_declaration(%s) terminal_declaration(%s) ttyS1_declaration(%s)" % (self.serial_declaration, self.terminal_declaration, self.ttyS1_declaration)

class inittab:
    def __init__(self):
        self.s1 = False
        self.defaultRunLevel = None
        self.errormsg = None

    def __str__(self):
        return "s1_declaration(%s) default_run_level(%s)" % (self.s1, self.defaultRunLevel)

class connectemc:
    def __init__(self):
        self.output = None
        self.errormsg = None

    def __str__(self):
        return self.output

class securetty:
    def __init__(self):
        self.errormsg = None
        self.data = set()

class bcu:
    def __init__(self):
        self.firmware = None
        self.biosversion = None
        self.errormsg = None

    def __str__(self):
        return "firmware_version=%s|biosversion=%s" % (self.firmware, self.biosversion)

class uname:
    def __init__(self):
        self.output = None
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= UNAME ERROR ===================\n" + self.errormsg
        else:
            return "============= UNAME =========================\n" + self.output


# record machine CPU and memory info
class machine:
    def __init__(self):
        self.total_cpucores = None
        self.memory_in_MB = None
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= CPU / Memory Info ERROR =======\n" + self.errormsg
        else:
            output = "Total CPU cores: %s, Memory: %s MB" % (self.total_cpucores, self.memory_in_MB)
            return "============= CPU / Memory Info =============\n" + output


class hdfs:
    def __init__(self):
        self.max_heap_size = 0
        self.namenode_heap_size = 0
        self.datanode_heap_size = 0
        self.site_config = dict()
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= HDFS ERROR ====================\n" + self.errormsg
        else:
            output  = "max heap size: %sM\n" % self.max_heap_size
            output  = "namenode heap size: %sM\n" % self.namenode_heap_size
            output  = "datanode heap size: %sM\n" % self.datanode_heap_size
            output += "\n".join(["%s = %s" % (k, self.site_config[k]) for k in sorted(self.site_config.iterkeys())])
            return "============= HDFS ==========================\n" + output


class hawq:
    def __init__(self):
        self.site_config = dict()
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= HAWQ ERROR ====================\n" + self.errormsg
        else:
            output  = "HAWQ checks \n" 
            output += "\n".join(["%s = %s" % (k, self.site_config[k]) for k in sorted(self.site_config.iterkeys())])
            return "============= HAWQ ==========================\n" + output


class diskusage_entry:
    def __init__(self, fs, size, used, avail, used_percent, mount):
        self.fs = fs
        self.size = size
        self.used = used
        self.avail = avail
        self.used_percent = used_percent
        self.mount = mount

    def __str__(self):
        return "%-40s %8s %8s %8s %8s %-20s" % (self.fs, self.size, self.used, self.avail, self.used_percent, self.mount)


class diskusage:
    def __init__(self):
        self.lines = []
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= DISK USAGE ERROR ==============\n" + self.errormsg
        else:
            output = "%-40s %8s %8s %8s %8s %-20s\n" % ("Filesystem", "Size", "Used", "Avail", "Use%", "Mounted on")
            output += "\n".join(str(ln) for ln in self.lines)
            return "============= DISK USAGE ====================\n" + output


class sysctl:
    def __init__(self):
        self.variables = dict() # option name => option value
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= SYSCTL ERROR ==================\n" + self.errormsg
        else:
            output = '\n'.join('%s = %s' % (k, self.variables[k]) for k in sorted(self.variables.iterkeys()))
            return "============= SYSCTL ========================\n" + output


class limitsconf_entry:
    def __init__(self, domain, type, item, value):
        self.domain = domain
        self.type = type
        self.item = item
        self.value = value

    def __str__(self):
        return "%s %s %s %s" % (self.domain, self.type, self.item, self.value)


class limitsconf:
    def __init__(self):
        self.lines = list()
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= LIMITS ERROR ==================\n" + self.errormsg
        else:
            output = "\n".join(str(ln) for ln in self.lines)
            return "============= LIMITS ========================\n" + output


class mounts:
    def __init__(self):
        self.entries = dict() # partition => mount object
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= MOUNT ERROR ===================\n" + self.errormsg
        else:
            output = "\n".join(str(self.entries[k]) for k in sorted(self.entries.keys()))
            return "============= MOUNT =========================\n" + output


class GpMount:
    def __init__(self):
        self.partition = None
        self.dir= None
        self.type = None
        self.options = set() # mount options

    def __str__(self):
        return "%s on %s type %s (%s)" % (self.partition, self.dir, self.type, ",".join(self.options))


class ioschedulers:
    def __init__(self):
        self.devices = dict() # device name => scheduler name
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= IO SCHEDULERS ERROR ===========\n" + self.errormsg
        else:
            output = "\n".join("%s: %s" % (k, v) for (k, v) in self.devices.items())
            return "============= IO SCHEDULERS =================\n" + output


class blockdev:
    def __init__(self):
        self.ra = dict() # device name => getra value
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= BLOCKDEV RA ERROR =============\n" + self.errormsg
        else:
            output = "\n".join("%s: %s" % (k, v) for (k, v) in self.ra.items())
            return "============= BLOCKDEV RA ===================\n" + output


class ntp:
    def __init__(self):
        self.running = False
        self.hosts = set()
        self.currentime = None
        self.errormsg = None

    def __str__(self):
        if self.errormsg:
            return "============= NTPD ERROR =====================\n" + self.errormsg
        else:
            output = "(running %s) (time %f) (peers: %s)" % (self.running, self.currenttime, self.hosts)
            return "============= NTPD ==========================\n" + output


class rclocal:
    def __init__(self):
        self.isexecutable = False # check that /etc/rc.d/rc.local is executable permissions

    def __str__(self):
        return "executable(%s)" % self.isexecutable

class solaris_etc_system:
    def __init__(self):
        self.parameters = dict() # dictionary of values
        self.errormsg = None

class solaris_etc_project:
    def __init__(self):
        self.lines = list() # list of lines in the file
        self.errormsg = None

class solaris_etc_user_attr:
    def __init__(self):
        self.lines = list() # list of lines in the file
        self.errormsg = None

class GenericSolarisOutputData:

    def __init__(self):
        self.etc_system = None
        self.etc_project = None
        self.etc_user_attr = None
        self.uname = None

    def __str__(self):

        grc = "============= /etc/system====================\n"
        gre = "============= /etc/system ERRORMSG===========\n"
        output = "%s%s\n%s%s" % (grc, self.etc_system.parameters.__str__(), gre, self.etc_system.errormsg)

        grc = "============= /etc/project===================\n"
        gre = "============= /etc/project ==================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.etc_project.lines.__str__(), gre, self.etc_project.errormsg)

        grc = "============= /etc/user_att==================\n"
        gre = "============= /etc/user_att==================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.etc_user_attr.lines.__str__(), gre, self.etc_user_attr.errormsg)

        grc = "============= UNAME==========================\n"
        gre = "============= UNAME ERRORMSG=================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.uname.__str__(), gre, self.uname.errormsg)

        return output


class GenericLinuxOutputData:
    def __init__(self):
        self.uname = None
        self.machine = None
        self.hdfs = None
        self.hawq = None
        self.diskusage = None
        self.sysctl = None
        self.limitsconf = None
        self.mounts = None
        self.ioschedulers = None
        self.blockdev = None
        self.ntp = None

    def __str__(self):
        applied_checks = filter(lambda x: x is not None,
                                [ self.uname, self.machine, self.hdfs, self.hawq, self.diskusage, self.sysctl,
                                  self.limitsconf, self.mounts, self.ioschedulers, self.blockdev, self.ntp ])
        return "\n".join(map(str, applied_checks))


class ApplianceOutputData:
    def __init__(self):
        self.chkconfig = None
        self.omreport = None
        self.grubconf = None
        self.mounts = None
        self.inittab = None
        self.uname = None
        self.securetty = None
        self.bcu = None
        self.blockdev = None
        self.rclocal = None
        self.ioschedulers = None
        self.sysctl = None
        self.limitsconf = None
        self.connectemc = None
        self.ntp = None

    def __str__(self):
        ser = "=============SERVICES=======================\n"
        xin = "=============XINETD  =======================\n"
        err = "=============CHKCONFIG ERRORMSG=============\n"
        output = "%s%s\n%s%s\n%s%s" % (ser, self.chkconfig.services.__str__(), xin, self.chkconfig.xinetd.__str__(), err, self.chkconfig.errormsg)

        omr = "=============OMREPORT VERSION ==============\n"
        ome = "=============OMREPORT VERSION ERRORMSG======\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.omversion, ome, self.omreport.omversion_errormsg)

        omr = "=============OMREPORT BIOS==================\n"
        ome = "=============OMREPORT BIOS ERRORMSG=========\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.bios.__str__(), ome,self.omreport.bios_errormsg)

        omr = "=============OMREPORT BIOSSETUP=============\n"
        ome = "=============OMREPORT BIOSSETUP ERRORMSG====\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.biossetup.__str__(), ome,self.omreport.biossetup_errormsg)

        omr = "=============OMREPORT CONTROLLER============\n"
        ome = "=============OMREPORT CONTROLLER ERRORMSG===\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.controller.__str__(), ome,self.omreport.controller_errormsg)

        boo = "=============OMREPORT BOOTORDER=============\n"
        boe = "=============OMREPORT BOOTORDER ERRORMSG====\n"
        output = "%s\n%s%s\n%s%s" % (output, boo, self.omreport.bootorder.__str__(), boe, self.omreport.bootorder_errormsg)

        omr = "=============OMREPORT REMOTEACCESS==========\n"
        ome = "=============OMREPORT REMOTEACCESS ERRORMSG=\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.remoteaccess.__str__(), ome,self.omreport.remoteaccess_errormsg)

        omr = "=============OMREPORT ALERTS==========\n"
        ome = "=============OMREPORT ALERTS ERRORMSG=\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.alerts.__str__(), ome,self.omreport.alerts_errormsg)

        omr = "=============OMREPORT VIRTUAL DISKS=========\n"
        ome = "=============OMREPORT VIRTUAL DISKS ERRORMSG\n"
        output = "%s\n%s%s\n%s%s" % (output, omr, self.omreport.vdisks.__str__(), ome,self.omreport.vdisks_errormsg)

        grc = "============= GRUB.CONF======================\n"
        gre = "============= GRUB.CONF ERRORMSG=============\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.grubconf.__str__(), gre, self.grubconf.errormsg)

        grc = "============= SYSCTL=========================\n"
        gre = "============= SYSCTL ERRORMSG================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.sysctl.variables.__str__(), gre, self.sysctl.errormsg)

        grc = "============= LIMITS=========================\n"
        gre = "============= LIMITS ERRORMSG================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.limitsconf.__str__(), gre, self.limitsconf.errormsg)

        mnt = "============= MOUNT==========================\n"
        mte = "============= MOUNT ERRORMSG=================\n"
        output = "%s\n%s%s\n%s%s" % (output, mnt, self.mounts.__str__(), mte, self.mounts.errormsg)

        grc = "============= INITTAB========================\n"
        gre = "============= INITTAB ERRORMSG===============\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.inittab.__str__(), gre, self.inittab.errormsg)

        grc = "============= UNAME==========================\n"
        gre = "============= UNAME ERRORMSG=================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.uname.__str__(), gre, self.uname.errormsg)

        grc = "============= CONNECTEMC=====================\n"
        gre = "============= CONNECtEMC ERRORMSG============\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.connectemc.__str__(), gre, self.connectemc.errormsg)

        grc = "============= SECURETTY======================\n"
        gre = "============= SECURETTY ERRORMSG=============\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.securetty.data.__str__(), gre, self.securetty.errormsg)

        grc = "============= IO SCHEDULERS==================\n"
        gre = "============= IO SCHEDULERS  ERRORMSG========\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.ioschedulers.devices.__str__(), gre, self.ioschedulers.errormsg)

        grc = "============= BLOCKDEV RA ====================\n"
        gre = "============= BLOCKDEV RA ERRORMSG============\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.blockdev.ra.__str__(), gre, self.blockdev.errormsg)

        grc = "============= BCU CNA ========================\n"
        gre = "============= BCU CNA ERRORMSG================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.bcu.__str__(), gre, self.bcu.errormsg)

        grc = "============= /etc/rc.d/rc.local =============\n"
        output = "%s\n%s%s" % (output, grc, self.rclocal.__str__())

        grc = "============= NTPD ===========================\n"
        gre = "============= NTPD ERRORMSG===================\n"
        output = "%s\n%s%s\n%s%s" % (output, grc, self.ntp.__str__(), gre, self.ntp.errormsg)

        return output
