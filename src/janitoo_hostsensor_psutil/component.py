# -*- coding: utf-8 -*-
"""The Raspberry hardware worker

Define a node for the cpu with 3 values : temperature, frequency and voltage

http://www.maketecheasier.com/finding-raspberry-pi-system-information/

"""

__license__ = """
    This file is part of Janitoo.

    Janitoo is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Janitoo is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Janitoo. If not, see <http://www.gnu.org/licenses/>.

"""
__author__ = 'Sébastien GALLET aka bibi21000'
__email__ = 'bibi21000@gmail.com'
__copyright__ = "Copyright © 2013-2014-2015-2016 Sébastien GALLET aka bibi21000"

import logging
logger = logging.getLogger(__name__)

import os, sys
import threading
import datetime
from pkg_resources import get_distribution, DistributionNotFound
import psutil

from janitoo.thread import JNTBusThread
from janitoo.options import get_option_autostart
from janitoo.utils import HADD
from janitoo.node import JNTNode
from janitoo.bus import JNTBus
from janitoo.component import JNTComponent
from janitoo.value import JNTValue, value_config_poll

##############################################################
#Check that we are in sync with the official command classes
#Must be implemented for non-regression
from janitoo.classes import COMMAND_DESC

COMMAND_METER = 0x0032
COMMAND_CONFIGURATION = 0x0070

assert(COMMAND_DESC[COMMAND_METER] == 'COMMAND_METER')
assert(COMMAND_DESC[COMMAND_CONFIGURATION] == 'COMMAND_CONFIGURATION')
##############################################################

def make_disks(**kwargs):
    return Disks(**kwargs)

def make_processes(**kwargs):
    return Processes(**kwargs)

class PSUtilComponent(JNTComponent):
    def __init__(self, **kwargs):
        JNTComponent.__init__(self, product_type="Software", product_manufacturer="PSUtil", **kwargs)
        logger.debug("[%s] - __init__ node uuid:%s", self.__class__.__name__, self.uuid)
        self._lock =  threading.Lock()
        self._psutil_last = False
        self._psutil_next_run = datetime.datetime.now() + datetime.timedelta(seconds=5)
        self._psutil_thread = None

    def check_heartbeat(self):
        """Check that the component is 'available'

        """
        #~ print "it's me %s : %s" % (self.values['upsname'].data, self._ups_stats_last)
        return self._psutil_last

    def start(self, mqttc):
        """Start the component. Can be used to start a thread to acquire data.

        """
        JNTComponent.start(self, mqttc)
        self.get_psutil()
        return True

    def get_psutil(self):
        """
        """
        if self._psutil_next_run < datetime.datetime.now():
            locked = self._lock.acquire(False)
            if locked == True:
                logger.debug("Got the lock !!!")
                if self._psutil_thread is not None and self._psutil_thread.is_alive() == True:
                    logger.warning("Start a new thread but the old one is already running. This should be a memory leak")
                self._psutil_thread = threading.Thread(target = self.psutil_thread)
                self._psutil_thread.start()

class Disks(JNTComponent):
    """ Use psutil to retrieve informations. """

    def __init__(self, bus=None, addr=None, **kwargs):
        JNTComponent.__init__(self, oid='hostsensor.disks', bus=bus, addr=addr, name="Disks statistics",
                product_name="Disks statistics", product_type="Software", product_manufacturer="PSUtil", **kwargs)
        logger.debug("[%s] - __init__ node uuid:%s", self.__class__.__name__, self.uuid)

        self._psutil = None
        self._psutil_last = False
        self._lock =  threading.Lock()
        self._psutil_next_run = datetime.datetime.now() + datetime.timedelta(seconds=5)
        uuid="partition"
        self.values[uuid] = self.value_factory['sensor_string'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The partition list',
            label='Partition',
            get_data_cb=self.get_partition,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The partition path', label='partition', get_data_cb=self.get_config, type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=1800)
        self.values[poll_value.uuid] = poll_value

        uuid="total"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The total size of partitions',
            label='Total',
            genre=0x01,
            get_data_cb=self.get_total,
        )
        config_value = self.values[uuid].create_config_value(help='The partition path', label='partition', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=900)
        self.values[poll_value.uuid] = poll_value

        uuid="used"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The used size of partitions',
            label='Used',
            genre=0x01,
            get_data_cb=self.get_used,
        )
        config_value = self.values[uuid].create_config_value(help='The partition path', label='partition', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=900)
        self.values[poll_value.uuid] = poll_value

        uuid="free"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The free size of partitions',
            label='Free',
            genre=0x01,
            get_data_cb=self.get_free,
        )
        config_value = self.values[uuid].create_config_value(help='The partition path', label='partition', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=900)
        self.values[poll_value.uuid] = poll_value

        uuid="percent_use"
        self.values[uuid] = self.value_factory['sensor_percent'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The percent_use of partitions',
            label='Percent_use',
            genre=0x01,
            get_data_cb=self.get_percent_use,
        )
        config_value = self.values[uuid].create_config_value(help='The partition path', label='partition', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=900)
        self.values[poll_value.uuid] = poll_value

    def get_config(self, node_uuid, index):
        """
        """
        if index == 0:
            res = self.values['partition'].get_config(node_uuid, index)
            if res is None:
                #~ self._lock.acquire()
                try:
                    #~ if self.values['partition'].get_config(node_uuid, index) is None:
                        parts = psutil.disk_partitions()
                        idx = 0
                        for part in parts:
                            self.values['partition'].set_config(node_uuid, idx, part.mountpoint)
                            self.values['total'].set_config(node_uuid, idx, part.mountpoint)
                            self.values['used'].set_config(node_uuid, idx, part.mountpoint)
                            self.values['free'].set_config(node_uuid, idx, part.mountpoint)
                            self.values['percent_use'].set_config(node_uuid, idx, part.mountpoint)
                            idx += 1
                except:
                    logger.exception("[%s] - Exception in get_config", self.__class__.__name__)
                #~ finally:
                    #~ self._lock.release()
        return self.values['partition'].instances[index]['config']

    def get_partition(self, node_uuid, index):
        """
        """
        return self.values['partition'].instances[index]['config']

    def get_total(self, node_uuid, index):
        """
        """
        self.get_psutil()
        uuid = 'total'
        try:
            for i in range(0, self.values[uuid].get_max_index(node_uuid)+1):
                self.values[uuid].set_data_index(index=i, data=self._psutil[i][uuid])
            return self._psutil[index][uuid]
        except:
            logger.exception("[%s] - Exception in get_total", self.__class__.__name__)
        return None

    def get_used(self, node_uuid, index):
        """
        """
        self.get_psutil()
        uuid = 'used'
        try:
            for i in range(0, self.values[uuid].get_max_index(node_uuid)+1):
                self.values[uuid].set_data_index(index=i, data=self._psutil[i][uuid])
            return self._psutil[index][uuid]
        except:
            logger.exception("[%s] - Exception in get_used", self.__class__.__name__)
        return None

    def get_free(self, node_uuid, index):
        """
        """
        self.get_psutil()
        uuid = 'free'
        try:
            for i in range(0, self.values[uuid].get_max_index(node_uuid)+1):
                self.values[uuid].set_data_index(index=i, data=self._psutil[i][uuid])
            return self._psutil[index][uuid]
        except:
            logger.exception("[%s] - Exception in get_free", self.__class__.__name__)
        return None

    def get_percent_use(self, node_uuid, index):
        """
        """
        self.get_psutil()
        uuid = 'percent_use'
        try:
            for i in range(0, self.values[uuid].get_max_index(node_uuid)+1):
                self.values[uuid].set_data_index(index=i, data=self._psutil[i][uuid])
            return self._psutil[index][uuid]
        except:
            logger.exception("[%s] - Exception in get_percent_use", self.__class__.__name__)
        return None

    def check_heartbeat(self):
        """Check that the component is 'available'

        """
        return self._psutil_last

    def get_psutil(self):
        """
        """
        if self._psutil_next_run < datetime.datetime.now():
            locked = self._lock.acquire(False)
            if locked == True:
                try:
                    self._psutil = {}
                    self._psutil_last = True
                    for index in self.values['partition'].instances:
                        if index not in self._psutil:
                            self._psutil[index] = {'total':None, 'used':None, 'free':None, 'percent_use':None}
                        try:
                            a, b, c, d = psutil.disk_usage(self.values['partition'].instances[index]['config'])
                            #~ print self.values['partition'].instances[index]['config'], a, b, c, d
                            self._psutil[index]['total'], self._psutil[index]['used'], \
                            self._psutil[index]['free'], self._psutil[index]['percent_use'] = \
                                psutil.disk_usage(self.values['partition'].instances[index]['config'])
                            #~ print self.values['partition'].instances[index]['config'], self._psutil[index]['total']
                        except:
                            self._psutil_last = False
                            logger.exception("[%s] - Exception in get_psutil", self.__class__.__name__)
                except:
                    self._psutil_last = False
                    logger.exception("[%s] - Exception in get_psutil", self.__class__.__name__)
                finally:
                    self._lock.release()
            min_poll = 9999
            for val_id in ["total", "used", "free", "percent_use"]:
                if self.values["%s_poll"%val_id].data > 0:
                    min_poll=min(min_poll, self.values["%s_poll"%val_id].data)
            self._psutil_next_run = datetime.datetime.now() + datetime.timedelta(seconds=min_poll)

class Processes(PSUtilComponent):
    """ Use psutil to retrieve informations. """

    def __init__(self, bus=None, addr=None, **kwargs):
        PSUtilComponent.__init__(self, oid='hostsensor.processes', bus=bus, addr=addr, name="Processes statistics",
                product_name="Processes monitoring", **kwargs)
        logger.debug("[%s] - __init__ node uuid:%s", self.__class__.__name__, self.uuid)

        uuid="memory_rss"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The RSS memory',
            label='RSS memory',
            get_data_cb=self.get_memory_rss,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="memory_vms"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The VMS memory',
            label='VMS memory',
            get_data_cb=self.get_memory_vms,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="io_counters_write"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The io_counters_write',
            label='io_counters_write',
            get_data_cb=self.get_io_counters_write,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=0, is_polled=False)
        self.values[poll_value.uuid] = poll_value

        uuid="io_counters_read"
        self.values[uuid] = self.value_factory['sensor_memory'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The io_counters_read',
            label='io_counters_read',
            get_data_cb=self.get_io_counters_read,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=0, is_polled=False)
        self.values[poll_value.uuid] = poll_value

        uuid="connections"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The connections',
            label='connections',
            get_data_cb=self.get_connections,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="num_threads"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The num_threads',
            label='num_threads',
            get_data_cb=self.get_num_threads,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="open_files"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The open_files',
            label='open_files',
            get_data_cb=self.get_open_files,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=0, is_polled=False)
        self.values[poll_value.uuid] = poll_value

        uuid="num_ctx_switches_voluntary"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The num_ctx_switches_voluntary',
            label='num_ctx_switches_voluntary',
            get_data_cb=self.get_num_ctx_switches_voluntary,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=0, is_polled=False)
        self.values[poll_value.uuid] = poll_value

        uuid="num_ctx_switches_involuntary"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The num_ctx_switches_involuntary',
            label='num_ctx_switches_involuntary',
            get_data_cb=self.get_num_ctx_switches_involuntary,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=0, is_polled=False)
        self.values[poll_value.uuid] = poll_value

        uuid="num_fds"
        self.values[uuid] = self.value_factory['sensor_integer'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The num_fds',
            label='num_fds',
            get_data_cb=self.get_num_fds,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="cpu_percent"
        self.values[uuid] = self.value_factory['sensor_percent'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The cpu_percent',
            label='cpu_percent',
            get_data_cb=self.get_cpu_percent,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

        uuid="memory_percent"
        self.values[uuid] = self.value_factory['sensor_percent'](options=self.options, uuid=uuid,
            node_uuid=self.uuid,
            help='The memory_percent',
            label='memory_percent',
            get_data_cb=self.get_memory_percent,
            genre=0x01,
        )
        config_value = self.values[uuid].create_config_value(help='The pid file of the service', label='Pid', type=0x08)
        self.values[config_value.uuid] = config_value
        poll_value = self.values[uuid].create_poll_value(default=300)
        self.values[poll_value.uuid] = poll_value

    def get_memory_rss(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["memory_rss"].get_data_index(node_uuid=node_uuid, index=index)

    def get_memory_vms(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["memory_vms"].get_data_index(node_uuid=node_uuid, index=index)

    def get_io_counters_write(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["io_counters_write"].get_data_index(node_uuid=node_uuid, index=index)

    def get_io_counters_read(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["io_counters_read"].get_data_index(node_uuid=node_uuid, index=index)

    def get_connections(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["connections"].get_data_index(node_uuid=node_uuid, index=index)

    def get_num_threads(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["num_threads"].get_data_index(node_uuid=node_uuid, index=index)

    def get_open_files(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["open_files"].get_data_index(node_uuid=node_uuid, index=index)

    def get_num_ctx_switches_voluntary(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["num_ctx_switches_voluntary"].get_data_index(node_uuid=node_uuid, index=index)

    def get_num_ctx_switches_involuntary(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["num_ctx_switches_involuntary"].get_data_index(node_uuid=node_uuid, index=index)

    def get_num_fds(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["num_fds"].get_data_index(node_uuid=node_uuid, index=index)

    def get_cpu_percent(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["cpu_percent"].get_data_index(node_uuid=node_uuid, index=index)

    def get_memory_percent(self, node_uuid, index):
        self.get_psutil()
        if self._psutil_last == True:
            return self.values["memory_percent"].get_data_index(node_uuid=node_uuid, index=index)

    def psutil_thread(self):
        """
        """
        logger.debug("Start the threaded function")
        try:
            active_ids = ['memory_rss', 'memory_vms', 'io_counters_read', 'io_counters_write', 'connections', 'num_threads',
                        'open_files', 'num_ctx_switches_voluntary', 'num_ctx_switches_involuntary', 'num_fds',
                        'cpu_percent', 'memory_percent']
            try:
                _psutil = {}
                pids={}
                for val_id in active_ids:
                    if val_id not in pids:
                        pids[val_id] = {}
                    configs = self.values[val_id].get_index_configs()
                    for pidf in configs :
                        try:
                            if os.path.isfile(pidf) == True:
                                with open(pidf, "r") as fpi:
                                    for line in fpi:
                                        val = 0
                                        try :
                                            val = int(line)
                                        except :
                                            pass
                                        if val != 0 :
                                            pidname = pidf.split("/")
                                            pname = pidname[len(pidname)-1].split(".")[0]
                                            pids[val_id][val]=pidf
                        except :
                            logger.exception("Exception when reading pid file")
                        _psutil[pidf] = {}
                        _psutil[pidf]['memory_rss'] = None
                        _psutil[pidf]['memory_vms'] = None
                        _psutil[pidf]['io_counters_read'] = None
                        _psutil[pidf]['io_counters_write'] = None
                        _psutil[pidf]['connections'] = None
                        _psutil[pidf]['num_threads'] = None
                        _psutil[pidf]['cpu_percent'] = None
                        _psutil[pidf]['memory_percent'] = None
                        _psutil[pidf]['open_files'] = None
                        _psutil[pidf]['num_ctx_switches_voluntary'] = None
                        _psutil[pidf]['num_ctx_switches_involuntary'] = None
                        _psutil[pidf]['num_fds'] = None
                procs = [p for p in psutil.process_iter()]
                for proc in procs[:]:
                    for key in pids.keys():
                        if proc.pid in pids[key] :
                            for config in configs:
                                if config == pids[key][proc.pid]:
                                    if config not in _psutil:
                                        _psutil[config] = {}
                                    try:
                                        _psutil[config]['memory_rss'], _psutil[config]['memory_vms'] = proc.memory_info()
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['io_counters_read'] = proc.io_counters().read_bytes
                                        _psutil[config]['io_counters_write'] = proc.io_counters().write_bytes
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['num_threads'] = proc.num_threads()
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['cpu_percent'] = round(proc.cpu_percent(interval=1.0), 2)
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['memory_percent'] = round(proc.memory_percent(), 2)
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['open_files'] = len(proc.open_files())
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        res=proc.num_ctx_switches()
                                        _psutil[config]['num_ctx_switches_voluntary'] = res.voluntary
                                        _psutil[config]['num_ctx_switches_involuntary'] = res.involuntary
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['num_fds'] = proc.num_fds()
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                                    try:
                                        _psutil[config]['connections'] = len(proc.connections())
                                    except:
                                        logger.exception("Exception catched when reading psutil")
                for val_id in active_ids:
                    for config in self.values[val_id].get_index_configs():
                        for which in _psutil:
                            if config == which:
                                self.values[val_id].set_data_index(config=config, data=_psutil[config][val_id])
                self._psutil_last = True
            except:
                logger.exception("[%s] - Exception in get_psutil", self.__class__.__name__)
                self._psutil_last = False
        except:
            logger.exception("[%s] - Exception in lock", self.__class__.__name__)
        finally:
            self._lock.release()
            logger.debug("And finally release the lock !!!")
        min_poll=9999
        for val_id in active_ids:
            if self.values["%s_poll"%val_id].data > 0:
                min_poll=min(min_poll, self.values["%s_poll"%val_id].data)
        self._psutil_next_run = datetime.datetime.now() + datetime.timedelta(seconds=min_poll)

