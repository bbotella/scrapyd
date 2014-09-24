from dateutil import parser
import json
import sys
from datetime import datetime
from multiprocessing import cpu_count

from twisted.internet import reactor, defer, protocol, error
from twisted.application.service import Service
from twisted.python import log

from scrapy.utils.python import stringify_dict
from scrapyd.scheduler import SpiderScheduler
from scrapyd.utils import get_crawl_args, get_spider_running, get_spider_finished
from scrapyd import __version__
from .interfaces import IPoller, IEnvironment

class Launcher(Service):

    name = 'launcher'

    def __init__(self, config, app):
        self.processes = {}
        self.finished = []
        self.finished_to_keep = config.getint('finished_to_keep', 100)
        self.max_proc = self._get_max_proc(config)
        self.runner = config.get('runner', 'scrapyd.runner')
        self.app = app
        self.config = config

    def startService(self):
        spider_scheduler = SpiderScheduler(self.config)
        running = get_spider_running(self.config)
        for project in running.keys():
            runner_db = running[project]
            for item in runner_db.iteritems():
                spider_scheduler.schedule(project, str(item[1]['_spider']), _job=str(item[0]), domain=str(item[1]['domain']), settings=item[1]['settings'])
            finished_jobs = get_spider_finished(self.config)
            finished_db = finished_jobs[project]
            for item in finished_db.iteritems():
                item = json.loads(item[1])
                pp = ScrapyProcessProtocol(item['slot'], item['project'], item['spider'], item['job'], item['env'], domain=item['domain'])
                pp.end_time = parser.parse(item['end_time'])
                pp.start_time = parser.parse(item['start_time'])
                self.finished.append(pp)

        for slot in range(self.max_proc):
            self._wait_for_project(slot)
        log.msg(format='Scrapyd %(version)s started: max_proc=%(max_proc)r, runner=%(runner)r',
                version=__version__, max_proc=self.max_proc,
                runner=self.runner, system='Launcher')

    def _wait_for_project(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)

    def _spawn_process(self, message, slot):
        msg = stringify_dict(message, keys_only=False)
        project = msg['_project']
        running = get_spider_running(self.config)
        runner_db = running[project]
        runner_db.__setitem__(msg['_job'], msg)
        args = [sys.executable, '-m', self.runner, 'crawl']
        args += get_crawl_args(msg)
        e = self.app.getComponent(IEnvironment)
        env = e.get_environment(msg, slot)
        env = stringify_dict(env, keys_only=False)
        pp = ScrapyProcessProtocol(slot, project, msg['_spider'], \
            msg['_job'], env, msg['domain'])
        pp.deferred.addBoth(self._process_finished, slot)
        reactor.spawnProcess(pp, sys.executable, args=args, env=env)
        self.processes[slot] = pp

    def _process_finished(self, msg, slot):
        process = self.processes.pop(slot)
        process.end_time = datetime.now()
        self.finished.append(process)
        running = get_spider_running(self.config)
        finished_jobs = get_spider_finished(self.config)
        project = msg.project
        runner_db = running[project]
        finished_db = finished_jobs[project]
        runner_db.__delitem__(msg.job)
        msg.protocol_dict['end_time'] = str(process.end_time)
        finished_db.__setitem__(msg.job, msg.get_json())
        del self.finished[:-self.finished_to_keep] # keep last 100 finished jobs
        self._wait_for_project(slot)

    def _get_max_proc(self, config):
        max_proc = config.getint('max_proc', 0)
        if not max_proc:
            try:
                cpus = cpu_count()
            except NotImplementedError:
                cpus = 1
            max_proc = cpus * config.getint('max_proc_per_cpu', 4)
        return max_proc

class ScrapyProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, slot, project, spider, job, env, domain=None):
        self.slot = slot
        self.pid = None
        self.project = project
        self.spider = spider
        self.job = job
        self.start_time = datetime.now()
        self.end_time = None
        self.env = env
        self.logfile = env.get('SCRAPY_LOG_FILE')
        self.itemsfile = env.get('SCRAPY_FEED_URI')
        self.deferred = defer.Deferred()
        self.domain = domain
        self.protocol_dict = {}
        self.protocol_dict['slot'] = slot
        self.protocol_dict['pid'] = self.pid
        self.protocol_dict['project'] = project
        self.protocol_dict['start_time'] = str(self.start_time)
        self.protocol_dict['end_time'] = self.end_time
        self.protocol_dict['env'] = env
        self.protocol_dict['domain'] = domain
        self.protocol_dict['spider'] = spider
        self.protocol_dict['job'] = job


    def outReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stdout" % self.pid)

    def errReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stderr" % self.pid)

    def connectionMade(self):
        self.pid = self.transport.pid
        self.protocol_dict['pid'] = self.pid
        self.log("Process started: ")

    def processEnded(self, status):
        if isinstance(status.value, error.ProcessDone):
            self.log("Process finished: ")
        else:
            self.log("Process died: exitstatus=%r " % status.value.exitCode)
        self.deferred.callback(self)

    def log(self, action):
        fmt = '%(action)s project=%(project)r spider=%(spider)r job=%(job)r pid=%(pid)r log=%(log)r items=%(items)r'
        log.msg(format=fmt, action=action, project=self.project, spider=self.spider,
                job=self.job, pid=self.pid, log=self.logfile, items=self.itemsfile)

    def get_json(self):
        return json.dumps(self.protocol_dict)