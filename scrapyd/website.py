from datetime import datetime

import socket

from twisted.web import resource, static
from twisted.application.service import IServiceCollection

from scrapy.utils.misc import load_object

from .interfaces import IPoller, IEggStorage, ISpiderScheduler

from urlparse import urlparse

class Root(resource.Resource):

    def __init__(self, config, app):
        resource.Resource.__init__(self)
        self.debug = config.getboolean('debug', False)
        self.runner = config.get('runner')
        logsdir = config.get('logs_dir')
        itemsdir = config.get('items_dir')
        local_items = itemsdir and (urlparse(itemsdir).scheme.lower() in ['', 'file'])
        self.app = app
        self.nodename = config.get('node_name', socket.gethostname())
        self.putChild('', Home(self, local_items))
        if logsdir:
            self.putChild('logs', static.File(logsdir, 'text/plain'))
        if local_items:
            self.putChild('items', static.File(itemsdir, 'text/plain'))
        self.putChild('jobs', Jobs(self, local_items))
        services = config.items('services', ())
        for servName, servClsName in services:
          servCls = load_object(servClsName)
          self.putChild(servName, servCls(self))
        self.update_projects()

    def update_projects(self):
        self.poller.update_projects()
        self.scheduler.update_projects()

    @property
    def launcher(self):
        app = IServiceCollection(self.app, self.app)
        return app.getServiceNamed('launcher')

    @property
    def scheduler(self):
        return self.app.getComponent(ISpiderScheduler)

    @property
    def eggstorage(self):
        return self.app.getComponent(IEggStorage)

    @property
    def poller(self):
        return self.app.getComponent(IPoller)


class Home(resource.Resource):

    def __init__(self, root, local_items):
        resource.Resource.__init__(self)
        self.root = root
        self.local_items = local_items

    def render_GET(self, txrequest):
        vars = {
            'projects': ', '.join(self.root.scheduler.list_projects()),
        }
        s = """
<html>
<head><title>Mindrop Crawlers</title></head>
<body>
<h1>Mindrop Crawlers</h1>
<p>Available projects: <b>%(projects)s</b></p>
<ul>
<li><a href="/jobs">Jobs</a></li>
""" % vars
        s += """
<li><a href="/logs/">Logs</a></li>

</ul>
</body>
</html>
""" % vars
        return s


class Jobs(resource.Resource):

    def __init__(self, root, local_items):
        resource.Resource.__init__(self)
        self.root = root
        self.local_items = local_items
        self.TITLE = 'Midrop Crawler Jobs'
        self.PAGE_TITLE = 'Jobs'
        self.BACKROUND_COLOR = '#063549'

    def render(self, txrequest):
        cols = 7
        s = ""
        s += self.render_header()
        s += self.render_menu()
        s += self.render_table()
        s += self.render_footer()
        return s

    def render_pending(self):
        s = ""
        for project, queue in self.root.poller.queues.items():
            for m in queue.list():
                s += "<tr>"
                s += "<td>%s</td>" % project
                s += "<td>"+'<a href="http://'+str(m['domain'])+'" target="_blank">'+str(m['domain'])+'</a>'+"</td>"
                s += "<td>%s</td>" % str(m['name'])
                s += "<td>%s</td>" % str(m['_job'])
                s += "</tr>"
        return s

    def render_running(self):
        s = ""
        for p in self.root.launcher.processes.values():
            s += "<tr>"
            s += "<td>%s</td>" % getattr(p, 'project')
            s += "<td>" + '<a href="http://'+getattr(p, 'domain')+'" target="_blank">'+getattr(p, 'domain')+'</a>' + "</td>"
            s += "<td>%s</td>" % getattr(p, 'spider')
            s += "<td>%s</td>" % getattr(p, 'job')
            s += "<td>%s</td>" % getattr(p, 'pid')
            s += "<td>%s</td>" % (datetime.now() - p.start_time)
            s += "<td><a href='/logs/%s/%s/%s.log'>Log</a></td>" % (p.project, p.spider, p.job)
            s += "</tr>"
        return s


    def render_finished(self):
        s = ""
        for p in self.root.launcher.finished:
            s += "<tr>"
            s += "<td>%s</td>" % getattr(p, 'project')
            s += "<td>" + '<a href="http://'+getattr(p, 'domain')+'" target="_blank">'+getattr(p, 'domain')+'</a>' + "</td>"
            s += "<td>%s</td>" % getattr(p, 'spider')
            s += "<td>%s</td>" % getattr(p, 'job')
            s += "<td></td>"
            s += "<td>%s</td>" % (p.end_time - p.start_time)
            s += "<td><a href='/logs/%s/%s/%s.log'>Log</a></td>" % (p.project, p.spider, p.job)
            s += "</tr>"
        return str(s)

    def render_header(self):
        s = ""
        s += "<html><head><title>"+self.TITLE+"</title></head>"
        s += "<body>"
        s += "<h1>"+self.PAGE_TITLE+"</h1>"
        return s

    def render_menu(self):
        s = ""
        s += "<p><a href='..'>Go back</a></p>"
        return s

    def render_footer(self):
        s = ""
        s += "</body>"
        s += "</html>"
        return s

    def render_table(self):
        cols = 7
        s = ""
        s += "<table border='1'>"
        s += "<tr><th>Project</th><th>Url</th><th>Spider</th><th>Job</th><th>PID</th><th>Runtime</th><th>Log</th>"
        s += "</tr>"
        s += "<tr><th colspan='"+str(cols)+"' style='background-color: "+self.BACKROUND_COLOR+"; color: #fff;'>Pending</th></tr>"
        s += self.render_pending()
        s += "<tr><th colspan='"+str(cols)+"' style='background-color: "+self.BACKROUND_COLOR+"; color: #fff;'>Running</th></tr>"
        s += self.render_running()
        s += "<tr><th colspan='"+str(cols)+"' style='background-color: "+self.BACKROUND_COLOR+"; color: #fff;'>Finished</th></tr>"
        s += self.render_finished()
        s += "</table>"
        return s