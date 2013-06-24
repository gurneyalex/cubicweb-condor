# copyright 2011 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
# contact http://www.logilab.fr -- mailto:contact@logilab.fr
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 2.1 of the License, or (at your option)
# any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
# 
# You should have received a copy of the GNU Lesser General Public License along
# with this program. If not, see <http://www.gnu.org/licenses/>.

"""
Condor job management view

"""
from __future__ import with_statement

from cwtags.tag import h1, h2, pre, div
from logilab.mtconverter import xml_escape

from cubicweb.selectors import match_user_groups
from cubicweb.view import StartupView
from cubicweb.web import formfields as ff, Redirect
from cubicweb.web.form import FormViewMixIn
from cubicweb.web.formwidgets import SubmitButton
from cubicweb.web.controller import Controller
from cubicweb.web.httpcache import NoHTTPCacheManager

from cubes.condor.commands import status, queue, remove, pool_debug

_ = unicode

class CondorJobView(FormViewMixIn, StartupView):
    __regid__ = 'condor_jobs'
    __select__ = StartupView.__select__
    title = _('view_condor_jobs')
    http_cache_manager = NoHTTPCacheManager
    condor_manager_groups = frozenset(('managers',))

    def call(self, **kwargs):
        w = self.w
        _ = self._cw._
        self._cw.html_headers.add_raw(u'<meta http-equiv="Refresh" content="91; url=%s"/>\n' %
                                          xml_escape(self._cw.build_url(vid='condor_jobs')))
        with(h1(w)):
            w(_('Condor information'))
        self.condor_queue_section()
        if self.condor_manager_groups.intersection(self._cw.user.groups):
            self.condor_remove_section()
        self.condor_status_section()
        if self.condor_manager_groups.intersection(self._cw.user.groups):
            self.condor_debug_section()

    def condor_debug_section(self):
        w = self.w
        _ = self._cw._
        with(h2(w)):
            w(_('Condor Pool Debug'))
        alert_style = """
            padding: 8px 35px 8px 14px;
            margin-bottom: 18px;
            color: #c09853;
            background-color: #fcf8e3;
            border: 1px solid #fbeed5;
        """
        with(div(w, style=alert_style)):
            w('Node Name | Domain UID | Local Credd server <br/>')
            w('<strong>Note:</strong> Domain UID and Local Credd server should be the same across all nodes'
            ' in the same pool')
        errcode, output = pool_debug(self._cw.vreg.config)
        with(pre(w)):
            w(xml_escape(output))

    def condor_status_section(self):
        w = self.w
        _ = self._cw._
        with(h2(w)):
            w(_('Condor Status'))
        errcode, output = status(self._cw.vreg.config)
        with(pre(w)):
            w(xml_escape(output))

    def condor_queue_section(self):
        w = self.w
        _ = self._cw._
        with(h2(w)):
            w(_('Condor Queue'))
        errcode, output = queue(self._cw.vreg.config)
        with(pre(w)):
            w(xml_escape(output))

    def condor_remove_section(self):
        w = self.w
        _ = self._cw._
        with(h2(w)):
            w(_('Condor Remove'))
        form = self._cw.vreg['forms'].select('base', self._cw, rset=self.cw_rset,
                                             form_renderer_id='base',
                                             domid='condor_remove',
                                             action=self._cw.build_url('do_condor_remove'),
                                             __errorurl=self._cw.build_url(vid='condor_jobs'),
                                             form_buttons=[SubmitButton()])
        form.append_field(ff.StringField(name='condor_schedd_name',
                                         label=_('Condor Schedd Name')))
        form.append_field(ff.IntField(min=0, name='condor_job_id',
                                       label=_('Condor Job ID')))
        renderer = form.default_renderer()
        def error_message(form):
            """ don't display the default error message"""
            return u''
        renderer.error_message = error_message
        form.render(w=w, renderer=renderer)

class CondorRemoveController(Controller):
    __regid__ = 'do_condor_remove'
    __select__ = Controller.__select__ & match_user_groups(CondorJobView.condor_manager_groups)

    def publish(self, rset=None):
        job_id = self._cw.form['condor_job_id']
        schedd_name = self._cw.form['condor_schedd_name']
        errcode, output = remove(self._cw.vreg.config, schedd_name, job_id)
        raise Redirect(self._cw.build_url(vid='condor_jobs',
                                          __message=xml_escape(output.strip()))
                       )
