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

import subprocess
from cwtags.tag import a, h1, h2, p, table, tr, td, li, ul, pre
from logilab.mtconverter import xml_escape

from cubicweb.selectors import match_user_groups
from cubicweb.view import StartupView
from cubicweb.web import formfields as ff, Redirect
from cubicweb.web.form import FormViewMixIn
from cubicweb.web.formwidgets import SubmitButton
from cubicweb.web.controller import Controller
from cubicweb.web.httpcache import NoHTTPCacheManager

from cubes.condor.commands import status, queue, remove

class CondorJobView(FormViewMixIn, StartupView):
    __regid__ = 'condor_jobs'
    __select__ = StartupView.__select__ & match_user_groups('managers')
    title = _('view_condor_jobs')
    http_cache_manager = NoHTTPCacheManager

    def call(self, **kwargs):
        w = self.w
        _ = self._cw._
        self._cw.html_headers.add_raw(u'<meta http-equiv="Refresh" content="91; url=%s"/>\n' %
                                          xml_escape(self._cw.build_url(vid='condor_jobs')))
        with(h1(w)):
            w(_('Condor information'))
        self.condor_queue_section()
        self.condor_remove_section()
        self.condor_status_section()

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
    __select__ = Controller.__select__ & match_user_groups('managers')
    
    def publish(self, rset=None):
        job_id = self._cw.form['condor_job_id']
        errcode, output = remove(self._cw.vreg.config, job_id)
        raise Redirect(self._cw.build_url(vid='condor_jobs',
                                          __message=xml_escape(output.strip()))
                       )