# -*- coding: utf-8 -*-

"""
    tikz
    ~~~~

    Render TikZ pictures

    We make use of

    * latex
    * pdftoppm (part of Poppler)
    * pnmcrop (part of Netpbm)
    * pnmtopng (part of Netpbm)

    Author: Christoph Reller
    Email: creller@ee.ethz.ch
    Version: 0.2
    $Date: 2010-12-17 15:32:57 +0100 (Fri, 17 Dec 2010) $
    $Revision: 29 $
"""    

import tempfile
import posixpath
import shutil
import sys
from os import path, getcwd, chdir, mkdir, system
from subprocess import Popen, PIPE, call
try:
    from hashlib import sha1 as sha
except ImportError:
    from sha import sha

from docutils import nodes
from docutils.parsers.rst import directives

from sphinx.errors import SphinxError
try:
    from sphinx.util.osutil import ensuredir, ENOENT, EPIPE
except:
    from sphinx.util import ensuredir, ENOENT, EPIPE
    
from sphinx.util.compat import Directive

class TikzExtError(SphinxError):
    category = 'Tikz extension error'

class tikz(nodes.General, nodes.Element):
    pass

class TikzDirective(Directive):
    has_content = True
    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = True
    option_spec = {'libs':directives.unchanged}

    def run(self):
        node = tikz()
        node['tikz'] = '\n'.join(self.content)
        node['caption'] = '\n'.join(self.arguments)
        node['libs'] = self.options.get('libs', '')
        return [node]

DOC_HEAD = r'''
\documentclass[12pt]{article}
\usepackage[utf8]{inputenc}
\usepackage{amsmath}
\usepackage{amsthm}
\usepackage{amssymb}
\usepackage{amsfonts}
\usepackage{bm}
\usepackage{tikz}
\usetikzlibrary{%s}
\pagestyle{empty}
'''

DOC_BODY = r'''
\begin{document}
\begin{tikzpicture}
%s
\end{tikzpicture}
\end{document}
'''

def render_tikz(self,tikz,libs):
    hashkey = tikz.encode('utf-8')
    fname = 'tikz-%s.png' % (sha(hashkey).hexdigest())
    relfn = posixpath.join(self.builder.imgpath, fname)
    outfn = path.join(self.builder.outdir, '_images', fname)

    if path.isfile(outfn):
        return relfn

    if hasattr(self.builder, '_tikz_warned'):
        return None
    
    ensuredir(path.dirname(outfn))
    curdir = getcwd()

    latex = DOC_HEAD % libs
    latex += self.builder.config.tikz_latex_preamble
    tikzz = tikz % {'wd': curdir}
    latex += DOC_BODY % tikzz
    if isinstance(latex, unicode):
        latex = latex.encode('utf-8')

    if not hasattr(self.builder, '_tikz_tempdir'):
        tempdir = self.builder._tikz_tempdir = tempfile.mkdtemp()
    else:
        tempdir = self.builder._tikz_tempdir

    chdir(tempdir)

    tf = open('tikz.tex', 'w')
    tf.write(latex)
    tf.close()

    try:
        try:
            p = Popen(['pdflatex', '--interaction=nonstopmode', 'tikz.tex'],
                      stdout=PIPE, stderr=PIPE)
        except OSError, err:
            if err.errno != ENOENT:   # No such file or directory
                raise
            self.builder.warn('LaTeX command cannot be run')
            self.builder._tikz_warned = True
            return None
    finally:
        chdir(curdir)

    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise TikzExtError('latex exited with error:\n[stderr]\n%s\n'
                           '[stdout]\n%s' % (stderr, stdout))

    chdir(tempdir)

    try:
        p = Popen(['pdftoppm', '-cropbox', '-png', '-r', '120', 'tikz.pdf', 'tikz'],
                  stdout=PIPE, stderr=PIPE)
    except OSError, e:
        if e.errno != ENOENT:   # No such file or directory
            raise
        self.builder.warn('pdftoppm command cannot be run')
        self.builder.warn(err)
        self.builder._tikz_warned = True
        chdir(curdir)
        return None
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        self.builder._tikz_warned = True
        raise TikzExtError('pdftoppm exited with error:\n[stderr]\n%s\n'
                           '[stdout]\n%s' % (stderr, stdout))

    try:
        p = Popen(['convert', 'tikz-1.png', '-trim', outfn])
    except OSError, e:
        if e.errno != ENOENT:   # No such file or directory
            raise
        self.builder.warn('convert command cannot be run')
        self.builder.warn(err)
        self.builder._tikz_warned = True
        chdir(curdir)
        return None
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        self.builder._tikz_warned = True
        raise TikzExtError('convert exited with error:\n[stderr]\n%s\n'
                           '[stdout]\n%s' % (stderr, stdout))

    chdir(curdir)
    return relfn

def html_visit_tikz(self,node):
    # print "\n***********************************"
    # print "You have entered the following argument"
    # print "***********************************"
    # print node['caption']
    # print "***********************************"
    # print "You have entered the following tikzlibraries"
    # print "***********************************"
    # print node['libs']
    # print "\n***********************************"
    # print "You have entered the following tikz-code"
    # print "***********************************"
    # print node['tikz']
    # print "***********************************"

    libs = self.builder.config.tikz_tikzlibraries + ',' + node['libs']
    libs = libs.replace(' ', '').replace('\t', '').strip(', ')

    try:
        fname = render_tikz(self,node['tikz'],libs)
    except TikzExtError, exc:
        info = str(exc)[str(exc).find('!'):-1]
        sm = nodes.system_message(info, type='WARNING', level=2,
                                  backrefs=[], source=node['tikz'])
        sm.walkabout(self)
        self.builder.warn('display latex %r: \n' % node['tikz'] + str(exc))
        raise nodes.SkipNode
    if fname is None:
        # something failed -- use text-only as a bad substitute
        self.body.append('<span class="math">%s</span>' %
                         self.encode(node['tikz']).strip())
    else:
        self.body.append(self.starttag(node, 'div', CLASS='figure'))
        self.body.append('<p>')
        self.body.append('<img src="%s" alt="%s" /></p>\n' %
                         (fname, self.encode(node['tikz']).strip()))
        if node['caption']:
            self.body.append('<p class="caption">%s</p>' % \
                             self.encode(node['caption']).strip())
        self.body.append('</div>')
        raise nodes.SkipNode

def latex_visit_tikz(self, node):
    if node['caption']:
        latex = '\\begin{figure}[htp]\\centering\\begin{tikzpicture}' + \
                node['tikz'] + '\\end{tikzpicture}' + '\\caption{' + \
                self.encode(node['caption']).strip() + '}\\end{figure}'
    else:
        latex = '\\begin{center}\\begin{tikzpicture}' + node['tikz'] + \
            '\\end{tikzpicture}\\end{center}'
    self.body.append(latex)

def depart_tikz(self,node):
    pass

def cleanup_tempdir(app, exc):
    if exc:
        return
    if not hasattr(app.builder, '_tikz_tempdir'):
        return
    try:
        shutil.rmtree(app.builder._tikz_tempdir)
    except Exception:
        pass

def setup(app):
    app.add_node(tikz,
                 html=(html_visit_tikz, depart_tikz),
                 latex=(latex_visit_tikz, depart_tikz))
    app.add_directive('tikz', TikzDirective)
    app.add_config_value('tikz_latex_preamble', '', 'html')
    app.add_config_value('tikz_tikzlibraries', '', 'html')
    app.add_config_value('tikz_transparent', True, False)
    app.connect('build-finished', cleanup_tempdir)
