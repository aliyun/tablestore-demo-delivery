#!/bin/env python2.7
# -*- python -*-

# The MIT License (MIT)

# Copyright (c) 2015 tyf00@aliyun.com (https://github.com/TimeExceed/water)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import os.path as op
import stat
import shutil as sh
import subprocess as sp
import random
from datetime import datetime
import hashlib
import zipfile
import re
import collections

env = Environment()
mode = ARGUMENTS.get('mode', 'debug')
env['BUILD_DIR'] = env.Dir('build/%s' % mode)
env['TMP_DIR_PATTERN'] = '$BUILD_DIR/tmp/%s.%s.%s/'
env['RANDOM'] = random.Random()
env['HEADER_DIR'] = env.Dir('$BUILD_DIR/include')
env['BIN_DIR'] = env.Dir('$BUILD_DIR/bin')
env['LIB_DIR'] = env.Dir('$BUILD_DIR/lib')
env.SetOption('duplicate', 'soft-hard-copy')
env.Decider('MD5')

# helper functions

def newTmpDir(env, usr):
    ts = datetime.utcnow()
    ts = ts.strftime('%Y%m%dT%H%M%S.%f')
    salt = env['RANDOM'].randint(0, 10000)
    d = env.Dir(env['TMP_DIR_PATTERN'] % (usr, ts, salt))
    os.makedirs(d.path)
    return d

env.AddMethod(newTmpDir)

def subDir(env, subd):
    env.SConscript('%s/SConscript' % subd, exports='env')

env.AddMethod(subDir)

_Glob = env.Glob
def Glob(pathname, ondisk=True, source=False, strings=False):
    fs = _Glob(pathname, ondisk, source, strings)
    fs.sort(key=lambda x:x.path)
    return fs
env.Glob = Glob

def symlink(src, dst):
    if op.islink(src):
        src = os.readlink(src)
    if op.isdir(dst):
        base = op.basename(src)
        dst = op.join(dst, base)
    os.symlink(op.abspath(src), dst)

# prepare build dir

def makeBuildDir():
    for d in [env['BUILD_DIR'], env['HEADER_DIR']]:
        d = d.abspath
        if not op.exists(d):
            os.makedirs(d)
        assert op.isdir(d)

def cleanBuildDir():
    buildDir = env['BUILD_DIR'].abspath
    for rt, dirs, files in os.walk(buildDir):
        try:
            dirs.remove('.git')
        except:
            pass
        for f in files:
            f = op.join(rt, f)
            if op.islink(f) or f.endswith('.gcno') or f.endswith('.gcda'):
                os.remove(f)

def firstDirname(p):
    x = p
    y = op.dirname(p)
    while len(y) > 0:
        x = y
        y = op.dirname(x)
    return x

def cloneFile(rt, fn):
    d = op.join(env['BUILD_DIR'].path, rt)
    if not op.exists(d):
        os.makedirs(d)
    os.symlink(op.abspath(op.join(rt, fn)), op.join(d, fn))
    
def cloneWorkSpace():
    buildDir = firstDirname(env['BUILD_DIR'].path)
    paths = os.listdir('.')
    for x in [buildDir, '.git', '.gitignore', '.sconsign.dblite', 'SConstruct']:
        try:
            paths.remove(x)
        except:
            pass
    for x in paths:
        if op.isfile(x):
            cloneFile('', x)
        if op.isdir(x):
            for rt, _, files in os.walk(x):
                for f in files:
                    cloneFile(rt, f)

makeBuildDir()
cleanBuildDir()
cloneWorkSpace()


# for clojure

def cloneInto(dstDir, srcs):
    for x in srcs:
        if x.isdir():
            dstRt = op.join(dstDir, op.basename(x.path))
            if not op.exists(dstRt):
                os.mkdir(dstRt)
            for rt, dirs, files in os.walk(x.path):
                for d in dirs:
                    d = x.rel_path(env.Dir(rt).Dir(d))
                    d = op.join(dstRt, d)
                    if not op.exists(d):
                        os.mkdir(d)
                for f in files:
                    g = x.rel_path(env.Dir(rt).File(f))
                    g = op.join(dstRt, g)
                    symlink(op.join(rt, f), g)
        else:
            symlink(x.path, op.join(dstDir, op.basename(x.path)))

def writeManifest(workdir, kws):
    if 'MANIFEST' not in kws:
        return None
    items = kws['MANIFEST']
    fn = op.join(workdir, 'manifest')
    with open(fn, 'w') as f:
        for k, v in items.items():
            f.write('%s: %s\n' % (k, v))
    return fn

def jar(env, target, source, **kwargs):
    def _jar(target, source, env):
        assert len(target) == 1
        dstJar = env.File(target[0])
        
        srcs = env.Flatten([source])
        for x in srcs:
            assert x.exists()

        workdir = env.newTmpDir('jar').path
        
        cloneInto(workdir, srcs)
        manifest = writeManifest(workdir, kwargs)

        tmpJar = op.join(workdir, op.basename(dstJar.path))
        if manifest:
            sp.check_call(['jar', 'cfm', tmpJar, manifest, '-C', workdir, '.'])
        else:
            sp.check_call(['jar', 'cf', tmpJar, '-C', workdir, '.'])
        sp.check_call(['jar', 'i', tmpJar])
        os.link(tmpJar, dstJar.path)
    env.Command(target, source, _jar)
    target = env.File(target)
    for x in source:
        if x.isdir():
            for rt, _, files in os.walk(x.abspath):
                for f in files:
                    env.Depends(target, env.File(op.join(rt, f)))
        else:
            env.Depends(target, x)
    return target

env.AddMethod(jar)

def _javac(target, source, env):
    target = target[0]
    source = source[0]
    tdir = env.newTmpDir('javac')
    srcs = []
    for rt, _, files in os.walk(source.path):
        for f in files:
            if f.endswith('.java'):
                srcs.append(op.join(rt, f))
    if '_JAVAC_CLASSPATH' in env:
        sp.check_call(['javac', '-sourcepath', source.path, '-d', tdir.path, '-cp', env['_JAVAC_CLASSPATH']] + srcs)
    else:
        sp.check_call(['javac', '-sourcepath', source.path, '-d', tdir.path] + srcs)
    for rt, _, files in os.walk(source.path):
        for f in files:
            if not f.endswith('.java'):
                from_ = op.join(rt, f)
                to = op.join(tdir.path, op.relpath(rt, source.path), f)
                symlink(from_, to)
    manifest = writeManifest(tdir.path, env)

    tmpJar = op.join(tdir.path, op.basename(target.path))
    if manifest:
        sp.check_call(['jar', 'cfm', tmpJar, manifest, '-C', tdir.path, '.'])
    else:
        sp.check_call(['jar', 'cf', tmpJar, '-C', tdir.path, '.'])
    sp.check_call(['jar', 'i', tmpJar])
    os.link(tmpJar, target.path)

def javac(env, target, source, **kwargs):
    tenv = env
    if 'LIBS' in kwargs:
        cp = ':'.join(x.path for x in env.Flatten(kwargs['LIBS']))
        tenv = env.Clone()
        tenv['_JAVAC_CLASSPATH'] = cp
    if 'MANIFEST' in kwargs:
        if tenv == env:
            tenv = env.Clone()
        tenv['MANIFEST'] = kwargs['MANIFEST']
    target = env.File(target)
    source = env.Dir(source)
    tenv.Command(target, source, _javac)
    target = env.File(target)
    for rt, _, files in os.walk(source.abspath):
        for f in files:
            env.Depends(target, env.File(op.join(rt, f)))
    

env.AddMethod(javac)

def zipper(target, source, env):
    assert len(target) == 1
    target = target[0]
    with zipfile.ZipFile(target.abspath, 'w') as zf:
        for x in source:
            zf.write(x.abspath, op.basename(x.path))

env.Append(BUILDERS={'zip': Builder(action=zipper, suffix='.zip')})

def download(env, target, source):
    target = env.File(target)
    if not op.exists(target.abspath):
        sp.check_call(['wget', '-c', '-O', target.abspath, source])
    return target

env.AddMethod(download)

def extract(env, target, source):
    tt = target
    ss = source
    def _extract(target, source, env):
        with zipfile.ZipFile(ss.abspath, 'r') as zf:
            zf.extractall(op.dirname(ss.abspath), tt)
    _target = env.Command(target, source, _extract)
    env.Depends(_target, source)
    return _target

env.AddMethod(extract)

# for C/C++

flags = {
    'CFLAGS': ['--std=c11'],
    'CXXFLAGS': ['--std=c++11'],
    'CCFLAGS': ['-Wall', '-Wfloat-equal',
                '-g', '-gdwarf-4', 
                '-I%s' % env['HEADER_DIR'].path],
    'LINKFLAGS': ['-Wl,-E']}
if mode == 'debug':
    flags['CCFLAGS'] += ['-O0', '--coverage', '-fsanitize=address', '-fvar-tracking-assignments']
    flags['LINKFLAGS'] += ['--coverage', '-fsanitize=address']
elif mode == 'release':
    flags['CCFLAGS'] += ['-O2', '-Werror', '-DNDEBUG']
env.MergeFlags(flags)

_extLibs = set([])
_libDeps = {}

def libDeps(env, lib, deps):
    if lib in _libDeps:
        _libDeps[lib] |= set(deps)
    else:
        _libDeps[lib] = set(deps)
env.AddMethod(libDeps)

def countDepends(init, deps):
    q = collections.deque(init)
    depCnts = dict((x, 0) for x in init)
    while len(q) > 0:
        x = q.popleft()
        if x in deps:
            for y in deps[x]:
                if y in depCnts:
                    depCnts[y] += 1
                else:
                    depCnts[y] = 1
                    q.append(y)
    return depCnts

def popNoDeps(depCnts, deps):
    while len(depCnts) > 0:
        for key, cnt in depCnts.items():
            if cnt == 0:
                break
        del depCnts[key]
        for x in deps.get(key, []):
            depCnts[x] -= 1
        yield key

def topologicalSort(init, deps):
    depCnts = countDepends(init, deps)
    return [x for x in popNoDeps(depCnts, deps)]


_Program = env.Program
def Program(env, target=None, source=None, **kwargs):
    if 'LIBS' in kwargs:
        global _libDeps
        libs = topologicalSort(set(kwargs['LIBS']), _libDeps)
        kwargs['LIBS'] = libs
    p = _Program(target, source, **kwargs)
    if 'LIBS' in kwargs:
        for x in kwargs['LIBS']:
            if x not in _extLibs:
                env.Depends(p, env.File('$LIB_DIR/lib%s.a' % x))
    env.Install('$BIN_DIR', p)
    return p
env.AddMethod(Program)

def Header(env, base, src):
    base = env.Dir('$HEADER_DIR').Dir(base)
    if not op.exists(base.abspath):
        os.makedirs(base.abspath)
    src = Flatten(src)
    for src in src:
        if src.isfile():
            src = src.abspath
            des = base.File(op.basename(src)).abspath
            os.symlink(src, des)
        else:
            src = src.abspath
            base = base.abspath
            for rt, dirs, files in os.walk(src):
                for d in dirs:
                    d = op.join(base, op.relpath(op.join(rt, d), src))
                    if not op.exists(d):
                        os.mkdir(d)
                for f in files:
                    s = op.join(rt, f)
                    d = op.join(base, op.relpath(s, src))
                    os.symlink(s, d)
env.AddMethod(Header)

env['BUILDERS']['Object'] = env['BUILDERS']['SharedObject']
env['BUILDERS']['StaticObject'] = env['BUILDERS']['SharedObject']

# for latex

def pathInFs(fs):
    return fs.abspath

def calcAuxDigest(tex, pdfDir):
    aux = os.path.join(pdfDir, os.path.splitext(os.path.basename(tex))[0] + '.aux')
    if os.path.exists(aux):
        digest = hashlib.md5()
        with open(aux) as f:
            digest.update(f.read())
        return digest.digest()
    else:
        return ''

def runPdfLatex(tex, pdfDir):
    sp.check_call(['pdflatex', tex], cwd=pdfDir)
    return calcAuxDigest(tex, pdfDir)

def pdflatex(target, source, env):
    assert len(target) == len(source)
    for pdf, tex in zip(map(pathInFs, target), map(pathInFs, source)):
        pdfDir = op.dirname(pdf)
        aux = calcAuxDigest(tex, pdfDir)
        while True:
            newAux = runPdfLatex(tex, pdfDir)
            if aux == newAux:
                break
            aux = newAux

env.Append(BUILDERS={'PdfLatex': Builder(action=pdflatex, suffix='.pdf')})

def luamp(env, source):
    def build(target, source, env):
        base, source = op.split(source[0].path)
        source, _ = op.splitext(source)
        with open(op.join(base, source + '.mp'), 'w') as fp:
            sp.check_call(['/usr/bin/lua5.3', source + '.lua'], stdout=fp, cwd=base)
        sp.check_call(['/usr/bin/mptopdf', '--latex', source + '.mp'], cwd=base)
        os.rename(op.join(base, source + '-0.pdf'), op.join(base, source + '.pdf'))

    source = op.basename(env.File(source).path)
    root, _ = op.splitext(op.basename(source))
    env.Command(root + '.pdf', source, build)

env.AddMethod(luamp)

# for docker

def dockerize(env, target, source, **kwargs):
    registry = kwargs['REGISTRY']
    def _dockerize(target, source, env):
        assert len(source) == 1
        source = source[0]
        assert source.isdir()

        assert len(target) == 1
        target = target[0]
        assert target.isfile() or not target.exists()

        out = sp.check_output(['sudo', 'docker', 'build', '.'], cwd=source.abspath)
        print out
        match = re.search('Successfully built (\w+)', out)
        assert match
        image = match.group(1)
        sp.check_call(['sudo', 'docker', 'tag', image, registry])
        sp.check_call(['sudo', 'docker', 'push', registry])

        with open(target.path, 'w') as fp:
            fp.write(image)
        
    env.Command(target, source, _dockerize)
    return target

env.AddMethod(dockerize)

# for golang

def gobuild(env, target, source, **kwargs):
    source = env.Dir(source)
    librt = env.Dir('#src/golibs/').abspath
    for rt, dirs, files in os.walk(librt):
        rel = op.relpath(rt, librt)
        for d in dirs:
            d = source.Dir(rel).Dir(d).abspath
            if op.exists(d):
                sh.rmtree(d)
            os.mkdir(d)
        for f in files:
            symlink(op.join(rt, f), source.Dir(rel).File(f).abspath)

    def _gobuild(target, source, env):
        assert len(source) == 1
        source = source[0]
        assert source.isdir()

        assert len(target) == 1
        target = target[0]
        assert target.isfile() or not target.exists()

        sp.check_call(['go', 'build', '-o', target.abspath], cwd = source.abspath)

    env.Command(target, source, _gobuild)
    return target

env.AddMethod(gobuild)

def gotest(env, target, source):
    def _gotest(target, source, env):
        assert len(source) == 1
        source = source[0]
        assert source.isdir()

        assert len(target) == 1
        target = target[0]
        assert target.isfile() or not target.exists()

        sp.check_call(['go', 'test', '-o', target.abspath, '-c', '-cover', '-race'],
                      cwd = source.abspath)

    env.Command(target, source, _gotest)
    source = env.Dir(source)
    for rt, _, files in os.walk(source.abspath):
        for f in files:
            env.Depends(target, env.Dir(rt).File(f))
    return target

env.AddMethod(gotest)

# for fcli

def _fcli_get_function(env, target, source, kwargs):
    cmd = [env.File('$BIN_DIR/fcli').abspath, 'function', 'get',
           '--service-name', kwargs['service'],
           '--function-name', kwargs['function']]
    with open('/dev/null') as cin:
        out = sp.check_output(cmd, stdin=cin)
    if 'does not exist in service' in out:
        return 'NOT_EXIST'
    else:
        return 'EXIST'

def fcli_upload_function(env, target, source, **kwargs):
    if 'handler' not in kwargs:
        raise Exception('handler is required in fcli_upload_function')
    if 'service' not in kwargs:
        raise Exception('service is required in fcli_upload_function')
    if 'function' not in kwargs:
        raise Exception('function is required in fcli_upload_function')
    def _fcli(target, source, env):
        assert len(source) == 1
        source = source[0]
        assert source.isdir()

        assert len(target) == 1
        target = target[0]
        assert target.isfile() or not target.exists()

        exists = _fcli_get_function(env, target, source, kwargs)
        cmd = [env.File('$BIN_DIR/fcli').abspath, 'function', 
               'update' if exists == 'EXIST' else 'create',
               '--runtime', 'python2.7',
               '--handler', kwargs['handler'],
               '--service-name', kwargs['service'],
               '--function-name', kwargs['function'],
               '--code-dir', source.abspath]
        if 'mem' in kwargs:
            cmd += ['--memory', '%d' % kwargs['mem']]
        if 'timeout' in kwargs:
            cmd += ['--timeout', '%d' % kwargs['timeout']]
        else:
            cmd += ['--timeout', '300']
        with open('/dev/null') as cin, open(target.abspath, 'wb') as fp:
            sp.check_call(cmd, stdin=cin, stdout=fp)
        with open(target.abspath) as fp:
            content = fp.read()
        if content:
            raise Exception('fail to execute fcli: ' + content)
    env.Command(target, source, _fcli)
    env.Depends(target, '$BIN_DIR/fcli')
    return target

env.AddMethod(fcli_upload_function)

# gogogo

env.SConscript('$BUILD_DIR/SConscript', exports='env')
