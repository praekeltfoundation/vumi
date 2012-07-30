#!/usr/bin/python

import os
import sys
import time


buildNumber = os.getenv('BUILD_NUMBER')
if not buildNumber:
    print "No build number"
    sys.exit(1)

sourceVersion = "0.5.0~a+%s" % (buildNumber,)
baseDir = os.getcwd()
tarball = 'vumi_%s.orig.tar.gz' % (sourceVersion,)


def configBuild(platform, platformver, dhmake):
    if os.path.exists('./debian/rules'):
        os.remove('./debian/rules')

    rules = open('./debian/rules', 'wt')
    rules.write("#!/usr/bin/make -f\n\n%%:\n"
                "\t\tdh \"$@\" --with %s\noverride_dh_clean:\n"
                "\t\tdh_clean\n\t\trm -rf *.egg-info\n" % dhmake)
    rules.close()

    os.chmod('./debian/rules', 0755)

    dch = """vumi (%s-0~ppa%s) %s; urgency=low

  * Initial release.

 -- Colin Alston <colin.alston@praekeltfoundation.org>  %s\n""" % (
        sourceVersion,
        platformver,
        platform,
        time.strftime('%a, %d %b %Y %H:%M:%S %z')
    )
    changelog = open('./debian/changelog', 'wt')
    changelog.write(dch)
    changelog.close()


def prepareBuild():
    os.chdir(baseDir)
    os.system('mkdir -p dist/vumi')
    os.system('git archive HEAD -o dist/%s' % (tarball,))


def runBuild(platform, platformver, dhmake):
    os.chdir(os.path.join(baseDir, 'dist/vumi'))
    os.system('tar xzf ../%s' % (tarball,))
    configBuild(platform, platformver, dhmake)
    os.system('debuild -S')


prepareBuild()
runBuild('lucid', 1, 'quilt,python-central')
runBuild('precise', 3, 'quilt,python2')
