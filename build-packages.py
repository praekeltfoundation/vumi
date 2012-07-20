#!/usr/bin/python

import sys, os, shutil, time

buildNumber = os.getenv('BUILD_NUMBER')
if not buildNumber:
    print "No build number"
    sys.exit(1)

def configBuild(platform, platformver, dhmake):
    if os.path.exists('./debian/rules'):
        os.remove('./debian/rules')

    rules = open('./debian/rules', 'wt')
    rules.write("#!/usr/bin/make -f\n\n%%:\n\t\tdh \"$@\" --with %s\noverride_dh_clean:\n\t\tdh_clean\n\t\trm -rf *.egg-info\n" % dhmake)
    rules.close()

    os.chmod('./debian/rules', 0755)

    dch = """vumi (0.5.0~a+%s-0~ppa%s) %s; urgency=low

  * Initial release.

 -- Colin Alston <colin.alston@praekeltfoundation.org>  %s\n""" % (
        buildNumber, 
        platformver,
        platform, 
        time.strftime('%a, %d %b %Y %H:%M:%S %z')
    )
    changelog = open('./debian/changelog', 'wt')
    changelog.write(dch)
    changelog.close()

def runBuild(platform, platformver, dhmake):
    configBuild(platform, platformver, dhmake)

    os.system('debuild -S -sn')

runBuild('lucid', 1, 'quilt,python-central')
runBuild('precise', 3, 'quilt,python2')
