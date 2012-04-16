from setuptools import setup, find_packages
import re


def listify(filename):
    return filter(None, open(filename, 'r').read().split('\n'))


_SIMPLE_VERSION_RE = re.compile("(?P<name>.*)-(?P<version>[0-9.]+)$")


def parse_requirements(filename):
    install_requires = []
    dependency_links = []
    for requirement in listify(filename):
        if requirement.startswith("https:") or requirement.startswith("http:"):
            (_, _, name) = requirement.partition('#egg=')
            ver_match = _SIMPLE_VERSION_RE.match(name)
            if ver_match:
                # egg names with versions need to be converted to
                # an == requirement.
                name = "%(name)s==%(version)s" % ver_match.groupdict()
            install_requires.append(name)
            dependency_links.append(requirement)
        else:
            install_requires.append(requirement)
    return install_requires, dependency_links

install_requires, dependency_links = parse_requirements("requirements.pip")

setup(
    name="vumi",
    version="0.4.0",
    url='http://github.com/praekelt/vumi',
    license='BSD',
    description="Super-scalable messaging engine for the delivery of SMS, "
                "Star Menu and chat messages to diverse audiences in "
                "emerging markets and beyond.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=find_packages() + [
        # NOTE:2012-01-18: This is commented out for now, pending a fix for
        # https://github.com/pypa/pip/issues/355
        #'twisted.plugins',
    ],
    package_data={'twisted.plugins': ['twisted/plugins/*.py']},
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
    ],
)
