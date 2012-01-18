from setuptools import setup, find_packages


def listify(filename):
    return filter(None, open(filename, 'r').read().split('\n'))


def parse_requirements(filename):
    install_requires = []
    dependency_links = []
    for requirement in listify(filename):
        if requirement.startswith("https:") or requirement.startswith("http:"):
            (_, _, name) = requirement.partition('#egg=')
            install_requires.append(name)
            dependency_links.append(requirement)
        else:
            install_requires.append(requirement)
    return install_requires, dependency_links

install_requires, dependency_links = parse_requirements("requirements.pip")

setup(
    name="vumi",
    version="0.4.0a",
    url='http://github.com/praekelt/vumi',
    license='BSD',
    description="Super-scalable messaging engine for the delivery of SMS, "
                "Star Menu and chat messages to diverse audiences in "
                "emerging markets and beyond.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    # NOTE: See https://github.com/pypa/pip/issues/355 regarding Twisted
    # plugins and "pip uninstall"
    packages=find_packages(exclude=['environments']) + ['twisted.plugins'],
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
