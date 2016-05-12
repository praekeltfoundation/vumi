import platform
import sys
from setuptools import setup, find_packages


# cryptography>=1.0 requires pypy>=2.6.0 because of the new cffi stuff.
cryptography = 'cryptography'
if platform.python_implementation() == "PyPy":
    if sys.pypy_version_info < (2, 6):
        cryptography = 'cryptography<1.0'


setup(
    name="vumi",
    version="0.6.8",
    url='http://github.com/praekelt/vumi',
    license='BSD',
    description="Super-scalable messaging engine for the delivery of SMS, "
                "Star Menu and chat messages to diverse audiences in "
                "emerging markets and beyond.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=find_packages() + [
        'twisted.plugins',
    ],
    package_data={'twisted.plugins': ['twisted/plugins/*.py']},
    include_package_data=True,
    scripts=[
        'vumi/scripts/vumi_tagpools.py',
        'vumi/scripts/vumi_redis_tools.py',
        'vumi/scripts/vumi_model_migrator.py',
        'vumi/scripts/vumi_count_models.py',
        'vumi/scripts/vumi_list_messages.py',
    ],
    install_requires=[
        cryptography,  # See above for pypy-version-dependent requirement.
        'zope.interface',
        'Twisted>=13.2.0',
        'txAMQP>=0.6.2',
        'PyYAML',
        'iso8601',
        'pyOpenSSL',
        'certifi',
        'service_identity',
        'txssmi>=0.3.0',
        'wokkel',
        'redis>=2.10.0',
        'txredis',
        'python-smpp>=0.1.5',
        'pytz',
        'riak>=2.1',
        'txJSON-RPC==0.3.1',
        'txTwitter>=0.1.4a',
        'treq',
        'confmodel>=0.2.0',
        'hyperloglog',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
    ],
)
