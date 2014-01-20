from setuptools import setup, find_packages


setup(
    name="vumi",
    version="0.5.0a",
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
    install_requires=[
        'zope.interface',
        'Twisted>=12.0.0',
        'txAMQP>=0.5',
        'PyYAML',
        'iso8601',
        'pyOpenSSL',
        'txssmi',
        'wokkel',
        'redis',
        'txredis',
        'python-smpp>=0.1.2',
        'pytz==2013b',
        'riakasaurus>=1.1.1',
        'riak==1.5.2',
        'txJSON-RPC==0.3.1',
        'txTwitter>=0.1.0a',
    ],
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
