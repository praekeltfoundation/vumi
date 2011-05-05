from setuptools import setup, find_packages

def listify(filename):
    return filter(None, open(filename,'r').read().split('\n'))

def remove_externals(requirements):
    return filter(lambda e: e.startswith('-e'), requirements)
print ['setuptools'] + remove_externals(listify('config/requirements.pip'))

setup(
    name = "vumi",
    version = "0.1.0",
    url = 'http://github.com/praekelt/vumi',
    license = 'BSD',
    description = "Super-scalable messaging engine for the delivery of SMS, " 
                  "Star Menu and chat messages to diverse audiences in " 
                  "emerging markets and beyond.",
    long_description = open('README.rst','r').read(),
    author = 'Praekelt Foundation',
    author_email = 'dev@praekeltfoundation.org',
    packages = find_packages(),
    install_requires = ['setuptools'] + listify('config/requirements.pip'),
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ]
)

