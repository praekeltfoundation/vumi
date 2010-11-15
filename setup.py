from setuptools import setup, find_packages

setup(
    name = "praekelt-vumi",
    version = "0.01",
    url = 'http://github.com/praekelt/vumi',
    license = 'BSD',
    description = "Super-scalable messaging engine for the delivery of SMS, " \
                  "Star Menu and chat messages to diverse audiences in " \
                  "emerging markets and beyond.",
    author = 'Praekelt Foundation',
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    install_requires = ['setuptools',],
)

