#!/bin/bash
if [ -d "ve" ]; then
    echo "Virtualenv already created"
else
    echo "Creating virtualenv"
    virtualenv --no-site-packages ve
fi

echo "Activating virtualenv"
source ve/bin/activate

export PYTHONPATH=.

function hash_requirements() {
    hasher=$(which md5 || which md5sum)
    cat requirements.pip requirements-dev.pip | $hasher
}

function install_requirements() {
    pip install --upgrade -r requirements.pip && \
        pip install --upgrade -r requirements-dev.pip && \
        hash_requirements > requirements.pip.md5
}

if [ -f 'requirements.pip.md5' ]; then
    current=$(hash_requirements)
    cached=$(cat requirements.pip.md5)
    if [ "$current" = "$cached" ]; then
        echo "Requirements still up to date"
    else
        echo "Upgrading requirements"
        install_requirements
    fi
    true
else
    echo "Installing requirements"
    install_requirements
fi

./utils/run_tests.sh || exit 1

deactivate
