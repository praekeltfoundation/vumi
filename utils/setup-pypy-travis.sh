# NOTE: This script needs to be sourced so it can modify the environment.

# Get out of the virtualenv we're in.
deactivate

# Install pyenv.
curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

# Install pypy and make a virtualenv for it.
pyenv install -s pypy-$PYPY_VERSION
pyenv global pypy-$PYPY_VERSION
virtualenv -p $(which python) ~/env-pypy-$PYPY_VERSION
source ~/env-pypy-$PYPY_VERSION/bin/activate
