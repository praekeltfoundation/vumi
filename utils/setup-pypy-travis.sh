# NOTE: This script needs to be sourced so it can modify the environment.

# Get out of the virtualenv we're in.
deactivate

# Install pyenv.
curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

# pyenv, depending on the versions selected, suggests having these installed
# to side step common build problems
# See: https://github.com/pyenv/pyenv/wiki/Common-build-problems
apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev

# Install pypy and make a virtualenv for it.
pyenv install --list
pyenv install -s pypy-$PYPY_VERSION
pyenv global pypy-$PYPY_VERSION
virtualenv -p $(which python) ~/env-pypy-$PYPY_VERSION
source ~/env-pypy-$PYPY_VERSION/bin/activate
