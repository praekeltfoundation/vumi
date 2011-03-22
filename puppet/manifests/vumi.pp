# defaults for Exec
Exec {
    path => ["/bin", "/usr/bin", "/usr/local/bin"],
    user => 'vagrant',
}

# Make sure packge index is updated
class apt::update {
    exec { "Resynchronize apt package index":
        command => "apt-get update",
        user => "root",
    }
}

# Install these packages after apt-get update
define apt::package($ensure='latest') {
    package { $name: 
        ensure => $ensure, 
        require => Class['apt::update'];
    }
}

# Install these packages
class vumi::packages {
    apt::package { "build-essential": }
    apt::package { "python": }
    apt::package { "python-dev": }
    apt::package { "python-setuptools": }
    apt::package { "python-virtualenv": ensure => "1.4.5-1ubuntu1" }
    apt::package { "postgresql-8.4": ensure => "8.4.3-1" }
    apt::package { "libpq-dev": ensure => "8.4.7-0ubuntu0.10.04" }
    apt::package { "rabbitmq-server": ensure => "1.7.2-1ubuntu1" }
    apt::package { "git-core": ensure => "1:1.7.0.4-1ubuntu0.2" }
    apt::package { "openjdk-6-jre-headless": ensure => "6b20-1.9.7-0ubuntu1~10.04.1" }
    apt::package { "libcurl4-openssl-dev": ensure => "7.19.7-1ubuntu1" }
}

# Create these accounts
class vumi::accounts {
    
    rabbitmq::vhost { "/development":
        ensure => present,
        require => Class['vumi::packages']
    }
    
    rabbitmq::user { "vumi":
        ensure => present,
        password => "vumi",
        vhost => '/development',
        require => Class['vumi::packages']
    }
    
    postgres::role { "vumi":
        ensure => present,
        password => "vumi",
        require => Class["vumi::packages"],
    }
    postgres::database { "vumi":
        ensure => present,
        owner => vumi,
        template => "template0",
        require => Class["vumi::packages"],
    }
}

# We use pip for installing python software.
exec { "easy_install pip":
    command => "easy_install pip",
    refreshonly => true, 
    user => "root",
    require => Class["vumi::packages"], 
}

file {
    "/var/praekelt":
        ensure => "directory",
        owner => "vagrant";
}

exec { "Clone git repository":
    command => "git clone http://github.com/praekelt/vumi.git",
    cwd => "/var/praekelt",
    require => [
        Class['vumi::packages'],
        File['/var/praekelt']
    ],
    notify => "/var/praekelt/vumi repository ready",
    unless => "test -d /var/praekelt/vumi/.git"
}

exec { "Checkout development branch":
    command => "git checkout -b develop origin/develop",
    cwd => "/var/praekelt/vumi",
    require => [
        Class['vumi::packages'],
        File['/var/praekelt'],
    ],
    subscribe => "/var/praekelt/vumi repository ready",
    notify => "vumi development branch ready",
    unless => "git branch | grep '* develop'"
}

exec { "Update git repository":
    command => "git pull",
    cwd => "/var/praekelt/vumi",
    require => [
        Package['git-core'],
        File['/var/praekelt']
    ],
    subscribe => "/var/praekelt/vumi repository ready",
    notify => "vumi development branch ready",
    onlyif => "test -d /var/praekelt/vumi/.git"
}

exec { "Create virtualenv":
    command => "virtualenv --no-site-packages ve",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::packages"]
    ],
    subscribe => "vumi development branch ready",
    unless => "test -d ve"
}

exec { "Install requirements":
    command => ". ve/bin/activate && \
                    pip install -r config/requirements.pip && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::packages"]
    ],
    subscribe => "vumi development branch ready",
    timeout => "-1", # disable timeout
    onlyif => "test -d ve"
}

exec { "Install Vumi package":
    command => ". ve/bin/activate && \
                    python setup.py develop && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::packages"]
    ],
    subscribe => "vumi development branch ready",
    onlyif => "test -d ve"
}

exec { "Install Selenium SMPPSim":
    command => "sh install_smpp_simulator.sh",
    cwd => "/var/praekelt/vumi/utils",
    require => [
        Class["vumi::packages"]
    ],
    subscribe => "vumi development branch ready",
    timeout => "-1",
    unless => "test -d /var/praekelt/vumi/utils/smppsim"
}

class vumi::database {
    exec { "Syncdb":
        command => ". ve/bin/activate && \
                        ./manage.py syncdb --noinput && \
                    deactivate
                    ",
        cwd => "/var/praekelt/vumi",
        require => [
            Class["vumi::packages"]
        ],
        subscribe => "vumi development branch ready",
    }
    exec { "Migrate":
        command => ". ve/bin/activate && \
                        ./manage.py migrate --all && \
                    deactivate
                    ",
        cwd => "/var/praekelt/vumi",
        require => [
            Class["vumi::packages"]
        ],
        subscribe => "vumi development branch ready",
    }
}

exec { "Create Vumi Django user":
    command => ". ve/bin/activate && \
        echo \"from django.contrib.auth.models import *; u, created = User.objects.get_or_create(username='vumi', is_superuser=True, is_staff=True); u.set_password('vumi'); u.save()\" | ./manage.py shell && \
    deactivate",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::database"],
    ],
}

exec { "Restart Vumi":
    command => ". ve/bin/activate && \
                    supervisorctl reload && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::database"],
    ],
    onlyif => "ps -p `cat tmp/pids/supervisord.pid`"
}
exec { "Start Vumi":
    command => ". ve/bin/activate && \
                    supervisord && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    require => [
        Class["vumi::database"],
    ],
    unless => "ps -p `cat tmp/pids/supervisord.pid`"
}

# class vumi {
#     include apt::update,
#                 vumi::packages, 
#                 vumi::accounts,
#                 vumi::python_tools, 
#                 vumi::layout, 
#                 vumi::clone_repo, 
#                 vumi::virtualenv, 
#                 vumi::syncdb,
#                 vumi::createadmin,
#                 vumi::install_smpp_simulator, 
#                 vumi::startup
# }
# 
# include vumi