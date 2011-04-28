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
    apt::package { "build-essential": ensure => "11.4build1" }
    apt::package { "python": ensure => "2.6.5-0ubuntu1" }
    apt::package { "python-dev": ensure => "2.6.5-0ubuntu1" }
    apt::package { "python-setuptools": ensure => "0.6.10-4ubuntu1" }
    apt::package { "python-pip": ensure => "0.3.1-1ubuntu2" }
    apt::package { "python-virtualenv": ensure => "1.4.5-1ubuntu1" }
    apt::package { "postgresql-8.4": ensure => "8.4.3-1" }
    apt::package { "libpq-dev": ensure => "8.4.7-0ubuntu0.10.04" }
    apt::package { "rabbitmq-server": ensure => "1.7.2-1ubuntu1" }
    apt::package { "git-core": ensure => "1:1.7.0.4-1ubuntu0.2" }
    apt::package { "openjdk-6-jre-headless": ensure => "6b20-1.9.7-0ubuntu1~10.04.1" }
    apt::package { "libcurl4-openssl-dev": ensure => "7.19.7-1ubuntu1" }
}


# Download & install plugins for RabbitMQ & possibly others
class vumi::plugins {
    rabbitmq::plugin { "mochiweb":                  ensure => "2.3.1" }
    rabbitmq::plugin { "webmachine":                ensure => "2.3.1" }
    rabbitmq::plugin { "amqp_client":               ensure => "2.3.1" }
    rabbitmq::plugin { "rabbitmq-mochiweb":         ensure => "2.3.1" }
    rabbitmq::plugin { "rabbitmq-management-agent": ensure => "2.3.1" }
    rabbitmq::plugin { "rabbitmq-management":       ensure => "2.3.1" }
    # http://www.rabbitmq.com/releases/plugins/v2.3.1/rabbitmq-management-2.3.1.ez


# Create these accounts
class vumi::accounts {
    rabbitmq::vhost { "/develop":
        ensure => present
    }
    rabbitmq::user { "vumi":
        ensure => present,
        password => "vumi",
        vhost => '/develop'
    }
    postgres::role { "vumi":
        ensure => present,
        password => "vumi",
    }
}

class vumi::database {
    postgres::database { "vumi":
        ensure => present,
        owner => vumi,
        template => "template0",
    }
}

file {
    "/var/praekelt":
        ensure => "directory",
        owner => "vagrant";
}

exec { "Clone git repository":
    command => "git clone http://github.com/praekelt/vumi.git",
    cwd => "/var/praekelt",
    unless => "test -d /var/praekelt/vumi/.git"
}

exec { "Checkout development branch":
    command => "git checkout -b develop origin/develop",
    cwd => "/var/praekelt/vumi",
    unless => "git branch | grep '* develop'"
}

exec { "Update git repository":
    command => "git pull",
    cwd => "/var/praekelt/vumi",
    onlyif => "test -d /var/praekelt/vumi/.git"
}

exec { "Create virtualenv":
    command => "virtualenv --no-site-packages ve",
    cwd => "/var/praekelt/vumi",
    unless => "test -d ve"
}

exec { "Install requirements":
    command => ". ve/bin/activate && \
                    pip install -r config/requirements.pip && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    timeout => "-1", # disable timeout
    onlyif => "test -d ve"
}

exec { "Copy template.py to develop.py":
    command => "cp template.py develop.py",
    cwd => "/var/praekelt/vumi/environments",
    unless => "test -f develop.py"
}

exec { "Install Selenium SMPPSim":
    command => "sh install_smpp_simulator.sh",
    cwd => "/var/praekelt/vumi/utils",
    timeout => "-1",
    unless => "test -d /var/praekelt/vumi/utils/smppsim"
}

exec { "Syncdb":
    command => ". ve/bin/activate && \
                    ./manage.py syncdb --noinput && \
                deactivate
                ",
    cwd => "/var/praekelt/vumi",
}
exec { "Migrate":
    command => ". ve/bin/activate && \
                    ./manage.py migrate --all && \
                deactivate
                ",
    cwd => "/var/praekelt/vumi",
}

exec { "Create Vumi Django user":
    command => ". ve/bin/activate && \
        echo \"from django.contrib.auth.models import *; u, created = User.objects.get_or_create(username='vumi', is_superuser=True, is_staff=True); u.set_password('vumi'); u.save()\" | ./manage.py shell && \
    deactivate",
    cwd => "/var/praekelt/vumi",
}

exec { "Restart Vumi":
    command => ". ve/bin/activate && \
                    supervisorctl -c supervisord.develop.conf reload && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    onlyif => "ps -p `cat tmp/pids/supervisord.pid`"
}
exec { "Start Vumi":
    command => ". ve/bin/activate && \
                    supervisord -c supervisord.develop.conf && \
                deactivate",
    cwd => "/var/praekelt/vumi",
    unless => "ps -p `cat tmp/pids/supervisord.pid`"
}

class vumi {
    include apt::update,
                vumi::accounts,
                vumi::packages, 
                vumi::database
}

Exec["Resynchronize apt package index"] 
    -> File["/var/praekelt"] 
    -> Class["vumi::packages"] 
    -> Class["vumi::accounts"]
    -> Class["vumi::database"]
    -> Exec["Clone git repository"]
    -> Exec["Update git repository"]
    -> Exec["Checkout development branch"] 
    -> Exec["Create virtualenv"] 
    -> Exec["Install Selenium SMPPSim"]
    -> Exec["Install requirements"] 
    -> Exec["Copy template.py to develop.py"]
    -> Exec['Syncdb']
    -> Exec['Migrate']
    -> Exec["Create Vumi Django user"]
    -> Exec["Restart Vumi"]
    -> Exec["Start Vumi"]

include vumi
