Exec {
    path => ["/bin", "/usr/bin", "/usr/local/bin"],
}

class vumi::apt_get_update {
    exec { "update apt-get":
        command => "apt-get update",
        user => "root",
    }
}
class vumi::dependencies {
    package {
        "build-essential": ensure => latest, require => Class["vumi::apt_get_update"]; 
        "python": ensure => present, require => Class["vumi::apt_get_update"];  
        "python-dev": ensure => present, require => Class["vumi::apt_get_update"];  
        "python-setuptools": ensure => present, require => Class["vumi::apt_get_update"];
        "python-virtualenv": ensure => "1.4.5-1ubuntu1", require => Class["vumi::apt_get_update"];
        "postgresql-8.4": ensure => "8.4.3-1", require => Class["vumi::apt_get_update"];
        "libpq-dev": ensure => "8.4.7-0ubuntu0.10.04", require => Class["vumi::apt_get_update"];
        "haproxy": ensure => "1.3.22-1", require => Class["vumi::apt_get_update"];
        "rabbitmq-server": ensure => "1.7.2-1ubuntu1", require => Class["vumi::apt_get_update"];
        "git-core": ensure => "1:1.7.0.4-1ubuntu0.2", require => Class["vumi::apt_get_update"];
        "openjdk-6-jre-headless": ensure => "6b20-1.9.7-0ubuntu1~10.04.1", require => Class["vumi::apt_get_update"];
        "libcurl4-openssl-dev": ensure => "7.19.7-1ubuntu1", require => Class["vumi::apt_get_update"];
    }
}

class vumi::accounts {
    exec { "rabbitmq_user":
        command => "sudo rabbitmqctl add_user vumi vumi
            sudo rabbitmqctl add_vhost /staging && \
            sudo rabbitmqctl set_permissions -p /staging vumi '.*' '.*' '.*'
            sudo rabbitmqctl add_vhost /production && \
            sudo rabbitmqctl set_permissions -p /production vumi '.*' '.*' '.*'
            true
            ",
        user => "root",
        require => Class["vumi::dependencies"],
    }
    postgres::role { "vumi":
        ensure => present,
        password => "vumi",
        require => Class["vumi::dependencies"],
    }
    postgres::database { "vumi":
        ensure => present,
        owner => vumi,
        template => "template0",
        require => Class["vumi::dependencies"],
    }
}

class vumi::python_tools {
    exec { "easy_install pip":
        command => "easy_install pip",
        refreshonly => true, 
        require => Class["vumi::accounts"], 
    }
}

class vumi::layout {
    file {
        "/var/praekelt":
            ensure => "directory",
            owner => "vagrant";
        "/etc/haproxy/haproxy.cfg": 
            source => "/vagrant/haproxy.cfg", ensure => file,
            require => Class["vumi::python_tools"],
    }
}

class vumi::clone_repo {
    exec { "clone_repo":
        command => "git clone http://github.com/praekelt/vumi.git && \
                    cd vumi && \
                    git checkout -b develop origin/develop && \
                    git submodule update --init
                    ",
        cwd => "/var/praekelt",
        require => Class["vumi::layout"],
        onlyif => "test ! -d /var/praekelt/vumi/.git"
    }
    exec { "pull repo":
        command => "pwd && git pull && git submodule update --init",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::layout"],
        onlyif => "test -d /var/praekelt/vumi/.git"
    }
}

class vumi::virtualenv {
    exec { "create_virtualenv":
        command => "virtualenv --no-site-packages ve && \
                        . ve/bin/activate && \
                        pip install -r config/requirements.pip && \
                        python setup.py develop && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::clone_repo"],
        timeout => "-1", # disable timeout
        onlyif => "test ! -d ve"
    }
    exec { "update_virtualenv":
        command => ". ve/bin/activate && \
                        pip install -r config/requirements.pip && \
                        python setup.py develop && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::clone_repo"],
        timeout => "-1", # disable timeout
        onlyif => "test -d ve"
    }
}

class vumi::install_smpp_simulator {
    exec { "install_smpp":
        command => "sh install_smpp_simulator.sh",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::virtualenv"],
        timeout => "-1",
        onlyif => "test ! -d /var/praekelt/vumi/smppsim"
    }
}

class vumi::syncdb {
    exec { "syncdb":
        command => ". ve/bin/activate && \
                        ./manage.py syncdb --noinput && \
                        ./manage.py migrate --all && \
                    deactivate
                    ",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::virtualenv"],
    }
}

class vumi::createadmin {
    exec { "createadmin":
        command => ". ve/bin/activate && \
            echo \"from django.contrib.auth.models import *; u, created = User.objects.get_or_create(username='vumi', is_superuser=True, is_staff=True); u.set_password('vumi'); u.save()\" | ./manage.py shell && \
        deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::syncdb"],
    }
}

class vumi::startup {
    exec { "restart_vumi":
        command => ". ve/bin/activate && \
                        supervisorctl reread && \
                        supervisorctl reload && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::createadmin"],
        onlyif => "ps -p `cat tmp/pids/supervisord.pid`"
    }
    exec { "start_vumi":
        command => ". ve/bin/activate && \
                        supervisord && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::createadmin"],
        unless => "ps -p `cat tmp/pids/supervisord.pid`"
    }
}

class vumi {
    include vumi::apt_get_update,
                vumi::dependencies, 
                vumi::accounts,
                vumi::python_tools, 
                vumi::layout, 
                vumi::clone_repo, 
                vumi::virtualenv, 
                vumi::syncdb,
                vumi::createadmin,
                vumi::install_smpp_simulator, 
                vumi::startup
}

include vumi