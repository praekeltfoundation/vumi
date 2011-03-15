class vumi::apt_get_update {
    exec { "update apt-get":
        command => "apt-get update",
        user => "root",
        path => ["/usr/bin"]
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
    }
    exec { "rabbitmq_user":
        command => "sudo rabbitmqctl add_user vumi vumi
            sudo rabbitmqctl add_vhost /staging && \
            sudo rabbitmqctl set_permissions -p /staging vumi '.*' '.*' '.*'
            sudo rabbitmqctl add_vhost /production && \
            sudo rabbitmqctl set_permissions -p /production vumi '.*' '.*' '.*'
            true
            ",
        user => "root",
        require => Package["rabbitmq-server"],
        path => ["/usr/sbin","/usr/bin"],
    }
    exec { "postgres_user":
        command => "createuser --superuser vumi
            createdb -W -U vumi -h localhost -E UNICODE staging
            createdb -W -U vumi -h localhost -E UNICODE production
            true
            ",
        user => "postgres",
        require => Package["postgresql-8.4"],
        path => ["/usr/sbin","/usr/bin"],
    }
}

class vumi::python_tools {
    exec { "easy_install pip":
        command => "easy_install pip",
        path => ["/usr/bin"],  
        refreshonly => true, 
        require => Class["vumi::dependencies"], 
        subscribe => Class["vumi::dependencies"],
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
                    mkdir -p logs && \
                    mkdir -p tmp/pids
                    ",
        path => ["/usr/bin", "/usr/local/bin"],
        cwd => "/var/praekelt",
        user => "vagrant",
        require => Class["vumi::layout"],
        onlyif => "test ! -d /var/praekelt/vumi/.git"
    }
    exec { "pull repo":
        command => "pwd && git pull",
        path => ["/bin","/usr/bin", "/usr/local/bin"],
        cwd => "/var/praekelt/vumi",
        user => "vagrant",
        require => Class["vumi::layout"],
        onlyif => "test -d /var/praekelt/vumi/.git"
    }
}

class vumi::environments_file {
    file { "/var/praekelt/vumi/environments/staging.py":
        content => "from vumi.webapp.settings import *",
        ensure => file,
        require => Class["vumi::clone_repo"],
    }
}

class vumi::virtualenv {
    exec { "create_virtualenv":
        command => "virtualenv --no-site-packages ve && \
                        . ve/bin/activate && \
                        pip install -r config/requirements.pip && \
                        python setup.py develop && \
                    deactivate",
        path => ["/usr/bin","/usr/local/bin"],
        cwd => "/var/praekelt/vumi",
        user => "vagrant",
        require => Class["vumi::environments_file"],
        timeout => "-1", # disable timeout
        onlyif => "test ! -d ve"
    }
    exec { "update_virtualenv":
        command => ". ve/bin/activate && \
                        pip install -r config/requirements.pip && \
                    deactivate",
        path => ["/usr/bin","/usr/local/bin"],
        cwd => "/var/praekelt/vumi",
        user => "vagrant",
        require => Class["vumi::clone_repo"],
        timeout => "-1", # disable timeout
        onlyif => "test -d ve"
    }
}

class vumi::install_smpp_simulator {
    exec { "install_smpp":
        command => "sh install_smpp_simulator.sh",
        cwd => "/var/praekelt/vumi",
        path => ["/usr/bin", "/bin", "/usr/local/bin"],
        user => "vagrant",
        require => Class["vumi::virtualenv"],
        timeout => "-1",
        onlyif => "test ! -d /var/praekelt/vumi/smppsim"
    }
}

class vumi::startup {
    exec { "start_vumi":
        command => ". ve/bin/activate
                    supervisorctl shutdown
                    sleep 5
                    supervisord
                    deactivate
                    ",
        path => ["/usr/bin"],
        cwd => "/var/praekelt/vumi/",
        user => "vagrant",
        require => Class["vumi::install_smpp_simulator"],
    }
}

class vumi {
    include vumi::apt_get_update,
                vumi::dependencies, 
                vumi::python_tools, 
                vumi::layout, 
                vumi::clone_repo, 
                vumi::environments_file,
                vumi::virtualenv, 
                vumi::install_smpp_simulator, 
                vumi::startup
}

include vumi