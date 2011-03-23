Exec {
    path => ["/bin", "/usr/bin", "/usr/local/bin"],
    user => 'vagrant',
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
        user => "root",
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
    exec { "Clone git repository":
        command => "git clone http://github.com/praekelt/vumi.git",
        cwd => "/var/praekelt",
        require => Class["vumi::layout"],
        unless => "test -d /var/praekelt/vumi/.git"
    }
    exec { "Checkout development branch":
        command => "git checkout -b develop origin/develop",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::layout"],
        unless => "git branch | grep '* develop'"
    }
    exec { "Update git repository":
        command => "git pull",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::layout"],
        onlyif => "test -d /var/praekelt/vumi/.git"
    }
}

class vumi::virtualenv {
    exec { "Create virtualenv":
        command => "virtualenv --no-site-packages ve",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::clone_repo"],
        unless => "test -d ve"
    }
    exec { "Install requirements":
        command => ". ve/bin/activate && \
                        pip install -r config/requirements.pip && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::clone_repo"],
        timeout => "-1", # disable timeout
        onlyif => "test -d ve"
    }
    exec { "Install Vumi package":
        command => ". ve/bin/activate && \
                        python setup.py develop && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::clone_repo"],
        onlyif => "test -d ve"
    }
}

class vumi::install_smpp_simulator {
    exec { "Install Selenium SMPPSim":
        command => "sh install_smpp_simulator.sh",
        cwd => "/var/praekelt/vumi/utils",
        require => Class["vumi::virtualenv"],
        timeout => "-1",
        unless => "test -d /var/praekelt/vumi/utils/smppsim"
    }
}

class vumi::syncdb {
    exec { "Syncdb":
        command => ". ve/bin/activate && \
                        ./manage.py syncdb --noinput && \
                    deactivate
                    ",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::install_smpp_simulator"],
    }
    exec { "Migrate":
        command => ". ve/bin/activate && \
                        ./manage.py migrate --all && \
                    deactivate
                    ",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::install_smpp_simulator"],
    }
}

class vumi::createadmin {
    exec { "Create Vumi Django user":
        command => ". ve/bin/activate && \
            echo \"from django.contrib.auth.models import *; u, created = User.objects.get_or_create(username='vumi', is_superuser=True, is_staff=True); u.set_password('vumi'); u.save()\" | ./manage.py shell && \
        deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::syncdb"],
    }
}

class vumi::startup {
    exec { "Restart Vumi":
        command => ". ve/bin/activate && \
                        supervisorctl reload && \
                    deactivate",
        cwd => "/var/praekelt/vumi",
        require => Class["vumi::createadmin"],
        onlyif => "ps -p `cat tmp/pids/supervisord.pid`"
    }
    exec { "Start Vumi":
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
