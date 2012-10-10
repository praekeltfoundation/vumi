# defaults for Exec
Exec {
    path => ["/bin", "/usr/bin", "/usr/local/bin", "/usr/local/sbin"],
    user => 'vagrant',
}

# Make sure package index is updated (when referenced by require)
exec { "apt-get update":
    command => "apt-get update",
    user => "root",
}

# Install these packages after apt-get update
define apt::package($ensure='latest') {
    package { $name:
        ensure => $ensure,
        require => Exec['apt-get update'];
    }
}

# Install these packages
apt::package { "build-essential": ensure => latest }
apt::package { "python": ensure => latest }
apt::package { "python-dev": ensure => latest }
apt::package { "python-setuptools": ensure => latest }
apt::package { "python-software-properties": ensure => latest }
apt::package { "python-pip": ensure => latest }
apt::package { "python-virtualenv": ensure => latest }
apt::package { "rabbitmq-server": ensure => latest }
apt::package { "git-core": ensure => latest }
apt::package { "openjdk-6-jre-headless": ensure => latest }
apt::package { "libcurl3": ensure => latest }
apt::package { "libcurl4-openssl-dev": ensure => latest }
apt::package { "redis-server": ensure => latest }
apt::package { "protobuf-compiler": ensure => latest }

file {
    "/var/praekelt":
        ensure => "directory",
        owner => "vagrant";
}

exec { "Clone git repository":
    command => "git clone http://github.com/praekelt/vumi.git",
    cwd => "/var/praekelt",
    unless => "test -d /var/praekelt/vumi/.git",
    subscribe => [
        Package['git-core'],
        File['/var/praekelt']
    ],
}
exec { "Vumi setup":
    command => "python setup.py develop",
    cwd => "/var/praekelt/vumi/",
    user => "root",
    subscribe => [
        Exec['Clone git repository']
    ],
    refreshonly => true
}

exec { "RabbitMq setup":
    command => "/var/praekelt/vumi/utils/rabbitmq.setup.sh",
    user => "root",
    subscribe => [
        Exec['Vumi setup']
    ],
    refreshonly => true,
    require => [
        Package['rabbitmq-server'],
        Exec['Clone git repository']
    ]
}
