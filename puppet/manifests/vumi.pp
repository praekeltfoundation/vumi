# defaults for Exec
Exec {
    path => ["/bin", "/usr/bin", "/usr/local/bin"],
    user => 'vagrant',
}

# Make sure packge index is updated
exec { "apt-get update":
    command => "apt-get update",
    user => "root",
}

# Install these packages after apt-get update
define apt::package($ensure='latest') {
    package { $name:
        ensure => $ensure,
        subscribe => Exec['apt-get update'];
    }
}

# Install these packages
package { "build-essential": ensure => latest }
package { "python": ensure => latest }
package { "python-dev": ensure => latest }
package { "python-setuptools": ensure => latest }
package { "python-pip": ensure => latest }
package { "python-virtualenv": ensure => latest }
package { "rabbitmq-server": ensure => latest }
package { "git-core": ensure => latest }
package { "openjdk-6-jre-headless": ensure => latest }
package { "libcurl3": ensure => latest }
package { "libcurl4-openssl-dev": ensure => latest }
package { "redis-server": ensure => latest }

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
