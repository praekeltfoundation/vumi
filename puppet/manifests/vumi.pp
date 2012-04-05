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
package { "build-essential": ensure => "11.4build1" }
package { "python": ensure => "2.6.5-0ubuntu1" }
package { "python-dev": ensure => "2.6.5-0ubuntu1" }
package { "python-setuptools": ensure => "0.6.10-4ubuntu1" }
package { "python-pip": ensure => "0.3.1-1ubuntu2" }
package { "python-virtualenv": ensure => "1.4.5-1ubuntu1" }
package { "rabbitmq-server": ensure => "1.7.2-1ubuntu1" }
package { "git-core": ensure => "1:1.7.0.4-1ubuntu0.2" }
package { "openjdk-6-jre-headless": ensure => "6b20-1.9.10-0ubuntu1~10.04.3" }
package { "libcurl3": ensure => "7.19.7-1ubuntu1.1" }
package { "libcurl4-openssl-dev": ensure => "7.19.7-1ubuntu1.1" }
package { "redis-server": ensure => "2:2.4.10-ubuntu1~lucid",
                          require => Class["redis_ppa"] }

# Redis PPA
class redis_ppa {
  pparepo::install {
    "rwky/redis":
        apt_key => "6BEA97CEAC3CA381594EFA2CDBB0271C5862E31D",
        dist => "lucid",
  }
}

include "redis_ppa"

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
