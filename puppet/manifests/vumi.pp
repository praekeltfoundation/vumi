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
package { "postgresql-8.4": ensure => "8.4.3-1" }
package { "libpq-dev": ensure => "8.4.10-0ubuntu0.10.04.1" }
package { "rabbitmq-server": ensure => "1.7.2-1ubuntu1" }
package { "git-core": ensure => "1:1.7.0.4-1ubuntu0.2" }
package { "openjdk-6-jre-headless": ensure => "6b20-1.9.10-0ubuntu1~10.04.3" }
package { "libcurl3": ensure => "7.19.7-1ubuntu1.1" }
package { "libcurl4-openssl-dev": ensure => "7.19.7-1ubuntu1.1" }
package { "redis-server": ensure => "2:1.2.0-1" }

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
