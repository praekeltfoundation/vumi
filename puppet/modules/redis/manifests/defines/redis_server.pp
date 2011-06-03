/*

Define: redis::server

This resource compiles and install a Redis server and ensure it is running

Parameters:
- version: Redis version to install.
- path: Path where to download and compile Redis sources. (optional)
- bin: Path where to install Redis's executables. (optional)
- owner: Redis POSIX account. (default: redis)
- group: Redis POSIX group. (default: redis)
- master_ip: master's IP, to make that server a slave. (optional)
- master_port: master's port. (default 6379)
- master_password: password to access master. (optional)

Actions:
 - Downloads and compiles Redis.
 - Install binaries in $bin directory.
 - Ensure the Redis daemon is running.

Sample usage:
redis::server {
  redis:
    version	=> 'v2.0.4-stable';
}
*/
define redis::server(
  $version,
  $path = '/usr/local/src',
  $bin = '/usr/local/bin',
  $owner = 'redis',
  $group = 'redis',
  $master_ip=false,
  $master_port=6379,
  $master_password=false
) {
  include redis
  redis_source {
    redis:
      version	=> $version,
      path	=> $path,
      bin	=> $bin,
      owner	=> $owner,
      group	=> $group;
  }

  # Redis configuration
  file { 
    "/etc/redis.conf":
      ensure	=> present,
      content	=> template("redis/redis.conf.erb"),
      notify	=> Service['redis-server'];
  }

  # Logrotate
  file {
    '/etc/logrotate.d/redis':
      source	=> 'puppet:///redis/logrotate';
  }

  # Ensure Redis is running
  service {
    'redis-server':
      enable	=> true,
      ensure	=> running,
      pattern	=> '/usr/local/bin/redis-server';
  }
}
