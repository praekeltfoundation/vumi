import "defines/*.pp"

/*
Class: redis

This class creates the redis user and required packages to build redis.

Actions:
  - Creates redis user.
  - Install GCC

Sample usage:
This class shouldn't be included directly. Use redis::server instead
*/
class redis {
    user { "redis":
        uid	=> 800,
        ensure => present;
    }
}
