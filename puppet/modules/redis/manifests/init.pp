import "defines/*.pp"

class redis {
    user { "redis":
        ensure => present,
    }
    redis_source {
        git: 
            owner => "redis",
            group => "redis",
            require => User["redis"],
    }
}
