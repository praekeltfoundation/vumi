# From https://gist.github.com/844735

# Setup a PPA repo, where the name is "user/ppaname",
# e.g. "blueyed/ppa" ("ppa" being the default)

define pparepo::install($apt_key = "", $dist = $ppa_default_name, $supported = ["lucid", "hardy"], $ensure = present, $keyserver = "keyserver.ubuntu.com") {
  $name_for_file = regsubst($name, '/', '-', 'G')
  $file = "/etc/apt/sources.list.d/pparepo-${name_for_file}.list"
  file { "$file": }

  case $ensure {
    present: {
      # if ($dist) and ($dist in $supported) {
      if ($dist) {
        File["$file"] {
          content => "deb http://ppa.launchpad.net/$name/ubuntu $dist main\n"
        }
        File["$file"] { ensure => file }
        if ( $apt_key ) {
          apt::key { "$apt_key": }
        }
      } else {
        File["$file"] { ensure => false }
      }
    }
    absent:  {
      File["$file"] { ensure => false }
    }
    default: {
      fail "Invalid 'ensure' value '$ensure' for pparepo"
    }
  }
}

# source http://projects.puppetlabs.com/projects/1/wiki/Apt_Keys_Patterns

define apt::key($ensure = present, $keyserver = "keyserver.ubuntu.com") {
  $grep_for_key = "apt-key list | grep '^pub' | sed -r 's.^pub\\s+\\w+/..' | grep '^$name'"
  case $ensure {
    present: {
      exec { "Import $name to apt keystore":
        path        => "/bin:/usr/bin",
        environment => "HOME=/root",
        command     => "gpg --keyserver $keyserver --recv-keys $name && gpg --export --armor $name | apt-key add -",
        user        => "root",
        group       => "root",
        unless      => "$grep_for_key",
        logoutput   => on_failure,
      }
    }
    absent:  {
      exec { "Remove $name from apt keystore":
        path    => "/bin:/usr/bin",
        environment => "HOME=/root",
        command => "apt-key del $name",
        user    => "root",
        group   => "root",
        onlyif  => "$grep_for_key",
      }
    }
    default: {
      fail "Invalid 'ensure' value '$ensure' for apt::key"
    }
  }
}