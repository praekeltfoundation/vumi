# Copyright (c) 2008, Luke Kanies, luke@madstop.com
# Copyright (c) 2010, Praekelt Foundation
# 
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
# 
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

define postgres::role($ensure, $password = false, $login = true) {
    $passtext = $password ? {
        false => "",
        default => "PASSWORD '$password'"
    }
    $logintext = $login ? {
        false => "",
        true => "LOGIN"
    }
    case $ensure {
        present: {
            # The createuser command always prompts for the password.
            exec { "Create $name postgres role":
                command => "/usr/bin/psql -c \"CREATE ROLE $name $logintext $passtext\"",
                user => "postgres",
                unless => "/usr/bin/psql -c '\\du' | grep '^  *$name  *|'"
            }
        }
        absent:  {
            exec { "Remove $name postgres role":
                command => "/usr/bin/dropuser $name",
                user => "postgres",
                onlyif => "/usr/bin/psql -c '\\du' | grep '$name  *|'"
            }
        }
        default: {
            fail "Invalid 'ensure' value '$ensure' for postgres::role"
        }
    }
}
