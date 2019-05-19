#!/usr/bin/expect -f
set user root
set host 47.103.9.38
set port 22

spawn ssh -p $port $user@$host
interact
expect eof
