#!/usr/bin/expect -f
set user root
set host 172.96.228.156
set port 29612

spawn ssh -p $port $user@$host
interact
expect eof
