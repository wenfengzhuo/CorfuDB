#!/bin/bash

set -x

# Use TravisCI's build of Erlang 17.5

mkdir $HOME/otp
(
    cd $HOME/otp
    wget https://s3.amazonaws.com/travis-otp-releases/ubuntu/12.04/erlang-17.5-x86_64.tar.bz2
    tar xf $HOME/otp/erlang-17.5-x86_64.tar.bz2 -C $HOME/otp
    . $HOME/otp/17.5/activate

    # Does it work?
    which erl
    erl -s erlang halt
)

# Clone & build PropEr.

(
    cd $HOME/otp
    git clone git://github.com/manopapad/proper.git
    cd proper
    export PROPER_DIR=`pwd`
    make
)
export PROPER_DIR=$HOME/otp/proper

# We need Expect to deal with the Clojure shell's use of a pty.
sudo apt-get -y install expect

# Start server.
killall java ; sleep 1 ; killall -9 java ; sleep 1

data_dir=/tmp/some/path
rm -rf $data_dir ; mkdir -p $data_dir
log_file=$data_dir/server-log.out
touch $log_file
cat<<EOF > $data_dir/run
#!/usr/bin/expect

spawn bin/shell

send "(def q-opts (new java.util.HashMap))\n"
send "(.put q-opts \"<port>\" \"8000\")\n"
send "(new org.corfudb.cmdlets.QuickCheckMode q-opts)\n"
send "(org.corfudb.infrastructure.CorfuServer/main (into-array String (.split \"-l /tmp/some/path -s -d WARN 8000\" \" \")))\n"

set timeout 120
while {1} {
    expect -re {
        timeout { tell "\n\nTIMEOUT\n" ; exit 0 }
        eof     { tell "\n\nEOF\n" ; exit 0 }
        .       { }
    }
}

EOF
chmod +x $data_dir/run
$data_dir/run > $log_file 2>&1 &
sleep 1

count=0
set +x
while [ $count -lt 30 ]; do
    if [ `grep "Sequencer recovery requested" $log_file | wc -l` -ne 0 ]; then
        break
    fi
    sleep 1
    count=`expr $count + 1`
done

# TODO New server's endpoint name -> PropEr?

# Run PropEr tests

set -x

errors=0
cd test/src/test/erlang
./Build.sh proper
# ./Build.sh proper-shell -noshell -s map_qc cmd_prop
# errors=`expr $errors + $?`
# ./Build.sh proper-shell -noshell -s map_qc cmd_prop_parallel
# errors=`expr $errors + $?`

# Stop server
killall java ; sleep 1 ; killall -9 java ; sleep 1

# Report result (stdout, exit status)

# egrep 'ERR|WARN' $log_file | egrep -v 'Sequencer recovery requested but checkpoint not set'
cat $log_file | egrep -v 'Sequencer recovery requested but checkpoint not set'

exit $errors
