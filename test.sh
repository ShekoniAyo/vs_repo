#!/bin/bash

server_name=$(hostname)
echo $server_name

function memory_check() {
    echo " "
        echo "The current memory usage of ${server_name} is: "
        free -h
        echo " "
}

function cpu_check() {
    echo " "
        echo "CPU load on ${server_name} is: "
        uptime
        echo " "
}

function tcp_check() {
    echo " "
        echo "TCP connction on ${server_name} is: "
        wc -l < /proc/net/tcp
        echo " "
}

function kernel_check() {
    echo " "
        echo "Kernel version on ${server_name} is: "
        uname -r
        echo " "
}

function all_checks() {
    memory_check
    cpu_check
    tcp_check
    kernel_check
}

all_checks