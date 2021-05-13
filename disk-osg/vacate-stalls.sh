#!/bin/bash

dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

cache=$HOME/vacate-stalls.txt

limit=18.5

touch $cache

$dirname/condor-probe.py -clas12mon -vacate $limit >> $cache

if ! [ -z $1 ]; then
    tmp=$(mktemp /tmp/gemc/vacate.XXXXXX)
    # 1. remove username, keeping only site, host, and job ids
    # 2. sort and remove uniques, i.e. same host and job id
    # 3. count per host and sort by counts
    sed 's/@/ /g' $cache | awk '{print$1,$(NF-1),$NF}' | sort | uniq | awk '{print$1,$2}' | sort | uniq -c | sort -n -r > $tmp
    if [ $(cat $tmp | wc -l) -ne 0 ]; then
        echo "Vacated jobs longer than $limit hours in the past 24 hours:"
        cat $tmp
    fi
    rm -f $cache $tmp
fi

