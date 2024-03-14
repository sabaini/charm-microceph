#!/usr/bin/env bash

function check_osd_count() {
    local unit="${1?missing}"
    local count="${2?missing}"

    echo $USER

    osd_count=$(juju exec --unit ${unit} -- microceph disk list --json | jq '.ConfiguredDisks | length')
    if [[ $osd_count -ne $count ]] ; then
        echo "Expected OSDs $count, Actual ${osd_count}"
        exit 1
    fi

    juju status
}

function install_deps() {
    date
    sudo apt-get -qq install jq
    sudo snap install juju
    mkdir -p ~/.local/share/juju
    juju bootstrap localhost
    juju add-model microceph-test
    date
}

run="${1}"
shift

$run "$@"
