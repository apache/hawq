#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

function usage() {
  echo "USAGE: enable-ranger-plugin.sh -r ranger_host:ranger_port -u ranger_user -p ranger_password [-h hawq_host:hawq_port -c hawq_kerberos_service_name] -w hawq_user -q hawq_password -t lookup_authentication_type"
  exit 1
}

function fail() {
  echo "ERROR: $1"
  exit 1
}

function mask() {
  printf -v stars '%*s' ${#1} ''
  echo "[${stars// /*}]"
}

function read_value() {
  local input
  read -p "Enter value for $1 : " input
  echo $input
}

function read_password() {
  local input
  read -s -p "Enter value for $1 : " input
  echo $input
}

function read_authentication_type() {
  local input
  read -s -p "Enter value for $1 : " input
  echo $input
}

function get_ranger_url() {
  while [[ -z "$RANGER_URL" ]]
  do
    RANGER_URL=$(read_value "Ranger Admin host and port (e.g. abc.com:6080)")
  done
  local prefix="http://"
  RANGER_URL=${RANGER_URL#$prefix}
}

function get_ranger_user() {
  while [[ -z "$RANGER_USER" ]]
  do
    RANGER_USER=$(read_value "Ranger Admin user name")
  done
}

function get_ranger_password() {
  while [[ -z "$RANGER_PASSWORD" ]]
  do
    RANGER_PASSWORD=$(read_password "Ranger Admin password")
    echo
  done
}

# get property value from hawq-site.xml
function get_hawq_property() {
  local hawq_site_file="$HAWQ_DIR/etc/hawq-site.xml"
  local tag=$1
  local value=''
  if [ -f $hawq_site_file ] ; then
    value=`cat $hawq_site_file | tr '\n' ' ' | awk -F '<property>' '{ for(i = 1; i <= NF; i++) { print $i; } }' | grep $tag | sed -n 's|.*<value>\(.*\)</value>.*|\1|p'`
  fi
  echo $value
}

function get_hawq_url() {

  # get hawq master host and port
  # 1. read from command parameter -h
  # 2. read from hawq-site.xml
  if [[ -z "$HAWQ_URL" ]]; then
    local host=$(get_hawq_property hawq_master_address_host)
    local port=$(get_hawq_property hawq_master_address_port)

    if [[ -z "$host" || -z "$port" ]]; then
      HAWQ_URL=''
    else
      HAWQ_URL="$host:$port"
    fi
  fi

  # 3. read from user input
  local default=`hostname -f`
  default="${default}:5432"
  while [[ -z "$HAWQ_URL" ]]
  do
    HAWQ_URL=$(read_value "HAWQ Master host and port [${default}]")
  done

  local prefix="http://"
  HAWQ_URL=${HAWQ_URL#$prefix}
  local parts=(${HAWQ_URL//:/ })
  if [ ${#parts[@]} != 2 ]; then
    fail "Incorrect value for HAWQ Master host and port."
  fi
  HAWQ_HOST=${parts[0]}
  HAWQ_PORT=${parts[1]}

}

function get_hawq_user() {
  local default="gpadmin"
  while [[ -z "$HAWQ_USER" ]]
  do
    HAWQ_USER=$(read_value "HAWQ user name [${default}]")
  done
}
function get_hawq_password() {
  while [[ -z "$HAWQ_PASSWORD" ]]
  do
    HAWQ_PASSWORD=$(read_password "HAWQ password")
    echo
  done
}

function get_hawq_kerberos_service_name() {
  if [[ -z "$HAWQ_KERBEROS_SERVICE_NAME" ]]; then
    HAWQ_KERBEROS_SERVICE_NAME="postgres"
  fi
}

function get_lookup_authentication_type() {
  while [[ -z "$LOOKUP_AUTHENTICATION_TYPE" ]]
  do
    LOOKUP_AUTHENTICATION_TYPE=$(read_authentication_type "Lookup authentication type")
	echo
  done
}

function parse_params() {
  while [[ $# -gt 0 ]]
  do
    key="$1"
    case $key in
      -r)
        RANGER_URL="$2"
        shift
        ;;
      -u)
        RANGER_USER="$2"
        shift
        ;;
      -p)
        RANGER_PASSWORD="$2"
        shift
        ;;
      -h)
        HAWQ_URL="$2"
        shift
        ;;
      -w)
        HAWQ_USER="$2"
        shift
        ;;
      -q)
        HAWQ_PASSWORD="$2"
        shift
        ;;
      -c)
        HAWQ_KERBEROS_SERVICE_NAME="$2"
        shift
        ;;
      -t)
        LOOKUP_AUTHENTICATION_TYPE="$2"
        shift
        ;;
      *)
        usage
        ;;
    esac
    shift
  done
}

function validate_params() {
  get_ranger_url
  get_ranger_user
  get_ranger_password
  get_hawq_url
  get_hawq_user
  get_hawq_password
  get_hawq_kerberos_service_name
  get_lookup_authentication_type
  echo "RANGER URL  = ${RANGER_URL}"
  echo "RANGER User = ${RANGER_USER}"
  echo "RANGER Password = $(mask ${RANGER_PASSWORD})"
  echo "HAWQ HOST = ${HAWQ_HOST}"
  echo "HAWQ PORT = ${HAWQ_PORT}"
  echo "HAWQ User = ${HAWQ_USER}"
  echo "HAWQ Password = $(mask ${HAWQ_PASSWORD})"
}

function check_hawq_service_definition() {
  echo $(curl -sS -u ${RANGER_USER}:${RANGER_PASSWORD} http://${RANGER_URL}/service/public/v2/api/servicedef/name/hawq | grep hawq | wc -l)
}

function create_hawq_service_definition() {
  if [ $(check_hawq_service_definition) == 0 ]; then
    local json_file="$(dirname ${SCRIPT_DIR})/etc/ranger-servicedef-hawq.json"
    if [ ! -f ${json_file} ]; then
      fail "File ${json_file} not found."
    fi
    echo "HAWQ service definition was not found in Ranger Admin, creating it by uploading ${json_file}"
    local output=$(curl -sS -u ${RANGER_USER}:${RANGER_PASSWORD} -H "Content-Type: application/json" -X POST http://${RANGER_URL}/service/plugins/definitions -d @${json_file})
    local created=$(echo ${output} | grep created | wc -l)
    if [ ${created} == 0 ] || [ $(check_hawq_service_definition) == 0 ]; then
      fail "Creation of HAWQ service definition from ${json_file} in Ranger Admin at ${RANGER_URL} failed. ${output}"
    fi
  else
    echo "HAWQ service definition already exists in Ranger Admin, nothing to do."
  fi
}

function check_hawq_service_instance() {
  echo $(curl -sS -u ${RANGER_USER}:${RANGER_PASSWORD} http://${RANGER_URL}/service/public/v2/api/service/name/hawq | grep hawq | wc -l)
}

function create_hawq_service_instance() {
  if [ $(check_hawq_service_instance) == 0 ]; then
    local payload="{\"name\":\"hawq\",
                    \"type\":\"hawq\",
                    \"description\":\"HAWQ Master\",
                    \"isEnabled\":true,
                    \"configs\":{\"username\":\"${HAWQ_USER}\",
                               \"password\":\"${HAWQ_PASSWORD}\",
                               \"authentication\":\"${LOOKUP_AUTHENTICATION_TYPE}\",
                               \"principal\":\"${HAWQ_KERBEROS_SERVICE_NAME}\",
                               \"hostname\":\"${HAWQ_HOST}\",
                               \"port\":\"${HAWQ_PORT}\"}}"

    echo "HAWQ service instance was not found in Ranger Admin, creating it."
    local output=$(curl -sS -u ${RANGER_USER}:${RANGER_PASSWORD} -H "Content-Type: application/json" -X POST http://${RANGER_URL}/service/public/v2/api/service -d "${payload}")
    local created=$(echo ${output} | grep created | wc -l)
    if [ ${created} == 0 ] || [ $(check_hawq_service_instance) == 0 ]; then
      fail "Creation of HAWQ service instance in Ranger Admin at ${RANGER_URL} failed. ${output}"
    fi
  else
    echo "HAWQ service instance already exists in Ranger Admin, nothing to do."
  fi
}

function update_ranger_url() {
  local policy_mgr_url="http://${RANGER_URL}"
  local prop_file=$(dirname ${SCRIPT_DIR})/etc/rps.properties
  sed -i -e "s|^POLICY_MGR_URL=.*|POLICY_MGR_URL=${policy_mgr_url}|g" ${prop_file}
  echo "Updated POLICY_MGR_URL to ${policy_mgr_url} in ${prop_file}"
}

function update_java_home() {
  local jdk64="/usr/jdk64"
  local java_sdk="/etc/alternatives/java_sdk"
  local prop_file=$(dirname ${SCRIPT_DIR})/etc/rps.properties

  if [[ -d ${jdk64} ]]; then
    local DIR_NAME=$(ls ${jdk64} | sort -r | head -1)
    if [[ ${DIR_NAME} ]]; then
      JAVA_HOME_DIR="${jdk64}/${DIR_NAME}"
    fi
  elif [[ -d ${java_sdk} ]]; then
    JAVA_HOME_DIR="${java_sdk}"
  fi

  if [[ ${JAVA_HOME_DIR} ]]; then
    sed -i -e "s|/usr/java/default|${JAVA_HOME_DIR}|g" ${prop_file}
    echo "Updated default value of JAVA_HOME to ${JAVA_HOME_DIR} in ${prop_file}"
  elif [[ ! ${JAVA_HOME} ]]; then
    echo "Unable to locate JAVA_HOME on this machine. Please modify the default value of JAVA_HOME in ${prop_file}."
  fi
}

main() {
  if [[ $# -lt 1 ]]; then
    usage
  fi
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"
  HAWQ_DIR="$SCRIPT_DIR/../.."
  parse_params "$@"
  validate_params
  create_hawq_service_definition
  create_hawq_service_instance
  update_ranger_url
  update_java_home
}
main "$@"
