#!/usr/bin/env python

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import base64
import getpass
import json
import os
import shutil
import socket
import urllib2
import xml.etree.ElementTree as ET
from optparse import OptionParser

PLUGIN_VERSION = '${release}'
DEFAULT_STACK = '${default.stack}'
SUPPORTED_OS_LIST = ['redhat6']
HAWQ_LIB_STAGING_DIR = '${hawq.lib.staging.dir}'
REPO_VERSION = '${repository.version}'
HAWQ_REPO = '${hawq.repo.prefix}'
HAWQ_ADD_ONS_REPO = '${hawq.addons.repo.prefix}'

REPO_INFO = {
  HAWQ_REPO: {
    'repoid': '-'.join([HAWQ_REPO, REPO_VERSION]),
    'input_param': '--hawqrepo',
    'optional': False
  },
  HAWQ_ADD_ONS_REPO: {
    'repoid': '-'.join([HAWQ_ADD_ONS_REPO, REPO_VERSION]),
    'input_param': '--addonsrepo',
    'optional': True
  }
}


class APIClient:
  """
  Class which interacts with Ambari Server API
  """

  # Base API URL points to localhost. This script is to be executed on the Ambari Server
  BASE_API_URL = 'http://localhost:8080/api/v1'

  def __init__(self, user, password):
    self.user = user
    self.password = password
    self.encoded_credentials = base64.encodestring(self.user + ':' + self.password).replace('\n', '')

  def __request(self, method, url_path, headers=None, data=None):
    """
    Creates API requests and packages response into the following format: (response code, response body in json object)
    """
    headers = headers if headers is not None else {}
    headers['Authorization'] = 'Basic {0}'.format(self.encoded_credentials)

    req = urllib2.Request(self.BASE_API_URL + url_path, data, headers)
    req.get_method = lambda: method
    response = urllib2.urlopen(req)
    response_str = response.read()

    return response.getcode(), json.loads(response_str) if response_str else None

  def verify_api_reachable(self):
    """
    Returns true if Ambari Server is reachable through API
    """
    try:
      status_code, _ = self.__request('GET', '/stacks')
    except Exception as e:
      if type(e) == urllib2.HTTPError and e.code == 403:
        raise Exception('Invalid username and/or password.')
      elif type(e) == urllib2.URLError:
        raise Exception('Ambari-server is not running. Please start ambari-server.')
      else:
        raise Exception('Unable to connect to Ambari Server.\n' + str(e))

  def get_cluster_name(self):
    """
    Returns the name of the installed cluster
    """
    _, response_json = self.__request('GET', '/clusters')
    return None if len(response_json['items']) == 0 else response_json['items'][0]['Clusters']['cluster_name']

  def get_stack_info(self, cluster_name):
    """
    Returns stack information (stack name, stack version, repository version) of stack installed on cluster
    """
    _, response_json = self.__request('GET',
                                      '/clusters/{0}/stack_versions?ClusterStackVersions/state.matches(CURRENT)'.format(
                                          cluster_name))
    if 'items' not in response_json or len(response_json['items']) == 0:
      raise Exception('No Stack found to be installed on the cluster {0}'.format(cluster_name))
    stack_versions = response_json['items'][0]['ClusterStackVersions']
    return stack_versions['stack'], stack_versions['version'], stack_versions['repository_version']

  def get_existing_repository_info(self, stack_name, stack_version, repository_version):
    """
    Returns existing repo information for a given stack
    """
    url_path = '/stacks/{0}/versions/{1}/compatible_repository_versions/{2}?fields=*,operating_systems/*,operating_systems/repositories/*'.format(
        stack_name,
        stack_version,
        repository_version)
    _, response_json = self.__request('GET', url_path)
    return response_json

  def update_existing_repo(self, stack_name, stack_version, repository_version, merged_repo_info):
    """
    Sends a PUT request to add new repo information to the Ambari database
    """
    url_path = '/stacks/{0}/versions/{1}/repository_versions/{2}'.format(stack_name, stack_version,
                                                                         repository_version)

    headers = {}
    headers['X-Requested-By'] = 'ambari'
    headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'

    try:
      status_code, _ = self.__request('PUT', url_path, headers, merged_repo_info)
    except:
      # Ambari returns sporadic errors even if PUT succeeds
      # Ignore any exception, because existing information from cluster will be verified after PUT request
      return


class RepoUtils:
  """
  Utility class for handling json structure to add new repo to existing repo
  """

  def __transform_repo(self, repository):
    """
    Extracts and returns the base_url, repo_id and repo_name for each repository
    """
    repo_info_json = repository['Repositories']
    result = {}
    result['Repositories'] = dict(
        (k, v) for k, v in repo_info_json.iteritems() if k in ('base_url', 'repo_id', 'repo_name'))
    return result

  def __transform_os_repos(self, os_repos):
    """
    Constructs the json string for each operating system
    """
    result = {
      'OperatingSystems': {},
      'repositories': []
    }
    result['OperatingSystems']['os_type'] = os_repos['OperatingSystems']['os_type']
    result['repositories'] = [self.__transform_repo(repository) for repository in os_repos['repositories']]
    return result

  def __transform(self, repository_info):
    """
    Constructs the json string with required repository information
    """
    result = {
      'operating_systems': []
    }
    result['operating_systems'] = [self.__transform_os_repos(os_repos) for os_repos in
                                   repository_info['operating_systems']]
    return result

  def __create_repo_info_dict(self, repo):
    """
    Creates json string with new repo information
    """
    result = {}
    result['Repositories'] = {
      'base_url': repo['baseurl'],
      'repo_id': repo['repoid'],
      'repo_name': repo['reponame']
    }
    return result

  def verify_repos_updated(self, existing_repo_info, repos_to_add):
    """
    Checks if input repo exists for that os_type on the cluster
    """
    existing_repos = self.__transform(existing_repo_info)

    all_repos_updated = True

    for os_repos in existing_repos['operating_systems']:
      if os_repos['OperatingSystems']['os_type'] in SUPPORTED_OS_LIST:

        for repo_to_add in repos_to_add:
          repo_exists = False

          for existing_repo in os_repos['repositories']:
            if existing_repo['Repositories']['repo_id'] == repo_to_add['repoid'] and \
                    existing_repo['Repositories']['repo_name'] == repo_to_add['reponame'] and \
                url_exists(existing_repo['Repositories']['base_url'], repo_to_add['baseurl']):
              repo_exists = True

          all_repos_updated = all_repos_updated and repo_exists

    return all_repos_updated

  def add_to_existing_repos(self, existing_repo_info, repos_to_add):
    """
    Helper function for adding new repos to existing repos
    """
    existing_repos = self.__transform(existing_repo_info)

    for os_repos in existing_repos['operating_systems']:
      if os_repos['OperatingSystems']['os_type'] in SUPPORTED_OS_LIST:
        for repo_to_add in repos_to_add:
          repo_exists = False
          for existing_repo in os_repos['repositories']:
            if existing_repo['Repositories']['repo_id'] == repo_to_add['repoid']:
              repo_exists = True
              existing_repo['Repositories']['repo_name'] = repo_to_add['reponame']
              existing_repo['Repositories']['base_url'] = repo_to_add['baseurl']

          if not repo_exists:
            os_repos['repositories'].append(self.__create_repo_info_dict(repo_to_add))

    return json.dumps(existing_repos)


class InputValidator:
  """
  Class containing methods for validating command line inputs
  """

  def __is_repourl_valid(self, repo_url):
    """
    Returns True if repo_url points to a valid repository
    """
    repo_url = os.path.join(repo_url, 'repodata/repomd.xml')
    req = urllib2.Request(repo_url)

    try:
      response = urllib2.urlopen(req)
    except urllib2.URLError:
      return False

    if response.getcode() != 200:
      return False

    return True

  def verify_stack(self, stack):
    """
    Returns stack info of stack
    """
    if not stack:
      # Use default stack
      print 'INFO: Using default stack {0}, since --stack parameter was not specified.'.format(DEFAULT_STACK)
      stack = DEFAULT_STACK

    stack_pair = stack.split('-')

    if len(stack_pair) != 2:
      raise Exception('Specified stack {0} is not of expected format STACK_NAME-STACK_VERSION'.format(stack))

    stack_name = stack_pair[0]
    stack_version = stack_pair[1]

    stack_dir = '/var/lib/ambari-server/resources/stacks/{0}/{1}'.format(stack_name, stack_version)

    if not os.path.isdir(stack_dir):
      raise Exception(
          'Specified stack {0} does not exist under /var/lib/ambari-server/resources/stacks'.format(stack))

    return {
      'stack_name': stack_name,
      'stack_version': stack_version,
      'stack_dir': stack_dir
    }

  def verify_repo(self, repoid_prefix, repo_url):
    """
    Returns repo info of repo
    """
    repo_specified = True
    if not repo_url:
      # Use default repo_url
      repo_url = 'http://{0}/{1}'.format(socket.getfqdn(), REPO_INFO[repoid_prefix]['repoid'])
      repo_specified = False

    if not self.__is_repourl_valid(repo_url):
      if repo_specified:
        raise Exception('Specified URL {0} is not a valid repository. \n'
                        'Please specify a valid url for {1}'.format(repo_url,
                                                                    REPO_INFO[repoid_prefix]['input_param']))
      elif REPO_INFO[repoid_prefix]['optional']:
        return None
      else:
        raise Exception(
            'Repository URL {0} is not valid. \nPlease ensure setup_repo.sh has been run for the {1} repository on this machine '
            'OR specify a valid url for {2}'.format(repo_url, REPO_INFO[repoid_prefix]['repoid'],
                                                    REPO_INFO[repoid_prefix]['input_param']))

    return {
      'repoid': REPO_INFO[repoid_prefix]['repoid'],
      'reponame': REPO_INFO[repoid_prefix]['repoid'],
      'baseurl': repo_url
    }


def url_exists(repoA, repoB):
  """
  Returns True if given repourl repoA exists in repoB
  """
  if type(repoB) in (list, tuple):
    return repoA.rstrip('/') in [existing_url.rstrip('/') for existing_url in repoB]
  else:
    return repoA.rstrip('/') == repoB.rstrip('/')


def update_repoinfo(stack_dir, repos_to_add):
  """
  Updates the repoinfo.xml under the specified stack_dir
  """
  file_path = '{0}/repos/repoinfo.xml'.format(stack_dir)

  for repo in repos_to_add:
    repo['xmltext'] = '<repo>\n' \
                      '  <repoid>{0}</repoid>\n' \
                      '  <reponame>{1}</reponame>\n' \
                      '  <baseurl>{2}</baseurl>\n' \
                      '</repo>\n'.format(repo['repoid'], repo['reponame'], repo['baseurl'])

  tree = ET.parse(file_path)
  root = tree.getroot()
  file_needs_update = False

  for os_tag in root.findall('.//os'):

    if os_tag.attrib['family'] in SUPPORTED_OS_LIST:
      for repo_to_add in repos_to_add:

        repo_needs_update = False
        for existing_repo in os_tag.findall('.//repo'):

          existing_repoid = [repoid.text for repoid in existing_repo.findall('.//repoid')][0]
          existing_reponame = [repoid.text for repoid in existing_repo.findall('.//reponame')][0]
          existing_baseurl = [baseurl.text for baseurl in existing_repo.findall('.//baseurl')][0]

          if existing_repoid == repo_to_add['repoid']:
            repo_needs_update = True
            print 'INFO: Repository {0} already exists with reponame {1}, baseurl {2} in {3}'.format(
                repo_to_add['repoid'], existing_reponame, existing_baseurl, file_path)

            if existing_reponame != repo_to_add['reponame'] or existing_baseurl != repo_to_add['baseurl']:
              os_tag.remove(existing_repo)
              os_tag.append(ET.fromstring(repo_to_add['xmltext']))
              print 'INFO: Repository {0} updated with reponame {1}, baseurl {2} in {3}'.format(
                  repo_to_add['repoid'], repo_to_add['reponame'], repo_to_add['baseurl'], file_path)
              file_needs_update = True

        if not repo_needs_update:
          os_tag.append(ET.fromstring(repo_to_add['xmltext']))
          print 'INFO: Repository {0} with baseurl {1} added to {2}'.format(repo_to_add['repoid'],
                                                                            repo_to_add['baseurl'], file_path)
          file_needs_update = True

  if file_needs_update:
    tree.write(file_path)


def add_repo_to_cluster(api_client, stack, repos_to_add):
  """
  Adds the new repository to the existing cluster if the specified stack has been installed on that cluster
  """
  stack_name = stack['stack_name']
  stack_version = stack['stack_version']

  cluster_name = api_client.get_cluster_name()

  # Proceed only if cluster is installed
  if cluster_name is None:
    return

  repo_utils = RepoUtils()
  installed_stack_name, installed_stack_version, installed_repository_version = api_client.get_stack_info(
      cluster_name)

  # Proceed only if installed stack matches input stack
  if stack_name != installed_stack_name or stack_version != installed_stack_version:
    return

  existing_repo_info = api_client.get_existing_repository_info(stack_name, stack_version,
                                                               installed_repository_version)

  new_repo_info = repo_utils.add_to_existing_repos(existing_repo_info, repos_to_add)
  api_client.update_existing_repo(stack_name, stack_version, installed_repository_version, new_repo_info)

  if not repo_utils.verify_repos_updated(
      api_client.get_existing_repository_info(stack_name, stack_version, installed_repository_version),
      repos_to_add):
    raise Exception(
        'Failed to update repository information on existing cluster, {0} with stack {1}-{2}'.format(cluster_name,
                                                                                                     stack_name,
                                                                                                     stack_version))

  print 'INFO: Repositories are available on existing cluster, {0} with stack {1}-{2}'.format(cluster_name,
                                                                                              stack_name,
                                                                                              stack_version)


def write_service_info(stack_dir):
  """
  Writes the service info content to the specified stack_dir
  """
  stack_services = os.path.join(stack_dir, 'services')

  for service in ('HAWQ', 'PXF'):
    source_directory = os.path.join(HAWQ_LIB_STAGING_DIR, service)
    destination_directory = os.path.join(stack_services, service)

    if not os.path.exists(source_directory):
      raise Exception('{0} directory was not found under {1}'.format(service, HAWQ_LIB_STAGING_DIR))

    service_exists = False
    if os.path.exists(destination_directory):
      service_exists = True
      shutil.rmtree(destination_directory)

    if service_exists:
      print 'INFO: Updating service {0}, which already exists under {1}'.format(service, stack_services)

    shutil.copytree(source_directory, destination_directory)

    print 'INFO: {0} directory was successfully {1}d under directory {2}'.format(service,
                                                                                 'update' if service_exists else 'create',
                                                                                 stack_services)


def build_parser():
  """
  Builds the parser required for parsing user inputs from command line
  """
  usage_string = 'Usage: ./add-hawq.py --user admin --password admin --stack HDP-2.4 --hawqrepo http://my.host.address/hawq-2.0.1.0/ --addonsrepo http://my.host.address/hawq-add-ons-2.0.1.0/'
  parser = OptionParser(usage=usage_string, version='%prog {0}'.format(PLUGIN_VERSION))
  parser.add_option('-u', '--user', dest='user', help='Ambari login username (Required)')
  parser.add_option('-p', '--password', dest='password',
                    help='Ambari login password. Providing password through command line is not recommended.\n'
                         'The script prompts for the password.')
  parser.add_option('-s', '--stack', dest='stack', help='Stack Name and Version to be added.'
                                                        '(Eg: HDP-2.4 or HDP-2.5)')
  parser.add_option('-r', '--hawqrepo', dest='hawqrepo', help='Repository URL which points to the HAWQ packages')
  parser.add_option('-a', '--addonsrepo', dest='addonsrepo',
                    help='Repository URL which points to the HAWQ Add Ons packages')
  return parser


def main():
  parser = build_parser()

  options, _ = parser.parse_args()

  user = options.user if options.user else raw_input('Enter Ambari login Username: ')
  password = options.password if options.password else getpass.getpass('Enter Ambari login Password: ')

  try:
    # Verify if Ambari credentials are correct and API is reachable
    api_client = APIClient(user, password)
    api_client.verify_api_reachable()

    validator = InputValidator()

    stack_info = validator.verify_stack(options.stack)

    repos_to_add = [validator.verify_repo(HAWQ_REPO, options.hawqrepo)]

    add_ons_repo = validator.verify_repo(HAWQ_ADD_ONS_REPO, options.addonsrepo)
    if add_ons_repo is not None:
      repos_to_add.append(add_ons_repo)

    update_repoinfo(stack_info['stack_dir'], repos_to_add)
    add_repo_to_cluster(api_client, stack_info, repos_to_add)
    write_service_info(stack_info['stack_dir'])
    print '\nINFO: Please restart ambari-server for changes to take effect'
  except Exception as e:
    print '\nERROR: {0}'.format(str(e))


if __name__ == '__main__':
  main()