import sys
import urllib2, base64
import json

from optparse import OptionParser
from rangerrest import RangerRestHelper


def foo_callback(option, opt, value, parser):
  setattr(parser.values, option.dest, value.split(','))

def option_parser():
    '''option parser'''
    parser = OptionParser()
    parser.remove_option('-h')
    parser.add_option('-?', '--help', action='help')
    parser.add_option('-h', '--host', dest="host", help='host of the target DB', \
                      default='localhost')
    parser.add_option('-p', '--port', dest="port", \
                      help='port of the target DB', type='int', default=6080)
    parser.add_option('-U', '--rangeruser', dest="rangerusername", default='admin', \
                      help='ranger username')
    parser.add_option('-w', '--rangerpassword', dest="rangerpassword", \
                      default='admin', help='ranger password')
    parser.add_option('-f', '--full', action="store_true", dest="fullprivilege", \
                      default=False, help='also add full privilege for user')
    parser.add_option('-u', '--user', dest="users", type='string',\
                      action='callback', callback=foo_callback, \
                      help='the ranger user list to be added')
    parser.add_option('-d', '--deteleuser', dest="deleteduserame",\
                      type='string', action='callback', \
                      callback=foo_callback, help='delete a user in ranger')
    return parser


def delete_user(uname, rangerhelper):
    rangerhelper.delete_user_without_login(uname);
    
def add_user(uname, rangerhelper):
    userSuper = json.dumps({ "name":uname, "firstName":"super", \
                            "lastName": "", "loginId": uname, \
                            "emailAddress" : None, "description" : uname\
                            , "password" : uname, "groupIdList":[2,12], \
                            "status":1, "isVisible":1, "userRoleList": \
                            ["ROLE_SYS_ADMIN"], "userSource": 1 }) 
    rangerhelper.create_user_without_login(userSuper);
    
def add_full_privilege_for_user(uname, policy_names, rangerhelper):
    
    service_name = 'hawq'
    for policy_name in policy_names:
        response, is_success = rangerhelper.get_policy(service_name, policy_name);
        response_dict = json.load(response)
        for pitem in response_dict["policyItems"]:
            pitem['users'].append(uname)
        rangerhelper.update_policy(service_name, policy_name, \
                                json.dumps(response_dict));
    return True

if __name__ == '__main__':
    parser = option_parser()

    (options, args) = parser.parse_args()


    rangeruser = options.rangerusername
    rangerpasswd= options.rangerpassword
    host = options.host
    port = str(options.port)
    add_full_privilege = options.fullprivilege

    schema_policy_name = urllib2.quote('all - database, schema, function')
    table_policy_name = urllib2.quote('all - database, schema, table')
    language_policy_name = urllib2.quote('all - database, language')
    protocol_policy_name = urllib2.quote('all - protocol')
    sequence_policy_name = urllib2.quote('all - database, schema, sequence')
    tablespae_policy_name = urllib2.quote('all - tablespace')
    policy_names = [schema_policy_name, table_policy_name, \
                    language_policy_name, protocol_policy_name, \
                    sequence_policy_name, tablespae_policy_name]
    
    helper = RangerRestHelper(host, port, rangeruser, rangerpasswd);
    #unames = ["super", "superuser"]
    unames = options.users
    deletedunames = options.deleteduserame
    if deletedunames:
        for user in deletedunames:
            delete_user(user, helper)
            print 'user {} is added'.format(user)
    elif add_full_privilege:
        for user in unames:
            add_user(user, helper)
            add_full_privilege_for_user(user, policy_names, helper)
            print 'user {} complete'.format(user)
    else:
        for user in unames:
            add_user(user, helper)
            print 'user {} is added'.format(user)
        