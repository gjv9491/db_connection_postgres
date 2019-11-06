from torrential.db_connection_config import DbConnectionConfig
from torrential.db_connection_config import DbConnectionConfigException
from torrential.source_system_control import get_domain_target
from torrential.postgres_sql_wrapper import postgres_sql_wrapper as psw
from torrential.util import get_base_log
import logging

connections = 
##{
##    'ods': {
##        'region': <AWS RDS REGION>',
##        'application': 'dw',
##        'domain': <DOMAIN>',
##        'section': 'rds',
##        'type': 'pg'
##    }
##}


class PgConnectionException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return DbConnectionConfigException(self.value)


class PgConnection(object):

    def __init__(self, server, env):
        self.env = env
        self.server = server
        self.config = self.extract_connection_details()
        self.__uri = self.connection_uri()
        self.connect = self.get_connection_object()

    def extract_connection_details(self):
        base_log = get_base_log()
        try:
            connection_objects = connections[self.server]
            region = connection_objects.get('region')
            application = connection_objects.get('application')
            domain = connection_objects.get('domain')
            section = connection_objects.get('section')
            logging.info("%s server details found in connection config" % base_log)
        except:
            logging.error("%s server details missing in connection config" % base_log)
            raise PgConnectionException("%s server details missing in connection config" % base_log)
        return DbConnectionConfig(region, application, self.env, domain, section, "", get_domain_target(domain))

    def connection_uri(self):
        base_log = get_base_log()
        try:
            if not self.config.load_config():
                logging.error('%s missing_creds' % base_log)
                raise PgConnectionException('%s missing_creds' % base_log)
            __uri = self.config.uri
            logging.info('%s connection uri generated' % base_log)
        except AttributeError as detail:
            logging.error('%s missing_cred_parameters' % base_log)
            raise PgConnectionException('%s missing_cred_parameters' % base_log)
        except KeyError as detail:
            logging.error('%s missing_values' % base_log)
            raise PgConnectionException('%s missing_values' % base_log)
        return __uri

    def get_connection_object(self):
        base_log = get_base_log()
        try:
            conn_object = psw(self.__uri, connect=True)
            logging.info('%s connection object returned' % base_log)
        except:
            logging.error('%s OperationalError: Unable to connect to database.' % base_log)
            raise PgConnectionException('%s OperationalError: Unable to connect to database.' % base_log)
        return conn_object
