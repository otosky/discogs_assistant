import google.cloud.logging
from google.cloud.logging.resource import Resource
import logging
import os, json
from datetime import datetime

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_NAME')
GCP_INSTANCE_ID = os.environ.get('GCP_INSTANCE_ID')
GCP_INSTANCE_ZONE = os.environ.get('GCP_INSTANCE_ZONE')
GCP_LOG_RESOURCE_TYPE = os.environ.get('GCP_LOG_RESOURCE_TYPE')

logging_resource = Resource(type=GCP_LOG_RESOURCE_TYPE,
                            labels={
                                'project_id': GCP_PROJECT_ID,
                                'instance_id': GCP_INSTANCE_ID,
                                'zone': GCP_INSTANCE_ZONE
                            })

class GeneralLogger():

    def __init__(self, resource):
        client = google.cloud.logging.Client()
        self.client = client
        self.client.setup_logging()
        self.logger = client.logger('DiscogsAssistant')
        self.resource = resource
        
    def log_text(self, message, severity='INFO'):
        self.logger.log_text(message, resource=self.resource, severity=severity)
        

class LoggerBackend(GeneralLogger):

    def __init__(self, resource, username, trans_id, pipeline, script):
        self.user = username
        self.trans_id = trans_id
        self.pipeline_name = pipeline
        self.script = script
        self.user_details = {'name': self.user, 'trans_id': self.trans_id}
        self.pipeline_details = {'process': self.pipeline_name, 'script': self.script}
        self.default_level = 'INFO'
        super().__init__(resource)

    def log_to_stackdriver(self, function, stage, level=None, **kwargs):
        '''
        Logs start or end (stage) of function to Stackdriver.
        '''
        
        if not level:
            level = self.default_level

        format_ = {'user': self.user_details,
                    'pipeline': self.pipeline_details,
                    'function': {'name': function,
                                 'stage': stage
                                },
                    'level': level,
                    'timestamp': datetime.utcnow().isoformat()
        }

        ## extra kwargs might be:
        ## - criteria
        ## - criteria_id
        ## - num_items_to_scrape
        ## - interaction_type
        if kwargs:
            format_['extra'] = {**kwargs}
        format_ = json.dumps(format_)
        self.logger.log_struct(json.loads(format_), resource=self.resource)
        return None

class LoggerFrontEnd(GeneralLogger):

    def __init__(self, username):
        self.user = username
        super().__init__()
