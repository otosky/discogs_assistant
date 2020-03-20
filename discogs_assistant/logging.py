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

gae_logging_resource = Resource(type='gae_app',
                                labels = {
                                    'module_id': os.environ.get('GAE_SERVICE'),
                                    'project_id': os.environ.get('GOOGLE_CLOUD_PROJECT'),
                                    'version_id': os.environ.get('GAE_VERSION'),
                                    'zone': os.environ.get('GAE_ZONE', 'na')
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

    def __init__(self, resource, username):
        self.user = username
        self.default_level = 'INFO'
        super().__init__(resource)

    def _set_base_format(self):
        
        format_ = {
            'user': self.user,
            'timestamp': datetime.utcnow().isoformat()
        }
        return format_

    def log_service_request(self, service):
        
        format_ = self._set_base_format()
        format_['event_type'] = 'service_request'
        format_['service'] = service

        format_ = json.dumps(format_)
        self.logger.log_struct(json.loads(format_), resource=self.resource)

    def log_session(self, cookie, ip):
        
        format_ = self._set_base_format()
        format_['event_type'] = 'session_start'
        format_['cookie'] = cookie
        format_['ip'] = ip

        format_ = json.dumps(format_)
        self.logger.log_struct(json.loads(format_), resource=self.resource)

    def log_click_event(self, item_id):
        
        format_ = self._set_base_format()
        format_['event_type'] = 'external_link'
        # format_['click_type'] = click_type
        format_['item_id'] = item_id

        format_ = json.dumps(format_)
        self.logger.log_struct(json.loads(format_), resource=self.resource)

    def log_paginated_view(self, page_num, data, service):
        
        format_ = self._set_base_format()
        format_['event_type'] = 'pagination_click'
        format_['page_num'] = page_num
        if service == 'recommendations':
            format_['release_ids_seen'] = data
        elif service == 'carts':
            format_['cart'] = data
        
        format_ = json.dumps(format_)
        self.logger.log_struct(json.loads(format_), resource=self.resource)