from django.conf import settings

RETRY_LIMIT = 5
POOL_SIZE = 8
PACT_DOMAIN = settings.PACT_DOMAIN
PACT_HP_GROUPNAME = settings.PACT_HP_GROUPNAME
PACT_HP_GROUP_ID = settings.PACT_HP_GROUP_ID
PACT_CASE_TYPE = 'cc_path_client' # WRONG is cc_path_type

PACT_SCHEDULES_NAMESPACE = 'pact_weekly_schedule'

PACT_URL = settings.PACT_URL

