
#!/usr/bin/env python3
import os

SNOWSQL_PASSPHRASE = ''
AWS_KEY_ID = ''
AWS_SECRET_KEY = ''

# Email config
SENDER = 'datateam@zds-tech-prod-api-01.bo3.e-dialog.com'
CPAUSER_EMAIL = ["cpateam@aptroid.com"]
TECH_NOTIFICATION_RECIPIENTS = ["jmolugu@zetaglobal.com"]

DB_HOST = 'zds-prod-pgdb02-01.bo3.e-dialog.com'
S3_BASE = 's3://temporary-data/DATATEAM_DND_FILES/CPA_SUPP_MALNG_REQ'


DB_CONFIG = {
    'orange': {'host': 'zds-prod-pgdb02-01.bo3.e-dialog.com', 'db': 'orange_db', 'user': 'datateam'},
    'arcamax': {'host': 'zds-prod-pgdb02-01.bo3.e-dialog.com', 'db': 'arcamax_db', 'user': 'datateam'},
    'mt2': {
        'host': 'cmprep-prod-ro-02-vip.bo3.e-dialog.com',
        'db': 'mt2_data',
        'user': 'cmp_cust_user',
        'password': os.getenv('CPA_MT2_PASSWORD', ''),
    }
}
