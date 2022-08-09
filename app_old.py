import sys
import logging
import rds_config
import pymysql
import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

rds_host  = "gerenciador-financas-database.cmwy4fkikpwu.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

client = botocore.session.get_session().create_client('secretsmanager')
logger.info("SUCCESS: Conexão com Secrets Manager realizada com sucesso")
cache_config = SecretCacheConfig()
cache = SecretCache( config = cache_config, client = client)
logger.info("SUCCESS: Cache realizado com sucesso")

secret_password = cache.get_secret_string('MySQLPassword')
logger.info(secret_password)
secret_user = cache.get_secret_string('MySQLUser')
logger.info(secret_user)
                 
logger.info("SUCCESS: Iniciando conexão com o RDS")

try:
    conn = pymysql.connect(host=rds_host, user=secret_user, passwd=secret_password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def handler(event, context):
    item_count = 0
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM TB_CATEGORIAS")
        for row in cur:
            item_count += 1
            logger.info(row)
    conn.commit()

    return "%d itens encontrados" %(item_count)