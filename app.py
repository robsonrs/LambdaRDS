import sys
import logging
import rds_config
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

rds_host  = "endpoint-rds"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
                 
logger.info("INFO: Iniciando conexão com o RDS")
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Erro ao se conectar com o banco")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Conexão realizada com sucesso")

def handler(event, context):
    item_count = 0
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM TB_CATEGORIAS")
        for row in cur:
            item_count += 1
            logger.info(row)
    conn.commit()

    return "%d itens encontrados" %(item_count)