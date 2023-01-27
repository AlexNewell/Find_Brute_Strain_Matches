# Configure AWS connection & connect to tables
# Create engine to connect to AWS Postgres database
def create_aws_postgres_engine(env, config_loc):
	# For AWS config details
	import configparser

	# For easier writing to Postgres
	# NOTE: not necessarily ideal for bulk writes
	from sqlalchemy import create_engine

	# need to declare credential get line here because it's not just "prod"/"dev"
	# Can simplify this once we swap those in credentials file
	if env == 'prod':
		cred_get = 'Postgres_Prod'
	elif env == 'dev':
		cred_get = 'Postgres_Dev'
	else:
		"Configure other environments. Currently configured: Prod"
		raise

	config = configparser.RawConfigParser()
	config.read(config_loc + '/.aws/config')

	cred = configparser.RawConfigParser()
	cred.read(config_loc + '/.aws/credentials')

	POSTGRES_HOST = config.get(env, 'postgres_host')
	POSTGRES_DB_NAME = config.get(env, 'postgres_db_name')
	POSTGRES_PORT = config.get(env, 'postgres_port')

	POSTGRES_USER = cred.get(cred_get, 'user')
	POSTGRES_PASSWD = cred.get(cred_get, 'passwd')

	engine = create_engine('postgresql://'
						   + POSTGRES_USER
						   + ':'
						   + POSTGRES_PASSWD
						   + '@'
						   + POSTGRES_HOST
						   + ':'
						   + POSTGRES_PORT
						   + '/'
						   + POSTGRES_DB_NAME)

	return engine
