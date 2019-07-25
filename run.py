import plac
import helper
from logger import logger
from connection import SnowflakeConnection

def main(config_file: ("Config file", 'option', 'c')='config.yml'):

    config = helper.read_yaml(config_file)

    for profile in config["profiles"]:
        
        profile_name = profile["name"]
        tables = profile["tables"]
        logger.info(profile_name)

        snow = SnowflakeConnection(profile)
        
        with snow.engine.connect() as con:
            
            for table in tables:
                con.execute(f"select count(*) from {table}")
            

if __name__ == '__main__':

    plac.call(main)
