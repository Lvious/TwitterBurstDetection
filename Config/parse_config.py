import configparser
import os

configDirPath = os.path.split(os.path.realpath(__file__))[0]

config_file = os.path.join(configDirPath,'parameters.ini')

config = configparser.ConfigParser()
config.read(config_file)
class parse_config:
    @staticmethod
    def get(section,option):
        return config.get(section,option)
    @staticmethod
    def set(section,option,value):
        config.set(section,option,value)
    @staticmethod
    def write():
        config.write(open(config_file,'w'))
    @staticmethod
    def printConfig():
        for s in config.sections():
            print(s)
            for o in config[s]:
                print('\t',o,'=',config[s][o])