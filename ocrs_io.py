import os
import yaml


def loadYamlCfg(iFile):

    if not os.path.isfile(iFile):
        print("No yaml: %s" % (iFile))
        return None
    try:
        with open(iFile, 'r') as stream:
            plt_cfg = yaml.load(stream)
    except IOError:
        print('plt yaml not valid: %s' % iFile)
        plt_cfg = None

    return plt_cfg