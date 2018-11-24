from nltk.stem.lancaster import *
from nltk.stem.snowball import *
from nltk.stem.porter import *
from nltk import pos_tag
from Config.parse_config import config

stemmer = None
if config.get('pre_process', 'stemmer') == 'Snowball':
    stemmer = SnowballStemmer("english")

if config.get('pre_process', 'stemmer') == 'Porter':
    stemmer = PorterStemmer()

if config.get('pre_process', 'stemmer') == 'Lancaster':
    stemmer = LancasterStemmer()


def stem(word):
    if stemmer is None:
        print('none')
        return word

    try:
        ret = stemmer.stem(word)
        pos_tag()
        ret = str(ret)
    except:
        ret = word

    return ret