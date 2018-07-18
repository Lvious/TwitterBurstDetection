from parse_config import config as parse_config
import fast_signi
import stemmer
import hashlib

_SIGNI_THRESHOLD = eval(parse_config.get('detection', 'detection_threshold'))

_SIGNI_TYPE=parse_config.get('detection', 'detection_signi_type')

_BIT_COUNT = eval(parse_config.get('detection', 'bit_count'))

# hash_available=[hashlib.md5, hashlib.sha1, hashlib.sha224, hashlib.sha256, hashlib.sha384,hashlib.sha512]

hash_available = [hashlib.md5]

def get_hash(hash_function, x: str):
    """Returns a given string's hash value, obtained by the given hashlib instance."""
    hash_function.update(x.encode())
    return int.from_bytes(hash_function.digest(), byteorder="big")


class Container():
    _THRESHOLD_FOR_CLEANING =  eval(parse_config.get('detection','threshold_for_cleaning'))
    _CAPACITY_FOR_CLEANING  = eval(parse_config.get('detection', 'capacity_for_cleaning'))
    def __init__(self):
        self.container={}
    def get(self,_id,_timestamp):
        sig_scores = []
        codes = [get_hash(h(), repr(_id)) % 2 ** _BIT_COUNT for h in hash_available]
        record = []
        for code in codes:
            if code in self.container:
                sig_scores.append(self.container[code])
                record.append(code)
            else:
                if _SIGNI_TYPE=='s':
                    sig_score = fast_signi.SignificanceScorer()
                    self.container[code] = sig_score
                    sig_scores.append(self.container[code])
                    record.append(0)
        return sig_scores,record
class SigniProcessor:
    def __init__(self):
        self.sig_scorers = Container()
        self.timestamp = None
    def process(self,_ptweet):
        self.timestamp = _ptweet.timestamp
        _tokens = _ptweet.tokens
        tokens = [stemmer.stem(x) for x in _tokens]
        if len(tokens)<3:
            return None,None
        unique_words = set(tokens)
        unique_word_pairs = set()

        for i in unique_words:
            for j in unique_words - {i}:
                # To us [a, b] = [b, a], and sorting gives us a distinct representation.
                unique_word_pairs.add(tuple(sorted([i, j])))
                
        max_sig = 0
        max_sig_instance = None
        sig_list=list()
        
        for token in unique_word_pairs:
            if _SIGNI_TYPE=='s':
                min_instance = []
                scores,codes = self.sig_scorers.get(token, self.timestamp)
                for x in scores:
                    min_instance.append(x.observe(int(self.timestamp), 1.0))
                count, ewma, ewmavar, sig = min(min_instance,key=lambda x:x[1])
#                 count, ewma, ewmavar, sig = min([x.observe(int(self.timestamp), 1.0) for x in self.sig_scorers.get(token, self.timestamp)],key=lambda x:x[1])
                if sig > max_sig and ewma>0:
                    max_sig = sig
                    max_sig_instance = _ptweet.datetime(), count, ewma, ewmavar, sig, token
                if sig > _SIGNI_THRESHOLD and ewma>0:
                    sig_list.append((_ptweet.datetime(), count, ewma, ewmavar, sig, token))
        if max_sig>_SIGNI_THRESHOLD:
#             print(max_sig_instance)
            return max_sig_instance,sig_list
        return None,None