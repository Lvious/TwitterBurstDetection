from math import  sqrt
_WINDOW_SIZE = 15

_ALPHA = 1.0 * _WINDOW_SIZE / (24*60)

_BETA = 1.0 * _WINDOW_SIZE

_ONE_MINUTE=60

class SignificanceScorer:

    def __init__(self):
        self.ewma   = 0
        self.ewmvar = 0
        self.count  = 0
        self.current_epoch = 0
        self.next_epoch = 0        
    @staticmethod
    def set_window_size(_wz,_cycle,_average):
        global _WINDOW_SIZE
        global _ALPHA
        global _BETA

        _WINDOW_SIZE = _wz
        _ALPHA = 1.0 * _WINDOW_SIZE / _cycle
        _BETA = _average * _WINDOW_SIZE
    
    def observe(self,_t,_x):
        if self.current_epoch==0:
            self.current_epoch = _t
            self.next_epoch = self.current_epoch+_ONE_MINUTE*_WINDOW_SIZE
        if _t<self.next_epoch:
            self.count +=_x
        else:
            d = self.count-self.ewma
            self.ewma = self.ewma +d*_ALPHA
            self.ewmvar = (1-_ALPHA)*(self.ewmvar+_ALPHA*(d**2))
            step_over = int((_t-self.next_epoch)/(_ONE_MINUTE*_WINDOW_SIZE))
            for i in range(step_over):
                self.ewmvar = (1-_ALPHA)*(self.ewmvar+_ALPHA*(d**2))
                self.ewma  *= (1-_ALPHA)
            self.current_epoch +=(step_over+1)*_ONE_MINUTE*_WINDOW_SIZE
            self.next_epoch+=(step_over+1)*_ONE_MINUTE*_WINDOW_SIZE
            estimated_sig = (self.count - max(self.ewma, _BETA)) / (sqrt(self.ewmvar) + _BETA)
            self.count = _x
            return self.count, self.ewma, self.ewmvar, estimated_sig
        estimated_sig = (self.count - max(self.ewma, _BETA)) / (sqrt(self.ewmvar) + _BETA)
        return self.count, self.ewma, self.ewmvar, estimated_sig
            