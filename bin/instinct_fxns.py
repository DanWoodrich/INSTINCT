from scipy import signal
from scipy.io import wavfile
import numpy as np

##########################################
#miscellaneous useful functions, in python
##########################################

samplerate, data = wavfile.read('C:/Apps/INSTINCT/Out/decimateTest/AU-ALIC01-170421-115000.wav')

print("done 1")

up=5
down=8

data_resamp = signal.resample_poly(data, up, down)

print("done 2")

wavfile.write('C:/Apps/INSTINCT/Out/decimateTest/AU-ALIC01-170421-115000_python.wav', int(samplerate*up/down),  np.asarray(data_resamp, dtype=np.int16))

print("done 3")
