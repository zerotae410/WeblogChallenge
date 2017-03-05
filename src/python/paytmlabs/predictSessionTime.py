'''
This is for predicting the session time for given ip.
Train data is, for X, 4 Dimension vertor ip address Y is the average time.
'''
from keras.models import Sequential
from keras.layers import Dense, Dropout

import numpy as np

# load data
data = np.loadtxt("/opt/resource/session_avg_data.txt", delimiter="\t", dtype=long)
# X and Y
# [ip1, ip2, ip3, ip4]
X = data[:, :-1]
# [avg_seesion_time]
Y = data[:, -1]

# Set constants
dimof_input = 4
batch_size = 512
dimof_middle = 128
dropout = 0.2
countof_epoch = 10
verbose = 1

print('batch_size: ', batch_size)
print('dimof_middle: ', dimof_middle)
print('dropout: ', dropout)
print('countof_epoch: ', countof_epoch)
print('verbose: ', verbose)
print()

# Set model
model = Sequential()
model.add(Dense(dimof_middle, input_dim=dimof_input, init='uniform', activation='relu'))
# model.add(Dropout(dropout))
model.add(Dense(dimof_middle, init='uniform', activation='relu'))
# model.add(Dropout(dropout))
model.add(Dense(1, init='uniform', activation='relu'))
model.compile(loss='mse', optimizer='sgd', metrics=['accuracy'])

# Train
model.fit(X, Y, validation_split=0.3, batch_size=batch_size, nb_epoch=countof_epoch, verbose=verbose)

# Evaluate
loss, accuracy = model.evaluate(X, Y, verbose=verbose)
print()
print('loss: ', loss)
print('accuracy: ', accuracy)

predict_time = model.predict(np.array([[1, 186, 101, 79] ,[210, 19, 202, 179]]), verbose=verbose)
print ('predicted for 1.186.101.79 and 210, 19, 202, 179:', predict_time)
