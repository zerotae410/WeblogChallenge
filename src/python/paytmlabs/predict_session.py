import numpy as np
from keras.layers import Dense
from keras.models import Sequential

# data is [ip, session_count, total_session_time, longest_sesstion_time, request_count, avg_session_time, unique_url_count]
xy = np.loadtxt('/opt/resource/session_train_data.txt', delimiter='\t', dtype=str)

x_data = xy[:, 0:-2]
ipFeaturesDict = dict((f[0], [int(f[1]), int(f[2]), int(f[3]), int(f[4])]) for f in x_data)
# x_data is [session_count, total_session_time, longest_sesstion_time, request_count]
x_data = [[int(f[1]), int(f[2]), int(f[3]), int(f[4])] for f in x_data]
x_data = np.array(x_data)

# y_data is [avg_session_time]
y_data = xy[:, [-2]]
## y_data is [unique url count]
#y_data = xy[:, [-1]]

y_data = [int(f[0]) for f in y_data]
y_data = np.array(y_data).reshape(-1, 1)

dim_of_input = 4
dim_of_middle = 128
batch_size = 1024
count_of_epoch = 1000
verbose = 1

print('dim_of_input: ', dim_of_input)
print('dim_of_middle: ', dim_of_middle)
print('batch_size: ', batch_size)
print('count_of_epoch: ', count_of_epoch)
print('verbose: ', verbose)
print()

# Set model
model = Sequential()
model.add(Dense(dim_of_middle, input_dim=dim_of_input, init='uniform', activation='linear'))
model.add(Dense(dim_of_middle, init='uniform', activation='linear'))
model.add(Dense(1, init='uniform'))
model.compile(loss='mae', optimizer='rmsprop', metrics=['accuracy'])

# Train
model.fit(x_data, y_data, validation_split=0.5, batch_size=batch_size, nb_epoch=count_of_epoch, verbose=verbose)

# Evaluate
loss, accuracy = model.evaluate(x_data, y_data, batch_size=batch_size, verbose=verbose)
print()
print('###################################')
print('loss: ', loss)
print('accuracy: ', accuracy)
print()
print('###################################')
test_ip_list = ['1.186.111.224',  # in train data [2, 904913, 782745, 24]
                '210.19.202.179'  # not in train data
                ]
x_test = [ipFeaturesDict[ip] if ipFeaturesDict.has_key(ip) else [1, 0, 0, 1] for ip in test_ip_list]
print("test IPs: ",  test_ip_list)
print("test IP's values: ",  x_test)
print("predict results: ", model.predict(np.array(x_test), verbose=verbose))

model.save('predict_session_time_model.h5')
# model.save('predict_session_url_count_model.h5')
