import numpy as np
from keras.layers import Dense
from keras.models import Sequential

# sort -reverse data
revers_data = []
with open('/opt/resource/load_train_data.txt', 'r') as lines:
    for line in lines:
        revers_data.insert(0, line)

max_input_size = 11
min_input_size = 5 # min concatenate minutes length
xy = []
input_bucket = []
old_time = 0
for line in revers_data:
    splits = line.split('\t')
    time = long(splits[0])
    requests = int(splits[1].rstrip())
    if not old_time:
        old_time = time
        input_bucket.append(requests)
        continue

    # check time is last minute
    if (time + 1) == old_time:
        old_time = time
        input_bucket.append(requests)
    else:
        # new train data
        intput_length = len(input_bucket)
        if intput_length < min_input_size:
            # discard less than five concatenate minutes data
            old_time = 0
            input_bucket = []
            continue
        # padding
        for i in range(max_input_size - intput_length):
            input_bucket.append(0)
        xy.append(input_bucket)
        old_time = time
        input_bucket = [requests]
# last train data
if len(input_bucket) >= min_input_size:
    xy.append(input_bucket)
    # padding
    for i in range(max_input_size - len(input_bucket)):
        input_bucket.append(0)

xy = np.array(xy)
print xy


# x_data is request counts for last four concatenate minutes
x_data = xy[:, 1:min_input_size]
x_data = np.array(x_data)

# y_data is [next minute request count]
y_data = xy[:, [0]]
y_data = np.array(y_data)

dim_of_input = min_input_size-1
dim_of_middle = 128
batch_size = 15
count_of_epoch = 10000
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
model.fit(x_data, y_data, validation_split=0.3, batch_size=batch_size, nb_epoch=count_of_epoch, verbose=verbose)

# Evaluate
loss, accuracy = model.evaluate(x_data, y_data, batch_size=batch_size, verbose=verbose)
print()
print('###################################')
print('loss: ', loss)
print('accuracy: ', accuracy)
print()
print('###################################')

print("next minute's load will be ", model.predict(np.array([[2465, 5061, 5163, 3300]]), verbose=verbose))

model.save('predict_load_model.h5')
