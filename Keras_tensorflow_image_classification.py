from __future__ import print_function

import numpy as np
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers.core import Dense,Activation
from keras.optimizers import SGD
from keras.utils import np_utils


####################################################################
#               Neural net param setting
####################################################################


NB_EPOCH = 200 
BATCH_SIZE = 128
VERBOSE = 1
NB_CLASSES = 10  # number of output digit
OPTIMIZER = SGD()
N_HIDDEN = 128 
VALIDATION_SPLIT = 0.2 # How much of train reserved for validation


# data:  Shuffle the data between train and test set 

(X_train,Y_train),(X_test,Y_test) = mnist.load_data()


# X_train 60000 rows of 28 * 28 values  ---> Reshaped in 60000 * 784 

RESHAPED = 784 

X_train = X_train.reshape(60000,RESHAPED)
X_test = X_test.reshape(10000,RESHAPED)

X_train = X_train.astype('float32')
X_test = X_test.astype('float32')

X_train /= 255
X_test /= 255


print(X_train.shape[0],'Train Samples')
print(X_test.shape[0],'Test samples')


# Convert the class vectors to binary class metrices

Y_train = np_utils.to_categorical(Y_train,NB_CLASSES)
Y_test = np_utils.to_categorical(Y_test,NB_CLASSES)

##################################################################
#                BUILDING MODEL
##################################################################

model = Sequential()
model.add(Dense(NB_CLASSES,input_shape=(RESHAPED,)))
model.add(Activation('softmax'))
model.summary()

# Compile the model with optimizer

model.compile(loss='categorical_crossentropy',optimizer=OPTIMIZER ,metrics=['accuracy'])


###################################################################
#           Training the model
###################################################################

history = model.fit(X_train,Y_train,batch_size=BATCH_SIZE,epochs=NB_EPOCH,verbose=VERBOSE,validation_split=VALIDATION_SPLIT)


##################################################################
#           Testing the model   
##################################################################

score = model.evaluate(X_test,Y_test,verbose=VERBOSE)

print("Test score:",score[0])
print("Test Accuracy:",score[1])

####################################################################

/Users/praveen/anaconda3/lib/python3.6/importlib/_bootstrap.py:219: RuntimeWarning: compiletime version 3.5 of module 'tensorflow.python.framework.fast_tensor_util' does not match runtime version 3.6
  return f(*args, **kwds)
60000 Train Samples
10000 Test samples
_________________________________________________________________
Layer (type)                 Output Shape              Param #
=================================================================
dense_1 (Dense)              (None, 10)                7850
_________________________________________________________________
activation_1 (Activation)    (None, 10)                0
=================================================================
Total params: 7,850
Trainable params: 7,850
Non-trainable params: 0
_________________________________________________________________
Train on 48000 samples, validate on 12000 samples
Epoch 1/200
2018-03-20 00:23:15.848917: I tensorflow/core/platform/cpu_feature_guard.cc:137] Your CPU supports instructions that this TensorFlow binary was not compiled to use: SSE4.1 SSE4.2AVX AVX2 FMA
48000/48000 [==============================] - 1s 22us/step - loss: 1.3833 - acc: 0.6698 - val_loss: 0.8918 - val_acc: 0.8273
Epoch 2/200
48000/48000 [==============================] - 1s 16us/step - loss: 0.7893 - acc: 0.8271 - val_loss: 0.6549 - val_acc: 0.8562
Epoch 3/200
48000/48000 [==============================] - 1s 14us/step - loss: 0.6407 - acc: 0.8489 - val_loss: 0.5604 - val_acc: 0.8685
Epoch 4/200
48000/48000 [==============================] - 1s 14us/step - loss: 0.5690 - acc: 0.8607 - val_loss: 0.5081 - val_acc: 0.8754
Epoch 5/200
48000/48000 [==============================] - 1s 13us/step - loss: 0.5253 - acc: 0.8674 - val_loss: 0.4741 - val_acc: 0.8809
Epoch 6/200
48000/48000 [==============================] - 1s 13us/step - loss: 0.4953 - acc: 0.8730 - val_loss: 0.4501 - val_acc: 0.8867
Epoch 7/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4731 - acc: 0.8775 - val_loss: 0.4323 - val_acc: 0.8900
Epoch 8/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4559 - acc: 0.8809 - val_loss: 0.4181 - val_acc: 0.8923
Epoch 9/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4419 - acc: 0.8840 - val_loss: 0.4067 - val_acc: 0.8952
Epoch 10/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4305 - acc: 0.8856 - val_loss: 0.3972 - val_acc: 0.8979
Epoch 11/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4207 - acc: 0.8873 - val_loss: 0.3890 - val_acc: 0.8988
Epoch 12/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4124 - acc: 0.8894 - val_loss: 0.3821 - val_acc: 0.8989
Epoch 13/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.4050 - acc: 0.8911 - val_loss: 0.3761 - val_acc: 0.8997
Epoch 14/200
48000/48000 [==============================] - 1s 14us/step - loss: 0.3986 - acc: 0.8921 - val_loss: 0.3709 - val_acc: 0.9004
Epoch 15/200
48000/48000 [==============================] - 1s 14us/step - loss: 0.3929 - acc: 0.8932 - val_loss: 0.3660 - val_acc: 0.9013
Epoch 16/200
48000/48000 [==============================] - 1s 13us/step - loss: 0.3877 - acc: 0.8942 - val_loss: 0.3618 - val_acc: 0.9022
Epoch 17/200

...........
............

Epoch 198/200
48000/48000 [==============================] - 1s 14us/step - loss: 0.2763 - acc: 0.9230 - val_loss: 0.2758 - val_acc: 0.9242
Epoch 199/200
48000/48000 [==============================] - 1s 12us/step - loss: 0.2761 - acc: 0.9231 - val_loss: 0.2758 - val_acc: 0.9243
Epoch 200/200
48000/48000 [==============================] - 1s 13us/step - loss: 0.2760 - acc: 0.9228 - val_loss: 0.2757 - val_acc: 0.9243
10000/10000 [==============================] - 0s 16us/step
Test score: 0.277021337405
Test Accuracy: 0.9234
Praveens-MacBook-Pro:~ praveen$