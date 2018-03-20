from __future__ import print_function 
import numpy as np 
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers.core import Dense,Activation
from keras.optimizers import SGD
from keras.utils import np_utils

#################################################
#   Network training parameters
##################################################

NB_EPOCH = 20 
BATCH_SIZE =128 
VERBOSE = 1 

NB_CLASS = 10 # Number of output classes

OPTIMIZER = SGD()
N_HIDDEN = 128 
VALIDATION_SPLIT = 0.2 # HOW MUCH TRAIN IS RESERVED FOR THE VALIDATION 

RESHAPED = 784 
#################################################
# DATA PREPERATION 
#################################################

(X_train,Y_train),(X_test,Y_test) = mnist.load_data()

# X_train is 60000 rows of 28 * 28 values --> Reshaped in 60000 * 784 


X_train = X_train.reshape(60000,RESHAPED)
X_test = X_test.reshape(10000,RESHAPED)


X_train = X_train.astype('float32')
X_test = X_test.astype('float32')

# Normalize 

X_train /= 255
X_test /= 255 

print(X_train.shape[0],'No of train samples')
print(X_test.shape[0],'No of Test samples')

Y_train = np_utils.to_categorical(Y_train,NB_CLASS)
Y_test = np_utils.to_categorical(Y_test,NB_CLASS)


###############################################
# Bulding model
###############################################

model = Sequential()
model.add(Dense(N_HIDDEN,input_shape=(RESHAPED,)))
model.add(Activation('relu'))
model.add(Dense(N_HIDDEN))
model.add(Activation('relu'))
model.add(Dense(NB_CLASS))
model.add(Activation('softmax'))

model.summary()

# Compiling the model 

model.compile(loss ='categorical_crossentropy',optimizer= OPTIMIZER , metrics=['accuracy'])


################################################
#   Training the model 
###############################################

history = model.fit(X_train,Y_train,batch_size=BATCH_SIZE,epochs=NB_EPOCH,verbose=VERBOSE,validation_split=VALIDATION_SPLIT)


###############################################
#   Model validation / Testing
##############################################

score = model.evaluate(X_test,Y_test,verbose=VERBOSE)

print('Test score:',score[0])
print('Test accuracy',score[1])

#############################################


Praveens-MacBook-Pro:~ praveen$ python "/Users/praveen/Desktop/Desktop_items/MachineLearning/Tensorflow/Keras/Image classification - Improving accuracy by adding the hidden layers.py"
Using TensorFlow backend.
/Users/praveen/anaconda3/lib/python3.6/importlib/_bootstrap.py:219: RuntimeWarning: compiletime version 3.5 of module 'tensorflow.python.framework.fast_tensor_util' does not match runtime version 3.6
  return f(*args, **kwds)
60000 No of train samples
10000 No of Test samples
_________________________________________________________________
Layer (type)                 Output Shape              Param #
=================================================================
dense_1 (Dense)              (None, 128)               100480
_________________________________________________________________
activation_1 (Activation)    (None, 128)               0
_________________________________________________________________
dense_2 (Dense)              (None, 128)               16512
_________________________________________________________________
activation_2 (Activation)    (None, 128)               0
_________________________________________________________________
dense_3 (Dense)              (None, 10)                1290
_________________________________________________________________
activation_3 (Activation)    (None, 10)                0
=================================================================
Total params: 118,282
Trainable params: 118,282
Non-trainable params: 0
_________________________________________________________________
Train on 48000 samples, validate on 12000 samples
Epoch 1/20
2018-03-21 00:23:39.407862: I tensorflow/core/platform/cpu_feature_guard.cc:137] Your CPU supports instructions that this TensorFlow binary was not compiled to use: SSE4.1 SSE4.2AVX AVX2 FMA
48000/48000 [==============================] - 2s 35us/step - loss: 1.3921 - acc: 0.6641 - val_loss: 0.6788 - val_acc: 0.8612
Epoch 2/20
48000/48000 [==============================] - 1s 28us/step - loss: 0.5567 - acc: 0.8627 - val_loss: 0.4322 - val_acc: 0.8873
Epoch 3/20
48000/48000 [==============================] - 1s 28us/step - loss: 0.4242 - acc: 0.8840 - val_loss: 0.3664 - val_acc: 0.8998
Epoch 4/20
48000/48000 [==============================] - 1s 27us/step - loss: 0.3735 - acc: 0.8951 - val_loss: 0.3353 - val_acc: 0.9057
Epoch 5/20
48000/48000 [==============================] - 1s 30us/step - loss: 0.3438 - acc: 0.9025 - val_loss: 0.3126 - val_acc: 0.9112
Epoch 6/20
48000/48000 [==============================] - 1s 27us/step - loss: 0.3225 - acc: 0.9087 - val_loss: 0.2976 - val_acc: 0.9168
Epoch 7/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.3060 - acc: 0.9135 - val_loss: 0.2839 - val_acc: 0.9208
Epoch 8/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2922 - acc: 0.9170 - val_loss: 0.2739 - val_acc: 0.9232
Epoch 9/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2802 - acc: 0.9209 - val_loss: 0.2634 - val_acc: 0.9267
Epoch 10/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2692 - acc: 0.9235 - val_loss: 0.2552 - val_acc: 0.9275
Epoch 11/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2596 - acc: 0.9262 - val_loss: 0.2467 - val_acc: 0.9317
Epoch 12/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2507 - acc: 0.9290 - val_loss: 0.2402 - val_acc: 0.9329
Epoch 13/20
48000/48000 [==============================] - 1s 26us/step - loss: 0.2421 - acc: 0.9314 - val_loss: 0.2330 - val_acc: 0.9337
Epoch 14/20
48000/48000 [==============================] - 1s 26us/step - loss: 0.2347 - acc: 0.9332 - val_loss: 0.2285 - val_acc: 0.9348
Epoch 15/20
48000/48000 [==============================] - 1s 28us/step - loss: 0.2274 - acc: 0.9357 - val_loss: 0.2213 - val_acc: 0.9379
Epoch 16/20
48000/48000 [==============================] - 1s 26us/step - loss: 0.2205 - acc: 0.9372 - val_loss: 0.2169 - val_acc: 0.9397
Epoch 17/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2143 - acc: 0.9389 - val_loss: 0.2114 - val_acc: 0.9404
Epoch 18/20
48000/48000 [==============================] - 1s 25us/step - loss: 0.2083 - acc: 0.9405 - val_loss: 0.2064 - val_acc: 0.9422
Epoch 19/20
48000/48000 [==============================] - 1s 26us/step - loss: 0.2027 - acc: 0.9426 - val_loss: 0.2019 - val_acc: 0.9437
Epoch 20/20
48000/48000 [==============================] - 1s 26us/step - loss: 0.1973 - acc: 0.9437 - val_loss: 0.1976 - val_acc: 0.9434
10000/10000 [==============================] - 0s 23us/step
Test score: 0.195731855072
Test accuracy 0.9443
Praveens-MacBook-Pro:~ praveen$