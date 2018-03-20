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