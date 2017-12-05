import pandas as pd
import tensorflow as tf
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timedelta;
from cassandra.util import OrderedMapSerializedKey
from sklearn import model_selection
from sklearn import linear_model
from sklearn import metrics

cluster = Cluster(['52.78.36.39'])
session = cluster.connect('fs2_test')
session.row_factory = dict_factory

def learn(start_date , end_date):
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d");
    end_date = datetime.strptime(end_date, "%Y-%m-%d");
    
    current_date = start_date;
    
    data = [];
    
    left_data = [];
    active_data = [];
 
    while current_date <= end_date:
        
        print("Processing data in %s" % (current_date.date()));
        rowset = session.execute("SELECT date, create_date, for_30_days, for_7_days, for_3_days, is_left FROM ml_data where date ='%s'"
                             % (current_date.date()));
    
        for row in rowset:
            new_row = {};
            create_date = datetime.fromtimestamp( row['create_date'].seconds); 
            since_create  = current_date - create_date;
            new_row.update({'since_create':since_create.days});
            
            for k,v in row.items():

                if type(v) is OrderedMapSerializedKey:
                    for k2,v2 in v.items():
                        updatekey = "%s_%s" % (k,k2);
                        new_row.update({updatekey:v2})
                else: 
                    if type(v) is bool: 
                        if v: 
                            v = 1;
                        else:
                            v = 0;
                    new_row.update({k:v});
            
            if( new_row['is_left']==1):
                left_data.append(new_row);
            else:
                active_data.append(new_row);
        
        current_date = current_date + timedelta(days=1);
        
    train = pd.DataFrame(active_data);
    left_train = pd.DataFrame(left_data);
   
    i=0;
    
    while i<5:
        train = pd.concat([train,left_train], ignore_index=True); # this is original date set 
        i+=1;

    y, X = train['is_left'] , train[['since_create','for_30_days_conn_time','for_7_days_conn_time','for_3_days_conn_time']];
    
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2)
   
    train_input_fn = tf.estimator.inputs.pandas_input_fn( x = X_train , y = y_train , num_epochs=1, shuffle=True);
    test_input_fn = tf.estimator.inputs.pandas_input_fn( x = X_test , y = y_test , num_epochs=1, shuffle=True);
    
    fc = [  tf.feature_column.numeric_column('since_create'), tf.feature_column.numeric_column('for_30_days_conn_time') , tf.feature_column.numeric_column('for_7_days_conn_time') , tf.feature_column.numeric_column('for_3_days_conn_time')]
    estimator = tf.estimator.LinearClassifier( fc );
    
    estimator.train(train_input_fn);
    accuracy_score = estimator.evaluate(input_fn=test_input_fn)["accuracy"]
    print("\nTest Accuracy: {0:f}\n".format(accuracy_score))

    #testDict = [{ 'for_30_days_conn_time' : 10000 , 'for_7_days_conn_time' : 100 , 'for_3_days_conn_time' : 0 }] ; 
    #testData = pd.DataFrame(testDict)
    # predict_input_fn = tf.estimator.inputs.pandas_input_fn( testData, None,  num_epochs=1,  shuffle=False);
    #predictions = list(estimator.predict(input_fn=predict_input_fn));
    #print(predictions)
    
    left_test = pd.DataFrame(left_data);
    y_left, X_left = left_test['is_left'] , left_test[['since_create','for_30_days_conn_time','for_7_days_conn_time','for_3_days_conn_time']];
    left_input_fn = tf.estimator.inputs.pandas_input_fn( x = X_left , y = y_left , num_epochs=1, shuffle=True);
    accuracy_score = estimator.evaluate(input_fn=left_input_fn)["accuracy"]
    print("\nLeft Test Accuracy: {0:f}\n".format(accuracy_score))
    
learn('2016-02-01','2017-10-01')

'''
x = { 'a':[3] , 'b':[2] }
y = { 'a': [4 , 5] , 'b':[ 6 , 16] }

i = pd.DataFrame(data = x);
j = pd.DataFrame(data = y);

r = pd.concat([i,j], ignore_index=True);

print( r.mad(0).a )
'''





