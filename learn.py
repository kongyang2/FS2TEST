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
 
    while current_date <= end_date:
        
        print("Processing data in %s" % (current_date.date()));
        rowset = session.execute("SELECT date, for_30_days, for_7_days, for_3_days, is_left FROM ml_data where date ='%s'"
                             % (current_date.date()));
    
        for row in rowset:
            new_row = {};
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
            
            data.append(new_row);
        
        current_date = current_date + timedelta(days=1);
        
    train = pd.DataFrame(data);
    
    
    
    for i,r in train.iterrows():
        if r['is_left'] == 1:
            print(r['for_30_days_conn_time'], r['for_7_days_conn_time'], r['for_3_days_conn_time'])
            
     
    y, X = train['is_left'] , train[['for_30_days_conn_time','for_7_days_conn_time','for_3_days_conn_time']];
    
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2)
   
    train_input_fn = tf.estimator.inputs.pandas_input_fn( x = X_train , y = y_train , num_epochs=1, shuffle=True);
    test_input_fn = tf.estimator.inputs.pandas_input_fn( x = X_test , y = y_test , num_epochs=1, shuffle=True);
    
    fc = [ tf.feature_column.numeric_column('for_30_days_conn_time') , tf.feature_column.numeric_column('for_7_days_conn_time') , tf.feature_column.numeric_column('for_3_days_conn_time')]
    estimator = tf.estimator.LinearClassifier( fc );
    
    estimator.train(train_input_fn);
    accuracy_score = estimator.evaluate(input_fn=test_input_fn)["accuracy"]
    print("\nTest Accuracy: {0:f}\n".format(accuracy_score))

    testDict = [{ 'for_30_days_conn_time' : 10000 , 'for_7_days_conn_time' : 100 , 'for_3_days_conn_time' : 0 }] ; 
    testData = pd.DataFrame(testDict)
    
    predict_input_fn = tf.estimator.inputs.pandas_input_fn( testData, None,  num_epochs=1,  shuffle=False);
    
    predictions = list(estimator.predict(input_fn=predict_input_fn));
    print(predictions)
            
learn('2016-02-01','2016-02-01')


