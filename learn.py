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
            if new_row['for_30_days_conn_time'] > 0:     
                new_row.update({'ratio_conn_time_7_to_30' : (new_row['for_7_days_conn_time']/7) / (new_row['for_30_days_conn_time']/30) });
            else:
                new_row.update({'ratio_conn_time_7_to_30' : 0 });
            
            data.append(new_row);
        
        current_date = current_date + timedelta(days=1);
        
    train = pd.DataFrame(data);
    
    y, X = train['is_left'] , train[['for_30_days_conn_time','for_7_days_conn_time','ratio_conn_time_7_to_30']];
    
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2)
    lr = linear_model.LogisticRegression()
    lr.fit(X_train, y_train)
    print(metrics.accuracy_score(y_test, lr.predict(X_test)))
    
    
            
learn('2016-02-01','2016-03-01')