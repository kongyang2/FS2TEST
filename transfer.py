import pymssql
from cassandra.cluster import Cluster

user = "JCAdmin"
password = "@Vmf?wUx@yU"

conn = pymssql.connect("52.52.170.33", user, password , "WorkDB")
cursor = conn.cursor(as_dict=True)

cluster = Cluster(['52.78.36.39'])
session = cluster.connect('fs2_test')


def read_from_sql(from_date, to_date):
    
    query = "SELECT StatDate,UserSN,ConnTime,PlayTime,PlayCntTotal,\
    WinCntTotal,Amount,CreateDate from dbo.DailyUser where StatDate >= '%s' and StatDate <= '%s'" % (from_date, to_date)
    
    cursor.execute(query)
    count = 0; 
    for row in cursor:
        
        query = "INSERT INTO daily_user (user_id, date, amount, conn_time, play_count, play_time, win_count, create_date)\
        values ('%s','%s',%d,%d,%d,%d,%d,'%s')" % (row['UserSN'], row['StatDate'], row['Amount'], row['ConnTime'], row['PlayCntTotal'], row['PlayTime'], row['WinCntTotal'], row['CreateDate'])
        
       # print(query)
        session.execute(query)
        count+=1;
        if count % 100 == 0:
            print('from sql %d fetched' % count , end='\r')

 
def move_to_daily_user_2():
    
    rows = session.execute("SELECT user_id , date , amount , conn_time, play_count , play_time , win_count , create_date FROM daily_user");
    count = 0;
    for r in rows:
        session.execute("INSERT INTO daily_user_2 (user_id,date,amount,conn_time,play_count,play_time,win_count, create_date , is_applied) VALUES ('%s','%s',%d,%d,%d,%d,%d,'%s',%s)"
                        % (r.user_id , r.date , r.amount , r.conn_time, r.play_count , r.play_time, r.win_count , r.create_date, False));
        count+=1;
        if count % 100 == 0:
            print('from daily_user %d fetched' % count , end='\r')
            
read_from_sql('2016-01-01', '2017-11-20')
move_to_daily_user_2();

