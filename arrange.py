from cassandra.cluster import Cluster
from datetime import datetime, timedelta;

cluster = Cluster(['52.78.36.39'])
session = cluster.connect('fs2_test')

session2 = cluster.connect('fs2_test')
session_ilu = cluster.connect('fs2_test');
        
        
# for test        
def update_all_tables_from_daily_user(target_date, date_diff_for_left_user):
    
    target_date = datetime.strptime(target_date, "%Y-%m-%d");
    
    rowset = session.execute("SELECT user_id, date, amount, conn_time, play_count, play_time, win_count from daily_user")
     
    for current in rowset:
        now = datetime.now().strftime('%Y-%m-%d');
        
        summary_set = session2.execute(
            "SELECT amount,conn_time,play_count,play_time,win_count,login_day_count \
            FROM user_summary_lifetime WHERE user_id='%s'"%(current.user_id) );
        
        if not summary_set:
            amount = 0; 
            conn_time = 0; 
            play_count = 0;
            play_time = 0;
            win_count = 0 
            login_day_count = 0;
        else:
            amount = summary_set[0].amount; 
            conn_time = summary_set[0].conn_time; 
            play_count = summary_set[0].play_count;
            play_time = summary_set[0].play_time;
            win_count = summary_set[0].win_count 
            login_day_count = summary_set[0].login_day_count;
        
              
        amount += current.amount; 
        conn_time += current.conn_time; 
        play_count += current.play_count;
        play_time += current.play_time;
        win_count += current.win_count; 
        login_day_count +=1;
                
        session2.execute("INSERT INTO user_summary_lifetime (user_id , amount , conn_time , play_count , play_time , win_count , login_day_count, last_updated) \
        VALUES ('%s',%d, %d , %d , %d , %d , %d ,'%s')" % (current.user_id, amount , conn_time , play_count, play_time, win_count, login_day_count, now));
        
        current_date = datetime.fromtimestamp(current.date.seconds); 
           
        if ((current_date <= target_date ) and (current_date > target_date - timedelta(days=30)) ):
            session2.execute("INSERT INTO daily_user_30 (user_id,date,amount,conn_time,play_count,play_time,win_count)\
            VALUES ('%s','%s', %d , %d , %d , %d , %d)" % (current.user_id , current.date, current.amount 
            , current.conn_time , current.play_count, current.play_time, current.win_count));
            


def is_left_user(user_id , target_date , left_days):
    
    
    #target_date = datetime.strptime(target_date, "%Y-%m-%d");

    left_days = timedelta(days=left_days);
    
    ret = session_ilu.execute("SELECT user_id from daily_user where user_id='%s' and date > '%s' and date <= '%s'"
                              % (user_id, target_date.date(), (target_date + left_days).date()));
                              
    if not ret:
        return True;
    else:
        return False;
    

class UserInputData:
    
    user_id = ''
    is_left = False
    
    def __init__(self, user_id):
        self.user_id = user_id; 
        self.for_30_days = {
        'amount': 0,
        'conn_time' : 0,
        'play_time' : 0,
        'play_count' : 0,
        'win_count' : 0,
        'login_day_count' : 0
        } 
        
        self.for_7_days = {
        'amount': 0,
        'conn_time' : 0,
        'play_time' : 0,
        'play_count' : 0,
        'win_count' : 0
        'login_day_count' : 0
        } 
    
        self.for_3_days = {
        'amount': 0,
        'conn_time' : 0,
        'play_time' : 0,
        'play_count' : 0,
        'win_count' : 0
        'login_day_count' : 0
        } 
    

def make_input_table(target_date):
    
    target_date = datetime.strptime(target_date, "%Y-%m-%d");
    
   # session.execute("TRUNCATE TABLE daily_user_30");
    target_users = session.execute("SELECT user_id FROM daily_user_2 WHERE date = '%s'" % (target_date.date()) );
    user_list = dict();
     
    for user in target_users:
        query = "SELECT user_id, date, amount, conn_time, play_count, play_time, win_count from daily_user \
        where date <= '%s' and date >= '%s' and user_id='%s'" % (target_date.date(),  (target_date - timedelta(days=30)).date(), user.user_id );

        rowset = session.execute(query);
        u = UserInputData(user.user_id);
        user_list[user.user_id] = u;
        
        for current in rowset:
            u.for_30_days['amount']+= current.amount; 
            u.for_30_days['conn_time']+= current.conn_time; 
            u.for_30_days['play_time']+= current.play_time; 
            u.for_30_days['play_count']+= current.play_count; 
            u.for_30_days['win_count']+= current.win_count; 
            u.for_30_days['login_day_count']+=1
            
            current_date = datetime.fromtimestamp(current.date.seconds); 
            if current_date >= (target_date - timedelta(days=7)):
                u.for_7_days['amount']+= current.amount; 
                u.for_7_days['conn_time']+= current.conn_time; 
                u.for_7_days['play_time']+= current.play_time; 
                u.for_7_days['play_count']+= current.play_count; 
                u.for_7_days['win_count']+= current.win_count; 
                u.for_7_days['login_day_count']+=1
            if current_date >= (target_date - timedelta(days=3)):
                u.for_3_days['amount']+= current.amount; 
                u.for_3_days['conn_time']+= current.conn_time; 
                u.for_3_days['play_time']+= current.play_time; 
                u.for_3_days['play_count']+= current.play_count; 
                u.for_3_days['win_count']+= current.win_count; 
                u.for_3_days['login_day_count']+=1
            
    #for u in user_list.values():
     #   print(u.user_id, u.for_30_days['conn_time'] , u.for_7_days['conn_time']);
        
        
    for u in user_list.values():
        if is_left_user(u.user_id, target_date , 30 ):
            u.is_left = True; 
            
        sql = "INSERT INTO ml_data ( id , date, for_30_days , for_7_days , for_3_days , is_left ) \
        VALUES ( '%s', '%s', { 'login_day_count' : %d , 'amount' : %d , 'conn_time' : %d , 'play_time' : %d , 'play_count' : %d , 'win_count' : %d }, \
        { 'login_day_count' : %d , 'amount' : %d , 'conn_time' : %d , 'play_time' : %d , 'play_count' : %d , 'win_count' : %d }, \
        { 'login_day_count' : %d , 'amount' : %d , 'conn_time' : %d , 'play_time' : %d , 'play_count' : %d , 'win_count' : %d }, %r)"\
         % ( u.user_id , target_date.date(), u.for_30_days['login_day_count'], u.for_30_days['amount'], u.for_30_days['conn_time'], u.for_30_days['play_time'], u.for_30_days['play_count'],  u.for_30_days['win_count'],
         u.for_7_days['login_day_count'], 'u.for_7_days['amount'], u.for_7_days['conn_time'], u.for_7_days['play_time'], u.for_7_days['play_count'],  u.for_7_days['win_count'],
         u.for_3_days['amount'], u.for_3_days['conn_time'], u.for_3_days['play_time'], u.for_3_days['play_count'],  u.for_3_days['win_count'],
         u.is_left );
         
        session.execute(sql);
            
#make_input_table('2017-01-01');


start_date = datetime.strptime('2016-02-01', "%Y-%m-%d");
current_date = start_date;
end_date = datetime.strptime('2017-11-01', "%Y-%m-%d");

while current_date < end_date:
    print(current_date.date());
    make_input_table(current_date.date().strftime("%Y-%m-%d"));
    current_date = current_date + timedelta(days=1);







    
    
                                                                                  