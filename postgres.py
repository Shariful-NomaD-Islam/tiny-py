import psycopg2


try:
    connection = psycopg2.connect(user = "user1",
                                  password = "pass1",
                                  host = "192.168.0.1",
                                  port = "5432",
                                  database = "db-name-1")

    cursor = connection.cursor()
    
    connection2 = psycopg2.connect(user = "user2",
                                  password = "pass2",
                                  host = "192.168.0.2",
                                  port = "5432",
                                  database = "db-name-2")

    cursor2 = connection2.cursor()

######### CREATE-OPERATION #######################    
    # Print PostgreSQL Connection properties
    # print ( connection.get_dsn_parameters(),"\n")
    
    # create_table_query = '''CREATE TABLE r6
    #       (ID INT PRIMARY KEY     NOT NULL,
    #       MODEL           TEXT    NOT NULL,
    #       PRICE         REAL); '''
    
    # cursor2.execute(create_table_query)
    # connection2.commit()
    # print("Table created successfully in PostgreSQL ")


######### INSERT-OPERATION #######################    
    query = """ INSERT INTO r6 (ID, MODEL, PRICE) VALUES (%s,%s,%s)"""
    record_to_insert = (5, 'One Plus 6', 950)
    cursor2.execute(postgres_insert_query, record_to_insert)
    connection2.commit()
    count = cursor2.rowcount
    print (count, "Record inserted successfully into mobile table")
    
    
######### DELETE-OPERATION #######################    
    query = """ DELETE FROM table-2 where ID = %s"""
    cursor2.execute(query, (5))
    connection2.commit()
    count = cursor2.rowcount
    print(count, " Record deleted successfully ")
    
    
######### SELECT-OPERATION #######################        
    query = "select id, primary_id, matched_id from table-1"
    cursor.execute(query)
    print("Selecting rows from mobile table using cursor.fetchall")
    mobile_records = cursor.fetchall() 
    
    afis_match_info = {}
    for row in mobile_records:
        if (row[1] in afis_match_info) == True:
            if afis_match_info[row[1]][0] > row[0]:
                afis_match_info[row[1]][0] = row[0]
        else:
            afis_match_info[row[1]] = [row[0],9999999]
        
        if (row[2] in afis_match_info) == True:
            if afis_match_info[row[1]][1] > row[0]:
                afis_match_info[row[1]][1] = row[0]
        else:
            afis_match_info[row[2]] = [9999999,row[0]]
    
    for k,v in afis_match_info.items():
        print(k,v)
        if v[0] < v[1]:
            query = "select global_id from table-2 where uuid = '{}'".format(k)
            cursor2.execute(query)
            record = cursor2.fetchone()
            if (record[0] == None):
                print (k, ' should get a global-id')
                
                ######### DELETE-OPERATION #######################    
                query = """update table-2 set status = %s where uuid = %s"""
                cursor2.execute(query, (45,k))
                connection2.commit()
                count = cursor2.rowcount
                print (count, " Record updated successfully in table-2")
            else:
                print (k, ' has a global-id: ', record[0])
        

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        if(connection2):
            cursor2.close()
            connection2.close()
            print("PostgreSQL connection2 is closed")
            
