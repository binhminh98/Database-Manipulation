import psycopg2                                                   # import libraries
import psycopg2.extras
from tabulate import tabulate

def getConn():
    #function to retrieve the password, construct
    #the connection string, make a connection and return it.
    pwFile = open("pw.txt", "r")
    pw = pwFile.read();
    pwFile.close()
    connStr = "host='cmpstudb-01.cmp.uea.ac.uk' \
               dbname= 'tda21hwu' user='tda21hwu' password = " + pw
    #connStr=("dbname='studentdb' user='dbuser' password= 'dbPassword' " )
    conn=psycopg2.connect(connStr)      
    return  conn

def clearOutput():                                                # define function clear output
    with open("output2.txt", "w") as clearfile:
        clearfile.write('')
        
def writeOutput(output):                                          # define function write output
    with open("output2.txt", "a") as myfile:
        myfile.write(output)

raw_input = open('testpart2.txt').read().splitlines()             # list of every line in raw data

# separate raw input list into smaller task lists
task_type = ['A','B','C','D','E','F','P','Q','R','S','T','V']     # list of task types
individual_task = []                                              # list of each task's details, reset for each task
task_list = []                                                    # an aggregated list of all tasks

for line in raw_input:
   if line in task_type:
        task_list.append(individual_task)                         # append the last individual task into task list
        individual_task = []                                      # reset individual task
        individual_task.append(line)                              # start a new task, append task_type to the first position
   elif line == 'X':
        task_list.append(individual_task)
        individual_task = []
        individual_task.append(line)                              # special case of task 'X'
        task_list.append(individual_task)
        individual_task = []
   else:
        individual_task.append(line)                              # append details into an individual task until the next task_type

clearOutput()
for task in task_list:
    try:
        conn=None
        conn=getConn()
        cur = conn.cursor()
        if(task == []):                                           # if the individual task is empty
            continue
        elif(task[0] == 'A'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "INSERT INTO student(sno,sname,semail)\
            VALUES ({number},'{name}','{email}')".format(number = int(task[1]),name = task[2],email = task[3])
            cur.execute(sql)
            conn.commit()
            cur.execute('SELECT * FROM student WHERE sno = {}'.format(int(task[1]))) # check against the database
            rows = cur.fetchall()
            # check for wrong input values #
            if rows != []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')            
                writeOutput(f'You have successfully inserted a student: {task[2]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #            
        elif(task[0] == 'B'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "INSERT INTO exam(excode,extitle,exlocation,exdate,extime)\
            VALUES ('{excode}','{extitle}','{exlocation}','{exdate}','{extime}')".format(excode = task[1],\
            extitle = task[2],exlocation = task[3],exdate = task[4],extime = task[5])
            cur.execute(sql)
            conn.commit()
            cur.execute("SELECT * FROM exam WHERE excode = '{}'".format(task[1])) # check against the database
            rows = cur.fetchall()
            # check for wrong input values #         
            if rows != []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')            
                writeOutput(f'You have successfully inserted an exam with excode: {task[1]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #    
        elif(task[0] == 'C'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = 'DELETE FROM student WHERE sno = {}'.format(task[1])
            cur.execute(sql)
            conn.commit()
            cur.execute('SELECT * FROM student WHERE sno = {}'.format(int(task[1]))) # check against the database
            rows = cur.fetchall()
            # check for wrong input values #
            if rows == []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')         
                writeOutput(f'You have successfully deleted a student with sno: {task[1]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #    
        elif(task[0] == 'D'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "DELETE FROM exam WHERE excode = '{}'".format(task[1])
            cur.execute(sql)
            conn.commit()
            cur.execute('SELECT * FROM exam WHERE excode != "{}"'.format(task[1])) # check against the database
            rows = cur.fetchall()
            # check for wrong input values #
            if rows == []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')
                writeOutput(f'You have successfully deleted an exam with excode: {task[1]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #    
        elif(task[0] == 'E'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "INSERT INTO entry(eno,excode,sno)\
            VALUES ((SELECT COALESCE(MAX(eno),0) FROM entry)+1,'{excode}',{sno})".format(excode = task[1],\
            sno = int(task[2]))
            cur.execute(sql)
            conn.commit()
            cur.execute("SELECT * FROM entry WHERE excode = '{excode}' AND \
                sno = '{sno}'".format(excode = task[1],sno = int(task[2]))) # check against the database
            rows = cur.fetchall()
            if rows != []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')
                writeOutput(f'You have successfully inserted an examination entry with excode: {task[1]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'F'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = f'UPDATE entry SET egrade = {task[2]} WHERE eno = {int(task[1])}'
            cur.execute(sql)
            conn.commit()
            cur.execute(f'SELECT * FROM entry WHERE eno = {int(task[1])} AND egrade = {task[2]}') # check against the database
            rows = cur.fetchall()
            # check for wrong input values #
            if rows != []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n')
                writeOutput(f'You have successfully updated a student\'s grade with eno: {task[1]} to: {task[2]}' + '\n \n')
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'P'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT sname AS student_name, exlocation AS exam_location, e.excode AS exam_code,\
                extitle AS exam_title, exdate AS exam_date, extime AS exam_time FROM exam e, student s, entry en \
	            WHERE e.excode = en.excode AND s.sno = en.sno AND s.sno = {sno};".format(sno = int(task[1]))
            cur.execute(sql)
            rows = cur.fetchall()
            # check for wrong input values #            
            if rows != []:                                        # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Student Name','Location','Exam Code','Exam title','Exam Date','Exam Time']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #            
        elif(task[0] == 'Q'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT en.excode AS exam_code, sname AS student_name, egrade AS grade, classify_grade(en.*) AS results\
	        FROM entry en, student s WHERE en.sno = s.sno ORDER BY en.excode, sname;"
            cur.execute(sql)
            rows = cur.fetchall()
            # check for wrong input values #
            if rows != []:                                         # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Exam Code','Student Name','Grade','Result']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'R'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT en.excode AS exam_code, sname AS student_name, egrade AS grade, classify_grade(en.*) AS results \
	              FROM entry en, student s WHERE en.sno = s.sno \
                  AND excode = '{excode}' ORDER BY en.excode, sname;".format(excode = task[1])
            cur.execute(sql)
            rows = cur.fetchall()
            # check for wrong input values #            
            if rows != []:                                         # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Exam Code','Student Name','Grade','Result']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'S'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT sname AS student_name, e.excode AS exam_code, extitle AS exam_title, egrade AS grade \
                  FROM exam e, student s, entry en \
                  WHERE e.excode = en.excode AND s.sno = en.sno AND s.sno = {sno} ORDER BY e.excode;".format(sno = int(task[1]))
            cur.execute(sql)
            rows = cur.fetchall()
            if rows != []:                                          # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Student Name','Exam Code','Exam Title','Grade']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'T'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT DISTINCT sname, classify_student(s.*,{sno}) AS membership_status \
                  FROM student s LEFT JOIN entry en ON s.sno = en.sno;".format(sno = int(task[1]))
            cur.execute(sql)
            rows = cur.fetchall()
            # check for wrong input values #            
            if rows != []:                                           # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Student Name','Membership Status']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'V'):
            cur.execute("SET SEARCH_PATH to database_coursework;");
            sql = "SELECT * FROM cancel WHERE sno = {sno};".format(sno = int(task[1]))
            cur.execute(sql)
            rows = cur.fetchall()
            # check for wrong input values #
            if rows != []:                                           # != to check if rows is not empty, == to check if rows is empty
                writeOutput(f'Task type: {task[0]}' + '. The result is: ' + '\n \n')
                writeOutput(tabulate(rows, headers=['Entry Number','Exam Code','Student Number','Timestamp','User']) + "\n \n")
            else:
                error_message = f'Task type: {task[0]}' + "\n" + f'Error message: Wrong input value' + "\n \n"
                writeOutput(error_message)
            # check for wrong input values #
        elif(task[0] == 'X'):
            print("Exit {}".format(task[0]))
            writeOutput("\nExit program!")
    except Exception as e:
        print (e)
        error_message = f'Task type: {task[0]}' + "\n" + f'Error message: {str(e)}' + "\n"
        writeOutput(error_message)
        continue
    finally:
        if conn:
            conn.close()