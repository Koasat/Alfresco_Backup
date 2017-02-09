import psycopg2
import psycopg2.extras
import sys
import array
from shutil import copyfile
import os

# data base properties

db_name = "alfresco"
db_user = "alfresco"
db_pwd = "password"

# contentstore folder from which the backup will be made and destination folder

contentstore_path = "/path/to/contentstore"
backup_path = "/path/to/Alfresco_Backup"

# connecting to the database

try:
	conn=psycopg2.connect("dbname='" + db_name + "' user='" + db_user + "' password='" + db_pwd + "'")
	print ("Connected to " + db_name + "\n--------------------------------------------------------------------------------- \n")
except:
	print ("I am unable to connect to the database.")
	
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# recursive function to retrieve the file path

def recover_rec (parent_id, path):
	p_id = str(parent_id)
	
	cur.execute("select * from alf_child_assoc where child_node_id =" + p_id + ";")
	c_node = cur.fetchone()
	new_path = path + '/' + c_node["qname_localname"]
	cur.execute("select * from alf_child_assoc where parent_node_id =" + p_id + ";")
	
	os.makedirs(os.path.dirname(backup_path+new_path), exist_ok=True) # recreate empty folders
	
	nodes = cur.fetchall()
	for node in nodes:
		if not(node["type_qname_id"]==37): # if it isn't 37 the parent is a file
			cur.execute("select * from alf_node_properties where node_id =" + str(node["parent_node_id"]) + "and long_value > 0;")
			np = cur.fetchone() # get the node's properties, the long_value is the content_data id
			if np:
				cur.execute("select * from alf_content_data where id = " + str(np['long_value']) + ";")
				nd = cur.fetchone() # content_url_id is the id in the alf_content_url table
				if nd:
					cur.execute("select * from alf_content_url where id = " + str(nd['content_url_id']) + ";")
					nu = cur.fetchone() # Alfresco calls the binary path a url...
					if nu:
						# os.makedirs(os.path.dirname(backup_path+new_path), exist_ok=True) # recreate only used folders						copyfile(contentstore_path + nu['content_url'][7:], backup_path + new_path)
			return
		recover_rec(node['child_node_id'],new_path)

recover_rec(13,"") # 13 is the directories root
