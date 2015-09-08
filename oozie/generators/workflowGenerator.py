#!/usr/bin/env python
from quik import FileLoader
import sys

class Action(object):
	def __init__(self, id, tableName, mapTasks, mapColumn, splitBy):
		self.name = 'sqoop_action_'+str(id)
		self.table_name = tableName
		self.map_tasks = mapTasks
		self.map_column = mapColumn
		self.split_by = splitBy
		self.next_action = 'end'
	def __str__(self):
		repr;	
	def __repr__(self):
		return self.name+" "+self.table_name

class Context(object):
	def __init__(self, db_name, warehouse_dir, first_action_name):
		self.warehouse_dir = warehouse_dir
		self.db_name = db_name
		self.first_action_name = first_action_name

def getSubflows(conf_file, num_flows ):
	actions = []
	subflows = [ [] for i in range(0, num_flows)]
	for line in open(conf_file, "r"):
		objects = line.split("^")
		action = None
		subflow_id = i%num_flows
		if len(objects)==2:
			action = Action(i, objects[0], objects[1], None, None)
		if len(objects)==4:
			if objects[2]=='--split-by':
				action = Action(i, objects[0], objects[1], None, objects[3])
			if objects[2]=='--map-column-hive':
				action = Action(i, objects[0], objects[1], objects[3], None)
		if len(objects)==6:
			if objects[2]=='--split-by':
				action = Action(i, objects[0], objects[1], objects[5], objects[3])
			if objects[4]=='--split-by':
				action = Action(i, objects[0], objects[1], objects[3], objects[5])
		if len(subflows[subflow_id]) > 0:
			subflows[subflow_id][-1].next_action = action.name
		subflows[subflow_id].append(action)
		i = i + 1
	return subflows

db_name = sys.argv[1]
num_subflows = int(sys.argv[2])
sqoop_conf_file = sys.argv[3]
templates_dir = sys.argv[4]
output_dir = sys.argv[5]

print "Generating workflows for "+db_name+" with "+str(num_subflows+" workflows"

subflows = getSubflows(sqoop_conf_file, num_subflows)
subflow_names = []
loader = FileLoader(templates_dir)
for i in range(0, len(subflows)):
	subflow = subflows[i]
	context = Context(db_name, '/data/database/', subflow[0].name) 
	template = loader.load_template("subflow.xml.template")
	rendered_xml = template.render({'actions': subflow, 'context':context},
                      loader=loader).encode('utf-8')
	subflow_name= "workflow_"+str(i)
	subflow_names.append(subflow_name)
	output_subflow_file=open(output_dir+"/"+subflow_name+".xml", "w")
	output_subflow_file.write(rendered_xml)
	output_subflow_file.close()

context = Context(db_name, '/data/database/',None) 
template = loader.load_template("workflowTotal.xml.template")
rendered_xml = template.render({'subflows': subflow_names, 'context':context},
                      loader=loader).encode('utf-8')

output_subflow_file=open(output_dir+"/workflow.xml", "w")
output_subflow_file.write(rendered_xml)
output_subflow_file.close()

	





	

