#!/usr/bin/env python
from sys import argv
import re
import xml.sax
import xml.etree.ElementTree as ET

class sqoopCommand(object):
	
	def __init__(self, id, tableName, mapTasks, mapColumn, splitBy):
		self.id = id
		self.tableName = tableName
		self.mapTasks = mapTasks
		self.mapColumn = mapColumn
		self.splitBy = splitBy
	
#ex startup: python generators/workflowsParse.py 9 psa ../resource/table.txt workflows/workflowFull_
#ex startup: python generators/workflowsParse.py 3 rpm ../resource/tablesUsed.txt workflows/devflows/workflow_
script, numSubFlows, dataSource, inFilename, outFilePath = argv

txt = open(inFilename, "r")

actions = []
i=0
for line in txt:
    objects = line.split("^")
    if len(objects)==2:
    	cmd = sqoopCommand(i, objects[0], objects[1], 0, 0)
    	actions.append(cmd)
    if len(objects)==4:
    	if objects[2]=='--split-by':
    		cmd = sqoopCommand(i, objects[0], objects[1], 0, objects[3])
    		actions.append(cmd)
    	if objects[2]=='--map-column-hive':
    		cmd = sqoopCommand(i, objects[0], objects[1], objects[3], 0)
    		actions.append(cmd)
    if len(objects)==6:
    	if objects[2]=='--split-by':
    		cmd = sqoopCommand(i, objects[0], objects[1], objects[5], objects[3])
    		actions.append(cmd)
    	if objects[4]=='--split-by':
    		cmd = sqoopCommand(i, objects[0], objects[1], objects[3], objects[5])
    		actions.append(cmd)
    i = i + 1


numActions = len(actions)
maxSize = int(numSubFlows) #8 or 9 optimal for current setup of mappers 3-19-15
root = ET.Element('workflow-app', xmlns='uri:oozie:workflow:0.2', name='sqoop-wf')
# for arr of max size create list of root elements then iterate through until out 
# of sqoop commands
workFlows = []
for index in range(maxSize):
	root = ET.Element('workflow-app', xmlns='uri:oozie:workflow:0.2', name='sqoop-wf')
	ET.SubElement(root, 'start', to='sqoop-node0')
	workFlows.append(ET.ElementTree(root))

i=0
fullWorkFlow = []
for action in actions:
	workFlowIndex = action.id % maxSize
	root = workFlows.pop().getroot()
	actXml = ET.SubElement(root, 'action', name='sqoop-node' + str(i/maxSize))	
	sqoop = ET.SubElement(actXml, 'sqoop', xmlns='uri:oozie:sqoop-action:0.2')
	ET.SubElement(sqoop, 'job-tracker').text = '${jobTracker}'
	ET.SubElement(sqoop, 'name-node').text = '${nameNode}'
	prepare = ET.SubElement(sqoop, 'prepare')
	ET.SubElement(prepare, 'delete', path='/data/database/' + dataSource + '/' + action.tableName)
	configuration = ET.SubElement(sqoop, 'configuration')
	property = ET.SubElement(configuration, 'property')
	ET.SubElement(property, 'name').text = 'mapred.job.queue.name'
	ET.SubElement(property, 'value').text = '${queueName}'
	property2 = ET.SubElement(configuration, 'property')
	ET.SubElement(property2, 'name').text = 'oozie.hive.defaults'
	ET.SubElement(property2, 'value').text = 'tmp/oozie-hive-site.xml'
	ET.SubElement(sqoop, 'arg').text = 'import'
	ET.SubElement(sqoop, 'arg').text = '--connect'
	ET.SubElement(sqoop, 'arg').text = '${jdbcUrl}'
	ET.SubElement(sqoop, 'arg').text = '--username'
	ET.SubElement(sqoop, 'arg').text = '${sqlUser}'
	ET.SubElement(sqoop, 'arg').text = '--password'
	ET.SubElement(sqoop, 'arg').text = '${sqlPass}'
	ET.SubElement(sqoop, 'arg').text = '--table'
	ET.SubElement(sqoop, 'arg').text = action.tableName
	ET.SubElement(sqoop, 'arg').text = '--warehouse-dir=/data/database/' + dataSource + '/'
	ET.SubElement(sqoop, 'arg').text = '-m'
	ET.SubElement(sqoop, 'arg').text = str(action.mapTasks)
	ET.SubElement(sqoop, 'arg').text = '--hive-import'
	ET.SubElement(sqoop, 'arg').text = '--hive-overwrite'
	ET.SubElement(sqoop, 'arg').text = '--hive-table'
	ET.SubElement(sqoop, 'arg').text = dataSource + '.' + action.tableName + '_stg'
	if action.mapColumn!=0:
		ET.SubElement(sqoop, 'arg').text = '--map-column-hive'
		ET.SubElement(sqoop, 'arg').text = action.mapColumn
	if action.splitBy!=0:
		ET.SubElement(sqoop, 'arg').text = '--split-by'
		ET.SubElement(sqoop, 'arg').text = action.splitBy
	ET.SubElement(sqoop, 'file').text = '/tmp/hive-site.xml#hive-site.xml'
	if (i+maxSize)/numActions > 0:
		ET.SubElement(actXml, 'ok', to='end')
	else: 
		ET.SubElement(actXml, 'ok', to='sqoop-node' + str(i/maxSize + 1))
	ET.SubElement(actXml, 'error', to='failEmail')
	workFlows.insert(0, ET.ElementTree(root))
	i = i + 1

i = 0
for workFlow in workFlows:
	root = workFlow.getroot()
	failEmail = ET.SubElement(root, 'action', name='failEmail')
	esubFail = ET.SubElement(failEmail, 'email', xmlns='uri:oozie:email-action:0.1')
	ET.SubElement(esubFail, 'to').text = '${alertEmail}'
	ET.SubElement(esubFail, 'subject').text = 'oozie job failure'
	ET.SubElement(esubFail, 'body').text = 'The workflow ${wf:id()} name ${wf:name()} path ${wf:appPath()} had issues and was killed.  The error message is: ${wf:errorMessage(wf:lastErrorNode())}'
	ET.SubElement(failEmail, 'ok', to='fail')
	ET.SubElement(failEmail, 'error', to='fail')
	kill = ET.SubElement(root, 'kill', name='fail')
	ET.SubElement(kill, 'message').text = 'Sqoop failed, error message[${wf:errorMessage(wf:lastErrorNode())}]'
	ET.SubElement(root, 'end', name='end')	
	tree = ET.ElementTree(root)
	tree.write(outFilePath + str(i+1) + '.xml')
	i = i+1