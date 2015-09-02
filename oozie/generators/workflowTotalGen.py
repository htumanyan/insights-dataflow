#!/usr/bin/env python
from sys import argv
import re
import xml.sax
import xml.etree.ElementTree as ET
	
#ex startup: python generators/workflowTotalGen.py 9 workflows/rpm/dev_us/

script, numSubFlows, flowPath = argv

maxSize = int(numSubFlows)
root = ET.Element('workflow-app', {"xmlns:sla":"uri:oozie:sla:0.2", "xmlns":"uri:oozie:workflow:0.5", "name":"sqoop-wf"})
ET.SubElement(root, 'start', to='forkingMain')
fork = ET.SubElement(root, 'fork', name='forkingMain')
# for arr of max size create list of root elements then iterate through until out 
# of sqoop commands
for index in range(maxSize):
	ET.SubElement(fork, 'path', start='sqoop-subFlow'+str(index+1))

for flownum in range(maxSize):
	action = ET.SubElement(root, 'action', name='sqoop-subFlow'+str(flownum+1))
	sub = ET.SubElement(action, 'sub-workflow')
	ET.SubElement(sub, 'app-path').text = '${nameNode}/user/oozie/share/' + flowPath + 'workflow_' + str(flownum+1) + '.xml'
	ET.SubElement(sub, 'propagate-configuration')
	ET.SubElement(action, 'ok', to='joiningMain')
	ET.SubElement(action, 'error', to='joiningMain')

ET.SubElement(root, 'join', name="joiningMain", to="endMain")
failEmail = ET.SubElement(root, 'action', name='failEmail')
esubFail = ET.SubElement(failEmail, 'email', xmlns='uri:oozie:email-action:0.1')
ET.SubElement(esubFail, 'to').text = '${alertEmail}'
ET.SubElement(esubFail, 'subject').text = 'oozie job failure'
ET.SubElement(esubFail, 'body').text = 'The workflow ${wf:id()} name ${wf:name()} path ${wf:appPath()} had issues and was killed.  The error message is: ${wf:errorMessage(wf:lastErrorNode())}'
ET.SubElement(failEmail, 'ok', to='fail')
ET.SubElement(failEmail, 'error', to='fail')
kill = ET.SubElement(root, 'kill', name='fail')
ET.SubElement(kill, 'message').text = 'Sqoop failed, error message[${wf:errorMessage(wf:lastErrorNode())}]'
ET.SubElement(root, 'end', name='endMain')
sla = ET.SubElement(root, 'sla:info')
ET.SubElement(sla, 'sla:nominal-time').text = '${nominal_time}'
ET.SubElement(sla, 'sla:should-start').text = '${10 * MINUTES}'
ET.SubElement(sla, 'sla:max-duration').text = '${45 * MINUTES}'
ET.SubElement(sla, 'sla:alert-events').text = 'duration_miss'
ET.SubElement(sla, 'sla:alert-contact').text = '${alertEmail}'
tree = ET.ElementTree(root)
tree.write(flowPath + 'WORKFLOWTEST.xml')
