package com.nusla.logi;

import org.w3c.dom.*;


import java.util.*;
import com.logixml.plugins.LogiPluginObjects10; 
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import javax.xml.transform.OutputKeys;


public class SQLPredicateGenerator 
{
	public void generatePredicates(LogiPluginObjects10 rdObjects) 
	{
		try
	      {
			Hashtable params = rdObjects.getPluginParameters();
			String dataElementName = (String) params.get("DataElementName");
			String predicateMapString = (String) params.get("PredicateMap");

			String sqlQueryPrefix = (String) params.get("sqlQueryPrefix");
			String sqlQuerySuffix = (String) params.get("sqlQuerySuffix");

			StringBuffer sqlQueryBuff = new StringBuffer();
			sqlQueryBuff.append(sqlQueryPrefix);
			
			rdObjects.addDebugMessage("SQLPredicateGenerator", "dataElementName", dataElementName);
			rdObjects.addDebugMessage("SQLPredicateGenerator", "predictateMapString", predicateMapString);
			
			String[] kvStrings = predicateMapString.split(",");
			StringBuffer predicateString = new StringBuffer();
			boolean isFirstElement = true;
			for(String kvString: kvStrings)
			{
				String[] kv = kvString.split(":");
				String sqlColumnName = kv[0];
				String requestParamName = kv[1];
				int iPos = rdObjects.getRequestParameterNames().indexOf((Object)requestParamName);
				if (iPos < 0)
		        { 
					rdObjects.addDebugMessage("SQLPredicateGenerator", "WARINING", "request parameter "+requestParamName+" not found");
		        }
				else
				{
					  String paramValue = (String)rdObjects.getRequestParameterValues().get(iPos);
					  rdObjects.addDebugMessage("SQLPredicateGenerator", "processing param", requestParamName+":"+sqlColumnName+":"+paramValue);
					  if(!paramValue.equals("") && !paramValue.equals("null"))
					  {
						  if(!isFirstElement)
						  {
							  predicateString.append(" AND ");
						  }
						  predicateString.append(" ");
						  predicateString.append(sqlColumnName);
						  predicateString.append("=");
						  predicateString.append(paramValue);	
						  isFirstElement = false;
					  }
					  
				}
			}
			
            if(predicateString.length() != 0)
            {
                sqlQueryBuff.append(" WHERE ");
                sqlQueryBuff.append(predicateString);
            }
			sqlQueryBuff.append(" ");
			sqlQueryBuff.append(sqlQuerySuffix);
			String finalSqlString = sqlQueryBuff.toString();
			rdObjects.addDebugMessage("SQLPredicateGenerator","resulting SQL", finalSqlString);
			
		     DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
	         DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
	         byte b[] = rdObjects.getCurrentDefinition().getBytes();
	         java.io.ByteArrayInputStream input = new java.io.ByteArrayInputStream(b);
	         Document xmlDefinition = docBuilder.parse(input);
	         NodeList nl = xmlDefinition.getElementsByTagName("DataLayer");
	         Element eleDataLayer = null;
	         for(int i=0; i<nl.getLength(); i++)
	         {
	        	  Element element = (Element)nl.item(i);
	        	 rdObjects.addDebugMessage("SQLPredicateGenerator","Found data layer object",element.getAttribute("ID"));
	        	  if(element.getAttribute("ID").equals(dataElementName))
	        	  {
	        		  eleDataLayer = element;
	        	  }
	         }
			 if (eleDataLayer  == null)
	         {
	            throw new Exception("The report is missing the "+dataElementName+" element.");
	         }
			 rdObjects.addDebugMessage("SQLPredicateGenerator","current source attribute value", eleDataLayer.getAttribute("Source"));
			  
		      eleDataLayer.setAttribute("Source", finalSqlString );
		      rdObjects.setCurrentDefinition(getOuterXml(xmlDefinition));
	      }
		catch(Exception ex)
		{
			 System.out.println("SQLPredicateGenerator Error " + ex.getMessage());
		}
		
	}
	
	private String getOuterXml(Node node)
	{
		try
		{
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty("omit-xml-declaration", "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(node), new StreamResult(writer));
			String result = writer.toString();
			return result;
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			System.out.println("Transformer Error " + ex.getMessage());
			return "Transformer getOuterXml Error " + ex.getMessage();
		}
	}
}