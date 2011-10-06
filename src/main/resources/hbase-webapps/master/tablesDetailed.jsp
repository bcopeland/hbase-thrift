<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HTableDescriptor" %><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
%>
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>HBase Master: <%= master.getServerName()%>%></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>

<h2>User Tables</h2>
<% HTableDescriptor[] tables = new HBaseAdmin(conf).listTables();
   if(tables != null && tables.length > 0) { %>
<table>
<tr>
    <th>Table</th>
    <th>Description</th>
</tr>
<%   for(HTableDescriptor htDesc : tables ) { %>
<tr>
    <td><%= htDesc.getNameAsString() %></td>
    <td><%= htDesc.toString() %></td>
</tr>
<%   }  %>

<p> <%= tables.length %> table(s) in set.</p>
</table>
<% } %>
</body>
</html>
