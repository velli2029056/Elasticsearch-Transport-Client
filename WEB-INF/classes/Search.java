import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.json.simple.*;
import org.json.simple.parser.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


public class Search {
	public static boolean isIndexExist(Client client, String index) throws Exception {
		return client.admin().indices().prepareExists(index).get().isExists();
	}
	@SuppressWarnings({ "unchecked", "resource" })
	public static void retrieve(String sql,PrintWriter out) throws Exception
	{
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		}
		catch(UnknownHostException e)
		{
			System.out.println(e);
		}
		//String sql="select firstname,lastname,salary,designation from employee where salary>15000";
		String arr[]=sql.split(" ");
		int len=arr.length;
	    boolean exists=isIndexExist(client,arr[3]);
	    if(exists)
	    {
	    	if(arr[1].equals("*") && len==4)
	    	{
		    	SearchResponse sr = new SearchTemplateRequestBuilder(client)
		    	        .setScript("{\n" +                                  
		    	                "        \"query\" : {\n" +
		    	                "            \"match_all\" : {}\n" +
		    	                "        }\n" +
		    	                "}")
		    	        .setScriptType(ScriptType.INLINE)  
		    	        .setRequest(new SearchRequest(arr[3]))                   
		    	        .get()                                             
		    	        .getResponse();   
		    	Map<String, Object> m=null;
				out.println("<html>");
				out.println("</style></head>");
				out.println("<body><fieldset><table class='table'><thead><tr>");
				int i=0;
				for (SearchHit hit : sr.getHits().getHits()) 
				{
			    	m=hit.getSourceAsMap();
			    	if(i==0)
					{
						for(Entry<String,Object> entry:m.entrySet())
							  out.println("<th scope='col'>"+entry.getKey()+"</th>");
						out.println("</tr></thead>");
						i++;
					}
					if(i!=0)
					{
						out.println("<tr>");
						for(Entry<String,Object> entry:m.entrySet())
						{
							  out.println("<td id='col1'>"+entry.getValue()+"</td>");
						}
						out.println("</tr>");
					}
		    	}
				out.println("</table></fieldset></body></html>");
	    	}
	    	else if(arr[1].contentEquals("*")&& arr[4].equalsIgnoreCase("where"))
	    	{
	    		char op=0;
	    		for (char ch : arr[5].toCharArray()) 
				{
				    if (!Character.isDigit(ch) && !Character.isLetter(ch))
				    {
				    	op=ch;
				    	break;
				    }
				}
	    		if(op=='=')
	    		{
	    			String vals[]=arr[5].split("=");
					QueryBuilder qb =matchQuery(vals[0],vals[1]);
	    			SearchResponse sr= client.prepareSearch(arr[3])
	    			        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.DESC)
	    			        .setScroll(new TimeValue(60000))
	    			        .setQuery(qb)
	    			        .setSize(100).get();
					Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
		    	}
	    		else if(op=='<')
	    		{
	    			String vals[]=arr[5].split("<");
	    			QueryBuilder qb =rangeQuery(vals[0]).lt(vals[1]);
	    			SearchResponse sr= client.prepareSearch(arr[3])
	    			        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
	    			        .setScroll(new TimeValue(60000))
	    			        .setQuery(qb)
	    			        .setSize(100).get();
			    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    		}
	    		else if(op=='>')
	    		{
	    			String vals[]=arr[5].split(">");
	    			QueryBuilder qb =rangeQuery(vals[0]).gt(vals[1]);
	    			SearchResponse sr= client.prepareSearch(arr[3])
	    			        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
	    			        .setScroll(new TimeValue(60000))
	    			        .setQuery(qb)
	    			        .setSize(100).get();
			    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    		}
	    	}
	    	else if(!arr[1].equals("*") && len==4)
	    	{
	    		String cols[]=arr[1].split(",");
	    		String script="{\n"+
	    					" \"_source\":{\n"+
	    					"  \"includes\":[\n"+"";
	    		for(int i=0;i<cols.length;i++)
	    		{
	    			if(i!=cols.length-1)
	    			script+="\""+cols[i]+"\","+"";
	    			else
	    			script+="\""+cols[i]+"\""+"";
	    		}
	    		script+="		]\n"+
    	        		"	},\n"+	
    	                "        \"query\" : {\n" +
    	                "            \"match_all\" : {}\n" +
    	                "        }\n" +
    	                "}";
	    		SearchResponse sr = new SearchTemplateRequestBuilder(client)
		    	        .setScript(script)
		    	        .setScriptType(ScriptType.INLINE)  
		    	        .setRequest(new SearchRequest(arr[3]))                   
		    	        .get()                                             
		    	        .getResponse();   
		    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    	}
	    	else 
	    	{
	    		String cols[]=arr[1].split(",");	
	    		char op=0;
	    		for (char ch : arr[5].toCharArray()) 
				{
				    if (!Character.isDigit(ch) && !Character.isLetter(ch))
				    {
				    	op=ch;
				    	break;
				    }
				}
	    		if(op=='=')
	    		{
	    			String cond[]=arr[5].split("=");
		    		String script="{\n"+
		    					" \"_source\":{\n"+
		    					"  \"includes\":[\n"+"";
		    		for(int i=0;i<cols.length;i++)
		    		{
		    			if(i!=cols.length-1)
		    			script+="\""+cols[i]+"\","+"";
		    			else
		    			script+="\""+cols[i]+"\""+"";
		    		}
		    		script+="		]\n"+
	    	        		"	},\n"+	
	    	                "        \"query\" : {\n" +
	    	                "            \"match\" : {"+
	    	                "\""+cond[0]+"\":\""+cond[1]+"\""+
	    	                "     		}\n" +
	    	                "        }\n" +
	    	                "}";
		    		SearchResponse sr = new SearchTemplateRequestBuilder(client)
			    	        .setScript(script)
			    	        .setScriptType(ScriptType.INLINE)  
			    	        .setRequest(new SearchRequest(arr[3]))                   
			    	        .get()                                             
			    	        .getResponse();   
			    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    		}
	    		else if(op=='<')
	    		{
	    			String cond[]=arr[5].split("<");
	    			String script="{\n"+
	    					" \"_source\":{\n"+
	    					"  \"includes\":[\n"+"";
	    		for(int i=0;i<cols.length;i++)
	    		{
	    			if(i!=cols.length-1)
	    			script+="\""+cols[i]+"\","+"";
	    			else
	    			script+="\""+cols[i]+"\""+"";
	    		}
	    		script+="		]\n"+
    	        		"	},\n"+	
    	                "        \"query\" : {\n" +
    	                "            \"range\" : {\n"+
    	                "			\""+cond[0]+"\":{\n"+
    	                "					\"lt\":"+cond[1]+"\n"+	
    	                "			}\n" +
    	                "        }\n" +
    	                "}\n"+
    	                "}";
	    		SearchResponse sr = new SearchTemplateRequestBuilder(client)
		    	        .setScript(script)
		    	        .setScriptType(ScriptType.INLINE)  
		    	        .setRequest(new SearchRequest(arr[3]))                   
		    	        .get()                                             
		    	        .getResponse();   
		    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    		}
	    		else if(op=='>')
	    		{
	    			String cond[]=arr[5].split(">");
	    			String script="{\n"+
	    					" \"_source\":{\n"+
	    					"  \"includes\":[\n"+"";
	    		for(int i=0;i<cols.length;i++)
	    		{
	    			if(i!=cols.length-1)
	    			script+="\""+cols[i]+"\","+"";
	    			else
	    			script+="\""+cols[i]+"\""+"";
	    		}
	    		script+="		]\n"+
    	        		"	},\n"+	
    	                "        \"query\" : {\n" +
    	                "            \"range\" : {\n"+
    	                "			\""+cond[0]+"\":{\n"+
    	                "					\"gt\":"+cond[1]+"\n"+	
    	                "			}\n" +
    	                "        }\n" +
    	                "}\n"+
    	                "}";
	    		SearchResponse sr = new SearchTemplateRequestBuilder(client)
		    	        .setScript(script)
		    	        .setScriptType(ScriptType.INLINE)  
		    	        .setRequest(new SearchRequest(arr[3]))                   
		    	        .get()                                             
		    	        .getResponse();   
		    	Map<String, Object> m1=null;
					out.println("<html>");
					out.println("</style></head>");
					out.println("<body><fieldset><table class='table'><thead><tr>");
					int i=0;
					for (SearchHit hit : sr.getHits().getHits()) 
					{
						m1=hit.getSourceAsMap();
						if(i==0)
						{
							for(Entry<String,Object> entry1:m1.entrySet())
								  out.println("<th scope='col'>"+entry1.getKey()+"</th>");
							out.println("</tr></thead>");
							i++;
						}
						if(i!=0)
						{
							out.println("<tr>");
							for(Entry<String,Object> entry1:m1.entrySet())
							{
								  out.println("<td id='col1'>"+entry1.getValue()+"</td>");
							}
							out.println("</tr>");
						}
					}
					out.println("</table></fieldset></body></html>");
	    		}
	    	}
	    }
	   else
	    {
	    	out.println("<p>Your specified index is not exists</p>");
	    }
	}
}
