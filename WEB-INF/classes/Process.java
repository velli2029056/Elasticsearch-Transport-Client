import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
public class Process
{
	public static boolean isIndexExist(Client client, String index) throws Exception {
		return client.admin().indices().prepareExists(index).get().isExists();
	}
	public static void insert(String sql,PrintWriter out)throws Exception
	{
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			//System.out.println("connected");
		}
		catch(UnknownHostException e)
		{
			System.out.println(e);
		}
		//String sql="insert into employee1 (firstname,lastname,address,id,company,location,salary,experience,designation,password) values (Suresh,Murugasamy,Kunnathur,pt-3126,zohocorporation,chennai,15000,5,developer,suresh20031999)";
		String arr[]=sql.split(" ");
	    boolean exists=isIndexExist(client,arr[2]);
	    if(exists)
	    {
			if(arr[0].equalsIgnoreCase("insert") && arr[1].equalsIgnoreCase("into") && arr[3].equalsIgnoreCase("values"))
			{
				String values[]=arr[4].split("\\(|\\)");
				ArrayList<String>[] listvalues = new ArrayList[values.length];
				for(int i=0;i<values.length/2;i++)
					listvalues[i]=new ArrayList<String>();
				for(int i=0,j=0;i<values.length;i++)
				{
					if(i%2!=0)
					{
						String vals[]=values[i].split(",");
						for(String s:vals)
							listvalues[j].add(s);
						j++;
					}
				}
				for(int i=0;i<values.length/2;i++)
				{
					if(listvalues[i].size()==10)
					{
						String json= "{" +
							"\"firstname\":\""+listvalues[i].get(0)+"\","+
							"\"lastname\":\""+listvalues[i].get(1)+"\","+
							"\"address\":\""+listvalues[i].get(2)+"\","+
							"\"id\":\""+listvalues[i].get(3)+"\","+
							"\"company\":\""+listvalues[i].get(4)+"\","+
							"\"location\":\""+listvalues[i].get(5)+"\","+
							"\"salary\":\""+listvalues[i].get(6)+"\","+
							"\"experience\":\""+listvalues[i].get(7)+"\","+
							"\"designation\":\""+listvalues[i].get(8)+"\","+
							"\"password\":\""+listvalues[i].get(9)+"\""+
							"}";
						IndexResponse response = client.prepareIndex("employee", "detai")
						        .setSource(json, XContentType.JSON)
						        .get();
						if(response!=null)
							out.println("<p>Data inserted successfully="+listvalues[i].get(0)+"</p>");
						else
							out.println("<p>Your data not inserted</p>");
					}
					else
					{
						out.println("<p>Some column missing</p>");
					}
				}
			}
			else if(arr[0].equalsIgnoreCase("insert") && arr[1].equalsIgnoreCase("into"))
			{
				String values[]=arr[3].split("\\(|\\)");
				ArrayList<String>[] columns = new ArrayList[values.length];
				columns[0]=new ArrayList<String>();
				for(String s1:values)
				{
					String vals[]=s1.split(",");
					for(int j=0;j<vals.length;j++){
							if(!vals[j].equalsIgnoreCase(""))
							columns[0].add(vals[j]);
					}
				}
				String values1[]=arr[5].split("\\(|\\)");
				ArrayList<String>[] listvalues = new ArrayList[values1.length];
				for(int i=0;i<values1.length/2;i++)
					listvalues[i]=new ArrayList<String>();
				for(int i=0,j=0;i<values1.length;i++)
				{
					if(i%2!=0)
					{
						String vals[]=values1[i].split(",");
						for(String s:vals)
							listvalues[j].add(s);
						j++;
					}
				}
				for(int i=0;i<values1.length/2;i++)
				{
					//System.out.println(listvalues[i].size()+"="+columns[0].size());
					if(listvalues[i].size()==columns[0].size())
					{
						String json="",json1="";
						for(int j=0;j<listvalues[i].size();j++)
						{
							if(j==0)
							json = "{" +
							"\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\","+"";
							else if(j!=listvalues[i].size()-1)
							{
							json="\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\","+"";
							}
							else
							{
								json="\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\""+
									"}";
							}
							json1+=json;
						}
						IndexResponse response = client.prepareIndex("employee", "details")
						        .setSource(json1, XContentType.JSON)
						        .get();
						if(response!=null)
							out.println("<p>Data inserted successfully</p>");
					}
					else
					{
						out.println("<p>Some column missing</p>");
					}
				}
			}
			client.close();
	    }
	   else
	    {
		  out.println("<p>Index not exists</p>");
	    }
	}
	public static void delete(String query,PrintWriter out)throws Exception
	{
		TransportClient client=null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		}
		catch(UnknownHostException e)
		{
			out.println("<p>"+e+"</p>");
		}
		String arr[]=query.split(" ");
		int len=arr.length;
		if(arr[0].equalsIgnoreCase("delete") && arr[1].equalsIgnoreCase("from"))
		{
			
			boolean exists=isIndexExist(client,arr[2]);
			if(exists)
			{
				if(arr[3].equalsIgnoreCase("where"))
				{
					Map<String,String> m=new HashMap<String,String>();
					char op = 0;
					for (char ch : arr[4].toCharArray()) 
					{
					    if (!Character.isDigit(ch) && !Character.isLetter(ch))
					    {
					    	op=ch;
					    }
					}
					if(op=='=')
						{
							String temp[]=arr[4].split("=");
						try {
								BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
								    .filter(QueryBuilders.matchQuery(temp[0], temp[1])) 
								    .source(arr[2])                                  
								    .get();                                             
								long deleteddocuments = response.getDeleted();
								if(response!=null)
								out.println("<p>Your details should be deleted successfully ,No of document deleted is:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
						else if(op=='<')
						{
							String temp[]=arr[4].split("<");
						try {
							 BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
								    .filter(QueryBuilders.rangeQuery(temp[0]).lt(temp[1])) 
								    .source(arr[2])                                  
								    .get();                                             
								long deleteddocuments = response.getDeleted();
							out.println("<p>Your documents deleted successfully,No of documents deleted:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
						else if(op=='>')
						{
							String temp[]=arr[4].split(">");
						try {
							BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
								    .filter(QueryBuilders.rangeQuery(temp[0]).gt(temp[1])) 
								    .source(arr[2])                                  
								    .get();                                             
								long deleteddocuments= response.getDeleted();
								out.println("<p>Your documents deleted successfully,No of documents deleted:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
				}
				else
				{
						try
						{	
							BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
								    .filter(QueryBuilders.matchAllQuery()) 
								    .source(arr[2])                                  
								    .get();                                             
								long deleteddocuments = response.getDeleted();   
							out.println("<p>Your details should be deleted successfully="+deleteddocuments+"</p>");
						}
						catch(Exception e)
						{
							out.println("<p>"+e+"</p>");
						}
				}
			}
			else
			out.println("<p>"+arr[2]+" index is not exist in the elastic search cluster</p>");
			}
		else if(arr[0].equalsIgnoreCase("drop") && arr[1].equalsIgnoreCase("table") && len>=2)
		{
			boolean exists=isIndexExist(client,arr[2]);
			if(exists)
			{
				try{
				DeleteIndexRequest request=new DeleteIndexRequest(arr[2]);
				DeleteIndexResponse response=client.admin().indices().delete(request).actionGet();
				if(response!=null)
				out.println("<p>Your index deleted</p>");
				}
				catch(Exception e)
				{
					out.println("<p>"+e+"</p>");
				}
			}
			else
			{
				out.println("<p>Your specified index is not present</p>");
			}
		}
		client.close();
	}
}