import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.io.*;
public class Update 
{	
	public static boolean isIndexExist(Client client, String index) throws Exception {
		return client.admin().indices().prepareExists(index).get().isExists();
	}
	@SuppressWarnings({ "unchecked", "resource" })
	public static void update(PrintWriter out,String sql) throws Exception
	{
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			out.println("<p>connected</p>");
		}
		catch(UnknownHostException e)
		{
			out.println("<p>"+e+"</p>");
		}
		String arr[]=sql.split(" ");
		boolean exists=isIndexExist(client,arr[1]);
		if(exists)
		{
			if(arr[0].equalsIgnoreCase("update") && arr[2].equalsIgnoreCase("set") && arr[4].equalsIgnoreCase("where"))
			{
				char op = 0;
				for (char ch : arr[5].toCharArray()) 
				{
				    if (!Character.isDigit(ch) && !Character.isLetter(ch))
				    {
				    	op=ch;
				    	break;
				    }
				}
				String values[]=arr[3].split(",");
				Map<String,String> m=new HashMap<String,String>();
				for(int i=0;i<values.length;i++)
				{
					String vals[]=values[i].split("=");
					m.put(vals[0],vals[1]);
				}
				if(op=='=')
				{
					String condition[]=arr[5].split("=");
					BulkByScrollResponse response=null;
					for(Entry<String, String> entry:m.entrySet())
					{
						UpdateByQueryRequestBuilder updateByQuery =UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
						updateByQuery.source(arr[1])
					 	.size(500)
					 	.script(new Script(ScriptType.INLINE,"painless","if(ctx._source."+condition[0]+"=="+condition[1]+"){ctx._source."+entry.getKey()+"="+entry.getValue()+";}",Collections.EMPTY_MAP));
						 response = updateByQuery.get();
					}
					if(response!=null)
					{
						out.println("<p>Your details updated successfully<p>");
					}
					else
					{
						out.println("<p>Your details should not updated<p>");
					}
				}
				else if(op=='<')
				{
				try {
						String condition[]=arr[5].split("<");
						for(Entry<String, String> entry:m.entrySet())
						{
						UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
						String column=entry.getKey();
						String value=entry.getValue();
						updateByQuery.source(arr[1])
						    .script(new Script(ScriptType.INLINE,"painless","ctx._source."+column+"="+value+"" ,Collections.<String, Object>emptyMap())) 				
							.filter(QueryBuilders.rangeQuery(condition[0]).lte(condition[1]));
							BulkByScrollResponse response = updateByQuery.get();
							if(response!=null)
							System.out.println("<p>Your details should be updated successfully</p>");
							else
							System.out.println("<p>Your details should not updated successfully</p>");
						}
					}
					catch(Exception e)
					{
						out.println("<p>"+e+"</p>");
					}
				}
				else if(op=='>')
				{
					try {
						String condition[]=arr[5].split(">");
						//out.println(condition[0]+"="+condition[1]);
						UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
						for(Entry<String, String> entry:m.entrySet())
						{
						String column=entry.getKey();
						String value=entry.getValue();
						updateByQuery.source(arr[1])
						    .script(new Script(ScriptType.INLINE,"painless","ctx._source."+column+"="+value+";" ,Collections.<String, Object>emptyMap()))
							.filter(QueryBuilders.rangeQuery(condition[0]).gte(condition[1]));
							BulkByScrollResponse response = updateByQuery.get();
							if(response!=null)
							out.println("<p>Your details should be updated successfully</p>");
							else
							out.println("<p>Your details should not updated successfully</p>");
						}
					}
					catch(Exception e)
					{
						out.println("<p>"+e+"</p>");
					}
				}
			}
		}
		else
		{
			out.println("<p>index is not exists</p>");
		}
	client.close();
	}
}
