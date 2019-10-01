import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.io.*;
public class Create 
{
	public static boolean isIndexExist(Client client, String index) throws Exception {
		return client.admin().indices().prepareExists(index).get().isExists();
	}
	@SuppressWarnings("deprecation")
	public static void create(String sql,PrintWriter out) throws Exception
	{
		@SuppressWarnings({ "resource", "unchecked" })
		TransportClient client=new PreBuiltTransportClient(Settings.EMPTY)
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		String arr[]=sql.split(" ");
		int len=arr.length;
		boolean exists=isIndexExist(client,arr[2]);
		if(!exists)
		{
			if(arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
			{
				if(len==3) 
				{
					client.admin().indices().prepareCreate(arr[2])
			        .setSettings(Settings.builder()             
			                .put("index.number_of_shards", 3)
			                .put("index.number_of_replicas", 2)
			        		)
			        .get();
					out.println("<p>Index created successfully</p>");
				}
				else
				{
					client.admin().indices().prepareCreate(arr[2])
			        .setSettings(Settings.builder()             
			                .put("index.number_of_shards", 3)
			                .put("index.number_of_replicas", 2)
			        		)
			        .get();
					out.println("<p>Index created successfully</p>");
					String pro[]=arr[4].split(",");
					Map<String, Object> properties = new HashMap<String, Object>();
					for(int i=0;i<pro.length;i++)
					{
						String field[]=pro[i].split("=");
						Map<String, Object> fields = new HashMap<String, Object>();
						fields.put("type",field[1]);
						properties.put(field[0], fields);	
					}
					Map<String, Object> mapping = new HashMap<String, Object>();
					mapping.put("properties", properties);
					client.admin().indices().preparePutMapping(arr[2])   
			        .setType("details")                                
			        .setSource(mapping)
			        .get();
					out.println("<p>Index mapping is also success</p>");
				}
			}
			else
			{
				out.println("<p>Sql Syntax error</p>");
			}
		}
		else
		{
			out.println("<p>Index is already exists</p>");
		}
	}
}
		
