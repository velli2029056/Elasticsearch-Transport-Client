import java.net.InetAddress;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
public class Completion extends HttpServlet
{
	@SuppressWarnings({ "resource", "unchecked" })
	public static void complete(String command,PrintWriter out)
	{
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			 CompletionSuggestionBuilder suggestBuilder = new CompletionSuggestionBuilder("suggest"); 
		     suggestBuilder.size(100000)
		                   .prefix(command, Fuzziness.ONE)
		                   .analyzer("standard");
		 
		     SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		     sourceBuilder.suggest(new SuggestBuilder().addSuggestion("suggest", suggestBuilder));
		     SearchResponse sr= client.prepareSearch("query")
		    		.setSource(sourceBuilder)
 			        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.DESC)
 			        .setScroll(new TimeValue(60000))
 			        .setSize(100000).get();
			 Suggest suggest = sr.getSuggest();
		     Suggestion<Entry<Option>> suggestion = suggest.getSuggestion("suggest");
		 	for(Entry<Option> entry: suggestion.getEntries()) {
		 	      for (Option option: entry.getOptions()) {
		 	       out.println("<option>"+option.getText().toString()+"</option>");
		 	      }
		 	}
		}	
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	public void doGet(HttpServletRequest request,HttpServletResponse response)throws ServletException,IOException
	{
		PrintWriter out=response.getWriter();
		String suggest=request.getParameter("q");
		Completion.complete(suggest,out);
	}
}