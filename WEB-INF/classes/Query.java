import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
public class Query extends HttpServlet
{
	public void doGet(HttpServletRequest request,HttpServletResponse response)throws ServletException,IOException
	{
			response.setContentType("text/html");
			PrintWriter out=response.getWriter();
			String sql=request.getParameter("id");
			out.println("<html>");
			String sp[]=sql.split(" ");
			if(sp[0].equalsIgnoreCase("insert"))
			{
				try{
					Process.insert(sql,out);
				}
				catch(Exception e){
					out.println(e);
				}
			}
			if(sp[0].equalsIgnoreCase("update"))
			{
					try{
					Update.update(out,sql);
					}
					catch(Exception e){
						out.println("<p>"+e+"</p>");
					}
			}
			if(sp[0].equalsIgnoreCase("create"))
			{
				try{
				Create.create(sql,out);
				}
				catch(Exception e)
				{
					out.println("<p>"+e+"</p>");
				}
			}
			if(sp[0].equalsIgnoreCase("delete")||sp[0].equalsIgnoreCase("drop"))
			{
				try{
					Process.delete(sql,out);
				}
				catch(Exception e)
				{
					out.println("<p>"+e+"</p>");
				}
			}
			if(sp[0].equalsIgnoreCase("select"))
			{
				try{
					Search.retrieve(sql,out);
				}
				catch(Exception e)
				{
					System.out.println(e);
				}
			}
	}
}