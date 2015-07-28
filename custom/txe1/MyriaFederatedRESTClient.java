import java.io.IOException;
import java.net.URLEncoder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
 
public class MyriaFederatedRESTClient {
    public static void main(String[] args) throws IOException {
	String coordinator = "node-037";
	int port = 8090;
	String query = "T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);";

	System.out.println(execute(coordinator, port, query));
    }

    public static String execute(String coordinator, int port, String query) throws IOException {
	Client client = Client.create();
 
	// NOTE: may have to move this to the request body if query gets too long
	WebResource webResource = client
	    .resource(String.format("http://%s:%d/execute?language=federated&query=%s", 
				    coordinator, port, URLEncoder.encode(query, "UTF-8")));
	ClientResponse response = webResource
            .accept("application/json")
	    .post(ClientResponse.class);

	System.out.println(response.getStatus());
	return response.getEntity(String.class);
    }
}
