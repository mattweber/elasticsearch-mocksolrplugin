package co.diji.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

/**
 * Class to handle sending responses to Solr clients.  
 * Supports xml and javabin formats.
 *
 */
public class SolrResponseWriter {
	protected final ESLogger logger;

	private final String contentTypeOctet = "application/octet-stream";
	private final String contentTypeXml = "application/xml; charset=UTF-8";

	public SolrResponseWriter() {
		this.logger = Loggers.getLogger(SolrResponseWriter.class);
	}

	/**
	 * Serializes the NamedList in the specified output format and sends it to the Solr Client.
	 * 
	 * @param obj the NamedList response to serialize
	 * @param request the ES RestRequest
	 * @param channel the ES RestChannel
	 */
	public void writeResponse(NamedList<Object> obj, RestRequest request, RestChannel channel) {
		// determine what kind of output writer the Solr client is expecting
		final String wt = request.hasParam("wt") ? request.param("wt").toLowerCase() : "xml";

		// determine what kind of response we need to send
		if (wt.equals("xml")) {
			writeXmlResponse(obj, channel);
		} else if (wt.equals("javabin")) {
			writeJavaBinResponse(obj, channel);
		}
	}

	/**
	 * Write the response object in JavaBin format.
	 * 
	 * @param obj the response object
	 * @param channel the ES RestChannel
	 */
	private void writeJavaBinResponse(NamedList<Object> obj, RestChannel channel) {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();

		// try to marshal the data
		try {
			new JavaBinCodec().marshal(obj, bo);
		} catch (IOException e) {
			logger.error("Error writing JavaBin response", e);
		}

		// send the response
		channel.sendResponse(new BytesRestResponse(bo.toByteArray(), contentTypeOctet));
	}

	private void writeXmlResponse(NamedList<Object> obj, RestChannel channel) {
		Writer writer = new StringWriter();

		// try to serialize the data to xml
		try {
			writer.write(XMLWriter.XML_START1);
			writer.write(XMLWriter.XML_START2_NOSCHEMA);

			// initialize the xml writer
			XMLWriter xw = new XMLWriter(writer);

			// loop though each object and convert it to xml
			int sz = obj.size();
			for (int i = 0; i < sz; i++) {
				xw.writeVal(obj.getName(i), obj.getVal(i));
			}

			writer.write("\n</response>\n");
			writer.close();
		} catch (IOException e) {
			logger.error("Error writing XML response", e);
		}

		// send the response
		channel.sendResponse(new BytesRestResponse(writer.toString().getBytes(), contentTypeXml));
	}
}