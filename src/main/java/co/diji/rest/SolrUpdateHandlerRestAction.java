package co.diji.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.binary.Hex;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentThrowableRestResponse;

public class SolrUpdateHandlerRestAction extends BaseRestHandler {

	// dummy xml response to send so Solr clients don't blow up without a response
	private final String xmlResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><response><lst name=\"responseHeader\"><int name=\"status\">0</int><int name=\"QTime\">5</int></lst></response>";

	// output content types
	private final String contentTypeXml = "application/xml; charset=UTF-8";
	private final String contentTypeFormEncoded = "application/x-www-form-urlencoded; charset=UTF-8";
	private final String contentTypeOctet = "application/octet-stream";

	// fields in the Solr input document to scan for a document id
	private final String[] idFields = { "id", "docid", "documentid", "contentid", "uuid", "url" };

	/**
	 * Rest actions that mock Solr update handlers
	 * 
	 * @param settings ES settings
	 * @param client ES client
	 * @param restController ES rest controller
	 */
	@Inject
	public SolrUpdateHandlerRestAction(Settings settings, Client client, RestController restController) {
		super(settings, client);

		// register update handlers
		// specifying and index and type is optional
		restController.registerHandler(RestRequest.Method.POST, "/_solr/update", this);
		restController.registerHandler(RestRequest.Method.POST, "/_solr/update/{handler}", this);
		restController.registerHandler(RestRequest.Method.POST, "/{index}/_solr/update", this);
		restController.registerHandler(RestRequest.Method.POST, "/{index}/_solr/update/{handler}", this);
		restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_solr/update", this);
		restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_solr/update/{handler}", this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.elasticsearch.rest.RestHandler#handleRequest(org.elasticsearch.rest.RestRequest, org.elasticsearch.rest.RestChannel)
	 */
	public void handleRequest(final RestRequest request, final RestChannel channel) {
		// Solr will send commits/optimize as encoded form parameters
		// detect this and just send the response without processing
		// we don't need to do commits with ES
		// TODO: support optimize
		if (request.header("Content-Type").equals(contentTypeFormEncoded)) {
			// find the output writer specified
			// it will be inside the content since we have form encoded
			// parameters
			String qstr = request.contentAsString();
			Map<String, String> params = request.params();
			if (qstr.contains("wt=javabin")) {
				params.put("wt", "javabin");
			} else if (qstr.contains("wt=xml")) {
				params.put("wt", "xml");
			} else {
				// we have an output writer we don't support yet
				// put junk into wt so sendResponse detects unknown wt
				logger.warn("Unknown wt for commit/optimize");
				params.put("wt", "invalid");
			}

			// send response to Solr
			sendResponse(request, channel);
			return;
		}

		// get the type of Solr update handler we want to mock, default to xml
		final String handler = request.hasParam("handler") ? request.param("handler").toLowerCase() : "xml";

		// Requests are typically sent to Solr in batches of documents
		// We can copy that by submitting batch requests to Solr
		BulkRequest bulkRequest = Requests.bulkRequest();

		// parse and handle the content
		if (handler.equals("xml")) {
			// XML Content
			// TODO: support this
			logger.warn("xml input not supported yet");
			sendResponse(request, channel);
		} else if (handler.equals("javabin")) {
			// JavaBin Content
			try {
				// We will use the JavaBin codec from solrj
				// unmarshal the input to a SolrUpdate request
				JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
				UpdateRequest req = codec.unmarshal(new ByteArrayInputStream(request.contentByteArray()), null);

				// Get the list of documents to index out of the UpdateRequest
				// Add each document to the bulk request
				List<SolrInputDocument> docs = req.getDocuments();
				if (docs != null) {
					for (SolrInputDocument doc : docs) {
						bulkRequest.add(getIndexRequest(doc, request));
					}
				}

				// See if we have any documents to delete
				// if yes, add them to the bulk request
				if (req.getDeleteById() != null) {
					for (String id : req.getDeleteById()) {
						bulkRequest.add(getDeleteRequest(id, request));
					}
				}

				// Execute the bulk request
				client.bulk(bulkRequest, new ActionListener<BulkResponse>() {

					// successful bulk request
					public void onResponse(BulkResponse response) {
						logger.info("Bulk request completed");
						for (BulkItemResponse itemResponse : response) {
							if (itemResponse.failed()) {
								logger.error("Index request failed {index:{}, type:{}, id:{}, reason:{}}", itemResponse.index(), itemResponse.type(), itemResponse.id(), itemResponse.failure().message());
							}
						}
					}

					// failed bulk request
					public void onFailure(Throwable e) {
						logger.error("Bulk request failed {reason:{}}", e);
					}
				});

				// send dummy response to Solr so the clients don't choke
				sendResponse(request, channel);

			} catch (Exception e) {
				// some sort of error processing the javabin input
				try {
					logger.error("Error processing javabin input", e);
					channel.sendResponse(new XContentThrowableRestResponse(request, e));
				} catch (IOException e1) {
					logger.error("Failed to send error response", e1);
				}
			}
		}
	}

	/**
	 * Sends a dummy response to the Solr client based on the specified Solr
	 * output writer.
	 * 
	 * @param request ES rest request
	 * @param channel ES rest channel
	 */
	private void sendResponse(RestRequest request, RestChannel channel) {
		// determine what kind of output writer the Solr client is expecting
		final String wt = request.hasParam("wt") ? request.param("wt").toLowerCase() : "xml";

		// determine what kind of response we need to send
		if (wt.equals("xml")) {
			// XML response, just send dummy xml response
			channel.sendResponse(new BytesRestResponse(xmlResponse.getBytes(), contentTypeXml));
		} else if (wt.equals("javabin")) {
			// JavaBin response
			// create NamedList with dummy Solr response
			// use the JavaBin codec to marshal the dummy data
			NamedList<Object> solrResponse = new SimpleOrderedMap<Object>();
			NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
			responseHeader.add("status", 0);
			responseHeader.add("QTime", 5);
			solrResponse.add("responseHeader", responseHeader);
			ByteArrayOutputStream bo = new ByteArrayOutputStream();

			// try to marshal the dummy data
			try {
				new JavaBinCodec().marshal(solrResponse, bo);
			} catch (IOException e) {
				logger.error("Error marshaling solrResponse");
			}

			// send the response
			channel.sendResponse(new BytesRestResponse(bo.toByteArray(), contentTypeOctet));
		} else {
			logger.warn("Unknown output writer: " + wt);
		}
	}

	/**
	 * Generates an ES DeleteRequest object based on the Solr document id
	 * 
	 * @param id the Solr document id
	 * @param request the ES rest request
	 * @return the ES delete request
	 */
	private DeleteRequest getDeleteRequest(String id, RestRequest request) {
		// get the index and type we want to execute this delete request on
		final String index = request.hasParam("index") ? request.param("index") : "solr";
		final String type = request.hasParam("type") ? request.param("type") : "docs";

		// create the delete request object
		DeleteRequest deleteRequest = new DeleteRequest(index, type, getMD5(id));
		deleteRequest.parent(request.param("parent"));

		// TODO: this was causing issues, do we need it?
		// deleteRequest.version(RestActions.parseVersion(request));
		// deleteRequest.versionType(VersionType.fromString(request.param("version_type"),
		// deleteRequest.versionType()));

		deleteRequest.routing(request.param("routing"));

		return deleteRequest;
	}

	/**
	 * Converts a SolrInputDocument into an ES IndexRequest
	 * 
	 * @param solrDoc the Solr input document to convert
	 * @param request the ES rest request
	 * @return the ES index request object
	 */
	private IndexRequest getIndexRequest(SolrInputDocument solrDoc, RestRequest request) {
		// get the index and type we want to index the document in
		final String index = request.hasParam("index") ? request.param("index") : "solr";
		final String type = request.hasParam("type") ? request.param("type") : "docs";

		// convert the SolrInputDocument into a map which will be used as the ES source field
		Map<String, Object> doc = convertToMap(solrDoc);

		// generate an id for the document
		String id = getIdForDoc(doc);

		// create an IndexRequest for this document
		IndexRequest indexRequest = new IndexRequest(index, type, id);
		indexRequest.routing(request.param("routing"));
		indexRequest.parent(request.param("parent"));
		indexRequest.source(doc);
		indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
		indexRequest.refresh(request.paramAsBoolean("refresh", indexRequest.refresh()));

		// TODO: this caused issues, do we need it?
		// indexRequest.version(RestActions.parseVersion(request));
		// indexRequest.versionType(VersionType.fromString(request.param("version_type"),
		// indexRequest.versionType()));

		indexRequest.percolate(request.param("percolate", null));
		indexRequest.opType(IndexRequest.OpType.INDEX);

		// TODO: force creation of index, do we need it?
		// indexRequest.create(true);

		String replicationType = request.param("replication");
		if (replicationType != null) {
			indexRequest.replicationType(ReplicationType.fromString(replicationType));
		}

		String consistencyLevel = request.param("consistency");
		if (consistencyLevel != null) {
			indexRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
		}

		// we just send a response, no need to fork
		indexRequest.listenerThreaded(true);

		// we don't spawn, then fork if local
		indexRequest.operationThreaded(true);

		return indexRequest;
	}

	/**
	 * Generates document id. A Solr document id may not be a valid ES id, so we
	 * attempt to find the Solr document id and convert it into a valid ES
	 * document id. We keep the original Solr id so the document can be found
	 * and deleted later if needed.
	 * 
	 * We check for Solr document id's in the following fields: id, docid,
	 * documentid, contentid, uuid, url
	 * 
	 * If no id is found, we generate a random one.
	 * 
	 * @param doc the input document
	 * @return the generated document id
	 */
	private String getIdForDoc(Map<String, Object> doc) {
		// start with a random id
		String id = UUID.randomUUID().toString();

		// scan the input document for an id
		for (String idField : idFields) {
			if (doc.containsKey(idField)) {
				id = doc.get(idField).toString();
				break;
			}
		}

		// return the id which is the md5 of either the
		// random uuid or id found in the input document.
		return getMD5(id);
	}

	/**
	 * Calculates the md5 hex digest of the given input string
	 * 
	 * @param input the string to md5
	 * @return the md5 hex digest
	 */
	private String getMD5(String input) {
		String id = "";
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			id = new String(Hex.encodeHex(md.digest(input.getBytes())));
		} catch (NoSuchAlgorithmException e) {
			id = input;
		}

		return id;
	}

	/**
	 * Converts a SolrInputDocument into a Map
	 * 
	 * @param doc the SolrInputDocument to convert
	 * @return the input document as a map
	 */
	private Map<String, Object> convertToMap(SolrInputDocument doc) {
		// create the Map we will put the fields in
		Map<String, Object> newDoc = new HashMap<String, Object>();

		// loop though all the fields and insert them into the map
		Collection<SolrInputField> fields = doc.values();
		if (fields != null) {
			for (SolrInputField field : fields) {
				newDoc.put(field.getName(), field.getValue());
			}
		}

		return newDoc;
	}
}
