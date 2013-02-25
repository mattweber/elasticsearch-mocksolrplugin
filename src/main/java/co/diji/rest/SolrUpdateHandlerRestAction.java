package co.diji.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.codec.binary.Hex;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
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
import org.elasticsearch.plugin.diji.MockSolrPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentThrowableRestResponse;

import co.diji.solr.SolrResponseWriter;

public class SolrUpdateHandlerRestAction extends BaseRestHandler {

    // content types
    private final String contentTypeFormEncoded = "application/x-www-form-urlencoded";

    // fields in the Solr input document to scan for a document id
    private final String[] idFields = {"id", "docid", "documentid", "contentid", "uuid", "url"};

    // the xml input factory
    private final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    // the response writer
    private final SolrResponseWriter solrResponseWriter = new SolrResponseWriter();

    // Set this flag to false if you want to disable the hashing of id's as they are provided by the Solr Input document
    // , which is the default behaviour.
    // You can configure this by adding 'plugin.diji.MockSolrPlugin.hashIds: false' to elasticsearch.yml
    private final boolean hashIds;

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

        hashIds = settings.getComponentSettings(MockSolrPlugin.class).getAsBoolean("MockSolrPlugin.hashIds", true);
        logger.info("Solr input document id's will " + (hashIds ? "" : "not ") + "be hashed to created ElasticSearch document id's");

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
     * @see org.elasticsearch.rest.RestHandler#handleRequest(org.elasticsearch.rest.RestRequest, org.elasticsearch.rest.RestChannel)
     */
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        // Solr will send commits/optimize as encoded form parameters
        // detect this and just send the response without processing
        // we don't need to do commits with ES
        // TODO: support optimize
        if (request.header("Content-Type").contains(contentTypeFormEncoded)) {
            // find the output writer specified
            // it will be inside the content since we have form encoded
            // parameters
            String qstr = request.content().toUtf8();
            Map<String, String> params = request.params();
            if (params.containsKey("wt")) {
                // output writer already found
            } else if (qstr.contains("wt=javabin")) {
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
            try {
                // create parser for the content
                XMLStreamReader parser = inputFactory.createXMLStreamReader(new StringReader(request.content().toUtf8()));

                // parse the xml
                // we only care about doc and delete tags for now
                boolean stop = false;
                while (!stop) {
                    // get the xml "event"
                    int event = parser.next();
                    switch (event) {
                        case XMLStreamConstants.END_DOCUMENT :
                            // this is the end of the document
                            // close parser and exit while loop
                            parser.close();
                            stop = true;
                            break;
                        case XMLStreamConstants.START_ELEMENT :
                            // start of an xml tag
                            // determine if we need to add or delete a document
                            String currTag = parser.getLocalName();
                            if ("doc".equals(currTag)) {
                                // add a document
                                Map<String, Object> doc = parseXmlDoc(parser);
                                if (doc != null) {
                                    bulkRequest.add(getIndexRequest(doc, request));
                                }
                            } else if ("delete".equals(currTag)) {
                                // delete a document
                                String docid = parseXmlDelete(parser);
                                if (docid != null) {
                                    bulkRequest.add(getDeleteRequest(docid, request));
                                }
                            }
                            break;
                    }
                }
            } catch (Exception e) {
                // some sort of error processing the xml input
                try {
                    logger.error("Error processing xml input", e);
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send error response", e1);
                }
            }
        } else if (handler.equals("javabin")) {
            // JavaBin Content
            try {
                // We will use the JavaBin codec from solrj
                // unmarshal the input to a SolrUpdate request
                JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
                UpdateRequest req = codec.unmarshal(new ByteArrayInputStream(request.content().array()), null);

                // Get the list of documents to index out of the UpdateRequest
                // Add each document to the bulk request
                // convert the SolrInputDocument into a map which will be used as the ES source field
                List<SolrInputDocument> docs = req.getDocuments();
                if (docs != null) {
                    for (SolrInputDocument doc : docs) {
                        bulkRequest.add(getIndexRequest(convertToMap(doc), request));
                    }
                }

                // See if we have any documents to delete
                // if yes, add them to the bulk request
                if (req.getDeleteById() != null) {
                    for (String id : req.getDeleteById()) {
                        bulkRequest.add(getDeleteRequest(id, request));
                    }
                }
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

        // only submit the bulk request if there are index/delete actions
        // it is possible not to have any actions when parsing xml due to the
        // commit and optimize messages that will not generate documents
        if (bulkRequest.numberOfActions() > 0) {
            client.bulk(bulkRequest, new ActionListener<BulkResponse>() {

                // successful bulk request
                public void onResponse(BulkResponse response) {
                    logger.info("Bulk request completed");
                    for (BulkItemResponse itemResponse : response) {
                        if (itemResponse.failed()) {
                            logger.error("Index request failed {index:{}, type:{}, id:{}, reason:{}}",
                                    itemResponse.index(),
                                    itemResponse.type(),
                                    itemResponse.id(),
                                    itemResponse.failure().message());
                        }
                    }
                }

                // failed bulk request
                public void onFailure(Throwable e) {
                    logger.error("Bulk request failed", e);
                }
            });
        }

        // send dummy response to Solr so the clients don't choke
        sendResponse(request, channel);
    }

    /**
     * Sends a dummy response to the Solr client
     * 
     * @param request ES rest request
     * @param channel ES rest channel
     */
    private void sendResponse(RestRequest request, RestChannel channel) {
        // create NamedList with dummy Solr response
        NamedList<Object> solrResponse = new SimpleOrderedMap<Object>();
        NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
        responseHeader.add("status", 0);
        responseHeader.add("QTime", 5);
        solrResponse.add("responseHeader", responseHeader);

        // send the dummy response
        solrResponseWriter.writeResponse(solrResponse, request, channel);
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
        DeleteRequest deleteRequest = new DeleteRequest(index, type, getId(id));
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
     * @param doc the Solr input document to convert
     * @param request the ES rest request
     * @return the ES index request object
     */
    private IndexRequest getIndexRequest(Map<String, Object> doc, RestRequest request) {
        // get the index and type we want to index the document in
        final String index = request.hasParam("index") ? request.param("index") : "solr";
        final String type = request.hasParam("type") ? request.param("type") : "docs";

        // Get the id from request or if not available generate an id for the document
        String id = request.hasParam("id") ? request.param("id") : getIdForDoc(doc);

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
     * Generates document id. A Solr document id may not be a valid ES id, so we attempt to find the Solr document id and convert it
     * into a valid ES document id. We keep the original Solr id so the document can be found and deleted later if needed.
     * 
     * We check for Solr document id's in the following fields: id, docid, documentid, contentid, uuid, url
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

        // always store the id back into the "id" field
        // so we can get it back in results
        doc.put("id", id);

        // return the id which is the md5 of either the
        // random uuid or id found in the input document.
        return getId(id);
    }

    /**
     * Return the given id or a hashed version thereof, based on the plugin configuration
     * 
     * @param id
     * @return
     */

    private final String getId(String id) {
        return hashIds ? getMD5(id) : id;
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

    /**
     * Reads a SolrXML document into a map of fields
     * 
     * @param parser the xml parser
     * @return the document as a map
     * @throws XMLStreamException
     */
    private Map<String, Object> parseXmlDoc(XMLStreamReader parser) throws XMLStreamException {
        Map<String, Object> doc = new HashMap<String, Object>();
        StringBuilder buf = new StringBuilder();
        String name = null;
        boolean stop = false;
        // infinite loop until we are done parsing the document or an error occurs
        while (!stop) {
            int event = parser.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT :
                    buf.setLength(0);
                    String localName = parser.getLocalName();
                    // we are looking for field elements only
                    if (!"field".equals(localName)) {
                        logger.warn("unexpected xml tag /doc/" + localName);
                        doc = null;
                        stop = true;
                    }

                    // get the name attribute of the field
                    String attrName = "";
                    String attrVal = "";
                    for (int i = 0; i < parser.getAttributeCount(); i++) {
                        attrName = parser.getAttributeLocalName(i);
                        attrVal = parser.getAttributeValue(i);
                        if ("name".equals(attrName)) {
                            name = attrVal;
                        }
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT :
                    if ("doc".equals(parser.getLocalName())) {
                        // we are done parsing the doc
                        // break out of loop
                        stop = true;
                    } else if ("field".equals(parser.getLocalName())) {
                        // put the field value into the map
                        // handle multiple values by putting them into a list
                        if (doc.containsKey(name) && (doc.get(name) instanceof List)) {
                            List<String> vals = (List<String>) doc.get(name);
                            vals.add(buf.toString());
                            doc.put(name, vals);
                        } else if (doc.containsKey(name)) {
                            List<String> vals = new ArrayList<String>();
                            vals.add((String) doc.get(name));
                            vals.add(buf.toString());
                            doc.put(name, vals);
                        } else {
                            doc.put(name, buf.toString());
                        }
                    }
                    break;
                case XMLStreamConstants.SPACE :
                case XMLStreamConstants.CDATA :
                case XMLStreamConstants.CHARACTERS :
                    // save all text data
                    buf.append(parser.getText());
                    break;
            }
        }

        // return the parsed doc
        return doc;
    }

    /**
     * Parse the document id out of the SolrXML delete command
     * 
     * @param parser the xml parser
     * @return the document id to delete
     * @throws XMLStreamException
     */
    private String parseXmlDelete(XMLStreamReader parser) throws XMLStreamException {
        String docid = null;
        StringBuilder buf = new StringBuilder();
        boolean stop = false;
        // infinite loop until we get docid or error
        while (!stop) {
            int event = parser.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT :
                    // we just want the id node
                    String mode = parser.getLocalName();
                    if (!"id".equals(mode)) {
                        logger.warn("unexpected xml tag /delete/" + mode);
                        stop = true;
                    }
                    buf.setLength(0);
                    break;
                case XMLStreamConstants.END_ELEMENT :
                    String currTag = parser.getLocalName();
                    if ("id".equals(currTag)) {
                        // we found the id
                        docid = buf.toString();
                    } else if ("delete".equals(currTag)) {
                        // done parsing, exit loop
                        stop = true;
                    } else {
                        logger.warn("unexpected xml tag /delete/" + currTag);
                    }
                    break;
                case XMLStreamConstants.SPACE :
                case XMLStreamConstants.CDATA :
                case XMLStreamConstants.CHARACTERS :
                    // save all text data (this is the id)
                    buf.append(parser.getText());
                    break;
            }
        }

        // return the extracted docid
        return docid;
    }
}
