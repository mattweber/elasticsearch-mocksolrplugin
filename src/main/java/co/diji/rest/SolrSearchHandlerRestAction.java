package co.diji.rest;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import co.diji.solr.SolrResponseWriter;

public class SolrSearchHandlerRestAction extends BaseRestHandler {

	// handles solr response formats
	private final SolrResponseWriter solrResponseWriter = new SolrResponseWriter();

	// regex and date format to detect ISO8601 date formats
	private final Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z");;
	private final DateTimeFormatter dateFormat = ISODateTimeFormat.dateOptionalTimeParser();

	/**
	 * Rest actions that mocks the Solr search handler
	 * 
	 * @param settings ES settings
	 * @param client ES client
	 * @param restController ES rest controller
	 */
	@Inject
	public SolrSearchHandlerRestAction(Settings settings, Client client, RestController restController) {
		super(settings, client);

		// register search handler
		// specifying and index and type is optional
		restController.registerHandler(RestRequest.Method.GET, "/_solr/select", this);
		restController.registerHandler(RestRequest.Method.GET, "/{index}/_solr/select", this);
		restController.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_solr/select", this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.elasticsearch.rest.RestHandler#handleRequest(org.elasticsearch.rest.RestRequest, org.elasticsearch.rest.RestChannel)
	 */
	public void handleRequest(final RestRequest request, final RestChannel channel) {
		// generate the search request
		SearchRequest searchRequest = getSearchRequest(request);
		searchRequest.listenerThreaded(false);

		// execute the search
		client.search(searchRequest, new ActionListener<SearchResponse>() {
			@Override
			public void onResponse(SearchResponse response) {
				try {
					// write response
					solrResponseWriter.writeResponse(createSearchResponse(request, response), request, channel);
				} catch (Exception e) {
					onFailure(e);
				}
			}

			@Override
			public void onFailure(Throwable e) {
				try {
					logger.error("Error processing executing search", e);
					channel.sendResponse(new XContentThrowableRestResponse(request, e));
				} catch (IOException e1) {
					logger.error("Failed to send failure response", e1);
				}
			}
		});
	}

	/**
	 * Generates an ES SearchRequest based on the Solr Input Parameters
	 * 
	 * @param request the ES RestRequest
	 * @return the generated ES SearchRequest
	 */
	private SearchRequest getSearchRequest(RestRequest request) {
		// get solr search parameters
		String q = request.param("q");
		int start = request.paramAsInt("start", 0);
		int rows = request.paramAsInt("rows", 10);
		String fl = request.param("fl");
		String sort = request.param("sort");

		// get index and type we want to search against
		final String index = request.hasParam("index") ? request.param("index") : "solr";
		final String type = request.hasParam("type") ? request.param("type") : "docs";

		// build the query
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		if (q != null) {
			QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(q);
			searchSourceBuilder.query(queryBuilder);
		}

		searchSourceBuilder.from(start);
		searchSourceBuilder.size(rows);

		// parse fl into individual fields
		// solr supports separating by comma or spaces
		if (fl != null) {
			if (!Strings.hasText(fl)) {
				searchSourceBuilder.noFields();
			} else {
				searchSourceBuilder.fields(fl.split("\\s|,"));
			}
		}

		// handle sorting
		if (sort != null) {
			String[] sorts = Strings.splitStringByCommaToArray(sort);
			for (int i = 0; i < sorts.length; i++) {
				String sortStr = sorts[i].trim();
				int delimiter = sortStr.lastIndexOf(" ");
				if (delimiter != -1) {
					String sortField = sortStr.substring(0, delimiter);
					if ("score".equals(sortField)) {
						sortField = "_score";
					}
					String reverse = sortStr.substring(delimiter + 1);
					if ("asc".equals(reverse)) {
						searchSourceBuilder.sort(sortField, SortOrder.ASC);
					} else if ("desc".equals(reverse)) {
						searchSourceBuilder.sort(sortField, SortOrder.DESC);
					}
				} else {
					searchSourceBuilder.sort(sortStr);
				}
			}
		} else {
			// default sort by descending score
			searchSourceBuilder.sort("_score", SortOrder.DESC);
		}

		// Build the search Request
		String[] indices = RestActions.splitIndices(index);
		SearchRequest searchRequest = new SearchRequest(indices);
		searchRequest.extraSource(searchSourceBuilder);
		searchRequest.types(RestActions.splitTypes(type));

		return searchRequest;
	}

	/**
	 * Converts the search response into a NamedList that the Solr Response Writer can use.
	 * 
	 * @param request the ES RestRequest
	 * @param response the ES SearchResponse
	 * @return a NamedList of the response
	 */
	private NamedList<Object> createSearchResponse(RestRequest request, SearchResponse response) {
		NamedList<Object> resp = new SimpleOrderedMap<Object>();
		resp.add("responseHeader", createResponseHeader(request, response));
		resp.add("response", convertToSolrDocumentList(request, response));
		return resp;
	}

	/**
	 * Creates the Solr response header based on the search response.
	 * 
	 * @param request the ES RestRequest
	 * @param response the ES SearchResponse
	 * @return the response header as a NamedList 
	 */
	private NamedList<Object> createResponseHeader(RestRequest request, SearchResponse response) {
		// generate response header
		NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
		responseHeader.add("status", 0);
		responseHeader.add("QTime", response.tookInMillis());

		// echo params in header
		NamedList<Object> params = new SimpleOrderedMap<Object>();
		if (request.hasParam("q"))
			params.add("q", request.param("q"));
		if (request.hasParam("fl"))
			params.add("fl", request.param("fl"));
		if (request.hasParam("sort"))
			params.add("sort", request.param("sort"));
		params.add("start", request.paramAsInt("start", 0));
		params.add("rows", request.paramAsInt("rows", 10));
		responseHeader.add("params", params);

		return responseHeader;
	}

	/**
	 * Converts the search results into a SolrDocumentList that can be serialized
	 * by the Solr Response Writer.   
	 * 
	 * @param request the ES RestRequest
	 * @param response the ES SearchResponse
	 * @return search results as a SolrDocumentList
	 */
	private SolrDocumentList convertToSolrDocumentList(RestRequest request, SearchResponse response) {
		SolrDocumentList results = new SolrDocumentList();

		// get the ES hits
		SearchHits hits = response.getHits();

		// set the result information on the SolrDocumentList 
		results.setMaxScore(hits.getMaxScore());
		results.setNumFound(hits.getTotalHits());
		results.setStart(request.paramAsInt("start", 0));

		// loop though the results and convert each
		// one to a SolrDocument
		for (SearchHit hit : hits.getHits()) {
			SolrDocument doc = new SolrDocument();

			// always add score to document
			doc.addField("score", hit.score());

			// attempt to get the returned fields
			// if none returned, use the source fields
			Map<String, SearchHitField> fields = hit.getFields();
			Map<String, Object> source = hit.sourceAsMap();
			if (fields.isEmpty()) {
				if (source != null) {
					for (String sourceField : source.keySet()) {
						Object fieldValue = source.get(sourceField);

						// ES does not return date fields as Date Objects
						// detect if the string is a date, and if so
						// convert it to a Date object
						if (fieldValue.getClass() == String.class) {
							if (datePattern.matcher(fieldValue.toString()).matches()) {
								fieldValue = dateFormat.parseDateTime(fieldValue.toString()).toDate();
							}
						}

						doc.addField(sourceField, fieldValue);
					}
				}
			} else {
				for (String fieldName : fields.keySet()) {
					SearchHitField field = fields.get(fieldName);
					Object fieldValue = field.getValue();

					// ES does not return date fields as Date Objects
					// detect if the string is a date, and if so
					// convert it to a Date object
					if (fieldValue.getClass() == String.class) {
						if (datePattern.matcher(fieldValue.toString()).matches()) {
							fieldValue = dateFormat.parseDateTime(fieldValue.toString()).toDate();
						}
					}

					doc.addField(fieldName, fieldValue);
				}
			}

			// add the SolrDocument to the SolrDocumentList
			results.add(doc);
		}

		return results;
	}
}