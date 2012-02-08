package co.diji.rest;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.queryFilter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
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
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import co.diji.solr.SolrResponseWriter;
import co.diji.utils.QueryStringDecoder;

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

	/**
	 * Parse uri parameters.
	 * 
	 * ES request.param does not support multiple parameters with the same name yet.  This
	 * is needed for parameters such as fq in Solr.  This will not be needed once a fix is
	 * in ES.  https://github.com/elasticsearch/elasticsearch/issues/1544
	 * 
	 * @param uri The uri to parse
	 * @return a map of parameters, each parameter value is a list of strings.
	 */
	private Map<String, List<String>> parseUriParams(String uri) {
		// use netty query string decoder
		QueryStringDecoder decoder = new QueryStringDecoder(uri);
		return decoder.getParameters();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.elasticsearch.rest.RestHandler#handleRequest(org.elasticsearch.rest.RestRequest, org.elasticsearch.rest.RestChannel)
	 */
	public void handleRequest(final RestRequest request, final RestChannel channel) {
		// Get the parameters
		final Map<String, List<String>> params = parseUriParams(request.uri());

		// generate the search request
		SearchRequest searchRequest = getSearchRequest(params, request);
		searchRequest.listenerThreaded(false);

		// execute the search
		client.search(searchRequest, new ActionListener<SearchResponse>() {
			@Override
			public void onResponse(SearchResponse response) {
				try {
					// write response
					solrResponseWriter.writeResponse(createSearchResponse(params, request, response), request, channel);
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
	private SearchRequest getSearchRequest(Map<String, List<String>> params, RestRequest request) {
		// get solr search parameters
		String q = request.param("q");
		int start = request.paramAsInt("start", 0);
		int rows = request.paramAsInt("rows", 10);
		String fl = request.param("fl");
		String sort = request.param("sort");
		List<String> fqs = params.get("fq");
		boolean hl = request.paramAsBoolean("hl", false);
		boolean facet = request.paramAsBoolean("facet", false);

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

		// handler filters
		if (fqs != null && !fqs.isEmpty()) {
			FilterBuilder filterBuilder = null;

			// if there is more than one filter specified build
			// an and filter of query filters, otherwise just
			// build a single query filter.
			if (fqs.size() > 1) {
				AndFilterBuilder fqAnd = andFilter();
				for (String fq : fqs) {
					fqAnd.add(queryFilter(QueryBuilders.queryString(fq)));
				}
				filterBuilder = fqAnd;
			} else {
				filterBuilder = queryFilter(QueryBuilders.queryString(fqs.get(0)));
			}

			searchSourceBuilder.filter(filterBuilder);
		}

		// handle highlighting
		if (hl) {
			// get supported highlighting parameters if they exist
			String hlfl = request.param("hl.fl");
			int hlsnippets = request.paramAsInt("hl.snippets", 1);
			int hlfragsize = request.paramAsInt("hl.fragsize", 100);
			String hlsimplepre = request.param("hl.simple.pre");
			String hlsimplepost = request.param("hl.simple.post");

			HighlightBuilder highlightBuilder = new HighlightBuilder();
			if (hlfl == null) {
				// run against default _all field
				highlightBuilder.field("_all", hlfragsize, hlsnippets);
			} else {
				String[] hlfls = hlfl.split("\\s|,");
				for (String hlField : hlfls) {
					// skip wildcarded fields
					if (!hlField.contains("*")) {
						highlightBuilder.field(hlField, hlfragsize, hlsnippets);
					}
				}
			}

			// pre tags
			if (hlsimplepre != null) {
				highlightBuilder.preTags(hlsimplepre);
			}

			// post tags
			if (hlsimplepost != null) {
				highlightBuilder.postTags(hlsimplepost);
			}

			searchSourceBuilder.highlight(highlightBuilder);

		}

		// handle faceting
		if (facet) {
			// get supported facet parameters if they exist
			List<String> facetFields = params.get("facet.field");
			String facetSort = request.param("facet.sort");
			int facetLimit = request.paramAsInt("facet.limit", 100);

			List<String> facetQueries = params.get("facet.query");

			if (facetFields != null && !facetFields.isEmpty()) {
				for (String facetField : facetFields) {
					TermsFacetBuilder termsFacetBuilder = new TermsFacetBuilder(facetField);
					termsFacetBuilder.size(facetLimit);
					termsFacetBuilder.field(facetField);

					if (facetSort != null && facetSort.equals("index")) {
						termsFacetBuilder.order(TermsFacet.ComparatorType.TERM);
					} else {
						termsFacetBuilder.order(TermsFacet.ComparatorType.COUNT);
					}

					searchSourceBuilder.facet(termsFacetBuilder);
				}
			}

			if (facetQueries != null && !facetQueries.isEmpty()) {
				for (String facetQuery : facetQueries) {
					QueryFacetBuilder queryFacetBuilder = new QueryFacetBuilder(facetQuery);
					queryFacetBuilder.query(QueryBuilders.queryString(facetQuery));
					searchSourceBuilder.facet(queryFacetBuilder);
				}
			}
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
	private NamedList<Object> createSearchResponse(Map<String, List<String>> params, RestRequest request, SearchResponse response) {
		NamedList<Object> resp = new SimpleOrderedMap<Object>();
		resp.add("responseHeader", createResponseHeader(params, request, response));
		resp.add("response", convertToSolrDocumentList(request, response));

		// add highlight node if highlighting was requested
		NamedList<Object> highlighting = createHighlightResponse(request, response);
		if (highlighting != null) {
			resp.add("highlighting", highlighting);
		}

		// add faceting node if faceting was requested
		NamedList<Object> faceting = createFacetResponse(request, response);
		if (faceting != null) {
			resp.add("facet_counts", faceting);
		}

		return resp;
	}

	/**
	 * Creates the Solr response header based on the search response.
	 * 
	 * @param request the ES RestRequest
	 * @param response the ES SearchResponse
	 * @return the response header as a NamedList 
	 */
	private NamedList<Object> createResponseHeader(Map<String, List<String>> params, RestRequest request, SearchResponse response) {
		// generate response header
		NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
		responseHeader.add("status", 0);
		responseHeader.add("QTime", response.tookInMillis());

		// echo params in header
		NamedList<Object> solrParams = new SimpleOrderedMap<Object>();
		for (String param : params.keySet()) {
			List<String> paramValue = params.get(param);
			if (paramValue != null && !paramValue.isEmpty()) {
				solrParams.add(param, paramValue.size() > 1 ? paramValue : paramValue.get(0));
			}
		}

		responseHeader.add("params", solrParams);

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

	/**
	 * Creates a NamedList for the for document highlighting response
	 * 
	 * @param request the ES RestRequest
	 * @param response the ES SearchResponse
	 * @return a NamedList if highlighting was requested, null if not
	 */
	private NamedList<Object> createHighlightResponse(RestRequest request, SearchResponse response) {
		NamedList<Object> highlightResponse = null;

		// if highlighting was requested create the NamedList for the highlights
		if (request.paramAsBoolean("hl", false)) {
			highlightResponse = new SimpleOrderedMap<Object>();
			SearchHits hits = response.getHits();
			// for each hit, get each highlight field and put the list
			// of highlight fragments in a NamedList specific to the hit
			for (SearchHit hit : hits.getHits()) {
				NamedList<Object> docHighlights = new SimpleOrderedMap<Object>();
				Map<String, HighlightField> highlightFields = hit.getHighlightFields();
				for (String fieldName : highlightFields.keySet()) {
					HighlightField highlightField = highlightFields.get(fieldName);
					docHighlights.add(fieldName, highlightField.getFragments());
				}

				// highlighting by placing the doc highlights in the response
				// based on the document id
				highlightResponse.add(hit.field("id").getValue().toString(), docHighlights);
			}
		}

		// return the highlight response
		return highlightResponse;
	}

	private NamedList<Object> createFacetResponse(RestRequest request, SearchResponse response) {
		NamedList<Object> facetResponse = null;

		if (request.paramAsBoolean("facet", false)) {
			facetResponse = new SimpleOrderedMap<Object>();

			// create NamedLists for field and query facets
			NamedList<Object> termFacets = new SimpleOrderedMap<Object>();
			NamedList<Object> queryFacets = new SimpleOrderedMap<Object>();

			// loop though all the facets populating the NamedLists we just created
			Iterator<Facet> facetIter = response.facets().iterator();
			while (facetIter.hasNext()) {
				Facet facet = facetIter.next();
				if (facet.type().equals(TermsFacet.TYPE)) {
					// we have term facet, create NamedList to store terms
					TermsFacet termFacet = (TermsFacet) facet;
					NamedList<Object> termFacetObj = new SimpleOrderedMap<Object>();
					for (TermsFacet.Entry tfEntry : termFacet.entries()) {
						termFacetObj.add(tfEntry.term(), tfEntry.count());
					}

					termFacets.add(facet.getName(), termFacetObj);
				} else if (facet.type().equals(QueryFacet.TYPE)) {
					QueryFacet queryFacet = (QueryFacet) facet;
					queryFacets.add(queryFacet.getName(), queryFacet.count());
				}
			}

			facetResponse.add("facet_fields", termFacets);
			facetResponse.add("facet_queries", queryFacets);

			// add dummy facet_dates and facet_ranges since we dont support them yet
			facetResponse.add("facet_dates", new SimpleOrderedMap<Object>());
			facetResponse.add("facet_ranges", new SimpleOrderedMap<Object>());

		}

		return facetResponse;
	}
}