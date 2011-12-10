package org.elasticsearch.plugin.diji;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import co.diji.rest.SolrUpdateHandlerRestAction;

public class MockSolrPlugin extends AbstractPlugin {
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.elasticsearch.plugins.Plugin#name()
	 */
	public String name() {
		return "MockSolrPlugin";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.elasticsearch.plugins.Plugin#description()
	 */
	public String description() {
		return "Mocks an instance of Solr";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.elasticsearch.plugins.AbstractPlugin#processModule(org.elasticsearch.common.inject.Module)
	 */
	@Override
	public void processModule(Module module) {
		if (module instanceof RestModule) {
			((RestModule) module).addRestAction(SolrUpdateHandlerRestAction.class);
		}
	}

}
