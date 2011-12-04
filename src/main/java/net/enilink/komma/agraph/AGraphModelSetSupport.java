package net.enilink.komma.agraph;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import net.enilink.composition.annotations.Iri;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGQuery;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.KommaCore;
import net.enilink.komma.common.AbstractKommaPlugin;
import net.enilink.komma.dm.IDataManager;
import net.enilink.komma.dm.IDataManagerFactory;
import net.enilink.komma.dm.change.IDataChangeSupport;
import net.enilink.komma.internal.sesame.SesameRepositoryDataManager;
import net.enilink.komma.model.IModelSet;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.IStatement;
import net.enilink.komma.core.IValue;
import net.enilink.komma.core.InferencingCapability;
import net.enilink.komma.core.KommaException;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIImpl;
import net.enilink.komma.sesame.SesameDataManagerFactory;
import net.enilink.komma.sesame.SesameModule;

@Iri(MODELS.NAMESPACE + "AGraphModelSet")
public abstract class AGraphModelSetSupport implements IModelSet.Internal {
	public static final String AGRAPH_INSTANCE = "localhost";
	public static final int AGRAPH_PORT = 10035;
	public static final String AGRAPH_USERNAME = "super";
	public static final String AGRAPH_PASSWORD = "super";

	protected Repository createRepository() throws RepositoryException {
		String url = getServerUrl();
		AGServer server = new AGServer(url, valueOrDefault(getUsername(),
				AGRAPH_USERNAME),
				valueOrDefault(getPassword(), AGRAPH_PASSWORD));
		AGCatalog catalog = server.getRootCatalog();
		AGRepository repository = new AGRepository(catalog, "enilink");
		return repository;
	}

	<T> T valueOrDefault(T value, T defaultValue) {
		return value != null ? value : defaultValue;
	}

	@Iri(MODELS.NAMESPACE + "host")
	public abstract String getHost();

	@Iri(MODELS.NAMESPACE + "port")
	public abstract Integer getPort();

	@Iri(MODELS.NAMESPACE + "username")
	public abstract String getUsername();

	@Iri(MODELS.NAMESPACE + "password")
	public abstract String getPassword();

	static class AGraphDataManagerFactory extends SesameDataManagerFactory {
		@Override
		public IDataManager get() {
			return injector.getInstance(AGraphDataManager.class);
		}
	}

	static class AGraphDataManager extends SesameRepositoryDataManager {
		@Inject
		public AGraphDataManager(Repository repository,
				IDataChangeSupport changeSupport) {
			super(repository, changeSupport);
			((AGRepositoryConnection) getConnection()).getHttpRepoClient()
					.setAllowExternalBlankNodeIds(true);
		}

		@Override
		protected Query prepareSesameQuery(String query, String baseURI)
				throws MalformedQueryException, RepositoryException {
			Query sesameQuery = null;
			for (String token : query.split("\\s")) {
				String tlc = token.toLowerCase();
				if (tlc.equals("select")) {
					sesameQuery = getConnection().prepareTupleQuery(
							QueryLanguage.SPARQL, query, baseURI);
					break;
				} else if (tlc.equals("construct") || tlc.equals("describe")) {
					sesameQuery = getConnection().prepareGraphQuery(
							QueryLanguage.SPARQL, query, baseURI);
					break;
				} else if (tlc.equals("ask")) {
					sesameQuery = getConnection().prepareBooleanQuery(
							QueryLanguage.SPARQL, query, baseURI);
					break;
				}
			}
			if (sesameQuery == null) {
				throw new KommaException("Unsupported query type.");
			}

			((AGQuery) sesameQuery).setEntailmentRegime(AGQuery.RESTRICTION);

			return sesameQuery;
		}

		@Override
		public boolean hasMatch(IReference subject, IReference predicate,
				IValue object) {
			Object result = createQuery("ASK { ?s ?p ?o }", null)
					.setParameter("s", subject).setParameter("p", predicate)
					.setParameter("o", object).evaluate().next();
			return Boolean.TRUE.equals(result);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public IExtendedIterator<IStatement> match(IReference subject,
				IReference predicate, IValue object) {
			return (IExtendedIterator) createQuery(
					"CONSTRUCT { ?s ?p ?o } WHERE {?s ?p ?o }", null)
					.setParameter("s", subject).setParameter("p", predicate)
					.setParameter("o", object).evaluate();
		}
	}

	@Override
	public void collectInjectionModules(Collection<Module> modules) {
		modules.add(Modules.override(new SesameModule()).with(
				new AbstractModule() {
					@Override
					protected void configure() {
						bind(AGraphDataManagerFactory.class)
								.in(Singleton.class);
						bind(IDataManagerFactory.class).to(
								AGraphDataManagerFactory.class);
						bind(IDataManager.class).toProvider(
								AGraphDataManagerFactory.class);
					}
				}));
		modules.add(new AbstractModule() {
			@Override
			protected void configure() {
				bind(InferencingCapability.class).toInstance(
						new InferencingCapability() {
							@Override
							public boolean doesRDFS() {
								return true;
							}

							@Override
							public boolean doesOWL() {
								return true;
							}
						});
			}

			@SuppressWarnings("unused")
			@Singleton
			@Provides
			protected Repository provideRepository() {
				try {
					Repository repository = createRepository();
					addBasicKnowledge(repository);
					return repository;
				} catch (RepositoryException e) {
					throw new KommaException("Unable to create repository.", e);
				}
			}
		});
	}

	protected void addBasicKnowledge(Repository repository)
			throws RepositoryException {
		String[] bundles = { "net.enilink.vocab.owl",
				"net.enilink.vocab.rdfs" };

		if (AbstractKommaPlugin.IS_ECLIPSE_RUNNING) {
			RepositoryConnection conn = null;

			try {
				conn = repository.getConnection();
				for (String name : bundles) {
					URL url = FileLocator.find(Platform.getBundle(name),
							new Path("META-INF/org.openrdf.ontologies"),
							Collections.emptyMap());
					if (url != null) {
						URL resolvedUrl = FileLocator.resolve(url);

						Properties properties = new Properties();
						InputStream in = resolvedUrl.openStream();
						properties.load(in);
						in.close();

						URI baseUri = URIImpl.createURI(url.toString())
								.trimSegments(1);
						for (Map.Entry<Object, Object> entry : properties
								.entrySet()) {
							String file = entry.getKey().toString();

							URIImpl fileUri = URIImpl.createFileURI(file);
							fileUri = fileUri.resolve(baseUri);

							resolvedUrl = FileLocator.resolve(new URL(fileUri
									.toString()));
							if (resolvedUrl != null) {
								in = resolvedUrl.openStream();
								if (in != null && in.available() > 0) {
									conn.add(in, "", RDFFormat.RDFXML,
											new org.openrdf.model.impl.URIImpl(
													getDefaultGraph()
															.toString()));
								}
								if (in != null) {
									in.close();
								}
							}
						}
					}
				}
			} catch (IOException e) {
				throw new KommaException("Cannot access RDF data", e);
			} catch (RepositoryException e) {
				throw new KommaException("Loading RDF failed", e);
			} catch (RDFParseException e) {
				throw new KommaException("Invalid RDF data", e);
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (RepositoryException e) {
						KommaCore.log(e);
					}
				}
			}
		}
	}

	protected String getServerUrl() {
		return "http://" + valueOrDefault(getHost(), AGRAPH_INSTANCE) + ":"
				+ valueOrDefault(getPort(), AGRAPH_PORT);
	}

	@Override
	public URI getDefaultGraph() {
		return URIImpl.createURI("komma:default");
	}

	@Override
	public boolean isPersistent() {
		return true;
	}
}
