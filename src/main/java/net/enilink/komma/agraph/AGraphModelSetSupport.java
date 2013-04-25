package net.enilink.komma.agraph;

import java.util.Arrays;
import java.util.Collection;

import net.enilink.composition.annotations.Iri;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

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
import net.enilink.commons.iterator.IMap;
import net.enilink.komma.dm.IDataManager;
import net.enilink.komma.dm.IDataManagerFactory;
import net.enilink.komma.dm.IDataManagerQuery;
import net.enilink.komma.dm.change.IDataChangeSupport;
import net.enilink.komma.internal.sesame.SesameRepositoryDataManager;
import net.enilink.komma.model.IModelSet;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.sesame.RemoteModelSetSupport;
import net.enilink.komma.core.IBindings;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.IStatement;
import net.enilink.komma.core.IValue;
import net.enilink.komma.core.InferencingCapability;
import net.enilink.komma.core.KommaException;
import net.enilink.komma.core.Statement;
import net.enilink.komma.core.URIImpl;
import net.enilink.komma.sesame.SesameDataManagerFactory;
import net.enilink.komma.sesame.SesameModule;

@Iri(MODELS.NAMESPACE + "AGraphModelSet")
public abstract class AGraphModelSetSupport extends RemoteModelSetSupport
		implements IModelSet.Internal {
	public static final String AGRAPH_USERNAME = "super";
	public static final String AGRAPH_PASSWORD = "super";

	protected Repository createRepository() throws RepositoryException {
		String url = valueOrDefault(getServer(),
				URIImpl.createURI("http://localhost:10035")).toString();
		AGServer server = new AGServer(url, valueOrDefault(getUsername(),
				AGRAPH_USERNAME),
				valueOrDefault(getPassword(), AGRAPH_PASSWORD));
		AGCatalog catalog = server.getRootCatalog();
		AGRepository repository = new AGRepository(catalog, valueOrDefault(
				getRepository(), "enilink"));
		return repository;
	}

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

		protected IReference[] addNull(boolean includeInferred,
				IReference[] contexts) {
			if (includeInferred) {
				contexts = Arrays.copyOf(contexts, contexts.length + 1);
				contexts[contexts.length - 1] = null;
			}
			return contexts;
		}

		@Override
		public <R> IDataManagerQuery<R> createQuery(String query,
				String baseURI, boolean includeInferred, IReference... contexts) {
			return super.createQuery(query, baseURI, includeInferred,
					addNull(includeInferred, contexts));
		}

		@Override
		protected Query prepareSesameQuery(String query, String baseURI,
				boolean includeInferred) throws MalformedQueryException,
				RepositoryException {
			Query sesameQuery = super.prepareSesameQuery(query, baseURI,
					includeInferred);
			((AGQuery) sesameQuery).setEntailmentRegime(AGQuery.RESTRICTION);
			return sesameQuery;
		}

		@Override
		public boolean hasMatch(IReference subject, IReference predicate,
				IValue object, boolean includeInferred, IReference... contexts) {
			Object result = createQuery("ASK { ?s ?p ?o }", null,
					includeInferred, contexts).setParameter("s", subject)
					.setParameter("p", predicate).setParameter("o", object)
					.evaluate().next();
			return Boolean.TRUE.equals(result);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public IExtendedIterator<IStatement> match(IReference subject,
				IReference predicate, IValue object, boolean includeInferred,
				IReference... contexts) {
			String query;
			if (contexts.length > 0) {
				query = "SELECT DISTINCT ?s ?p ?o ?g WHERE { GRAPH ?g { ?s ?p ?o } }";
			} else {
				query = "SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o }";
			}
			return (IExtendedIterator) createQuery(query, null,
					includeInferred, contexts).setParameter("s", subject)
					.setParameter("p", predicate).setParameter("o", object)
					.evaluate().mapWith(new IMap<Object, IStatement>() {
						@Override
						public IStatement map(Object value) {
							IBindings<?> bindings = (IBindings<?>) value;
							return new Statement(
									(IReference) bindings.get("s"),
									(IReference) bindings.get("p"), bindings
											.get("o"), (IReference) bindings
											.get("g"));
						}
					});
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

	@Override
	protected boolean skipRdfsOnImport() {
		return false;
	}
}
