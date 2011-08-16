package play.modules.esearch;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static play.templates.JavaExtensions.slugify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.db.Model;
import play.db.Model.Property;

/**
 * @author Rob Schellhorn
 * @since 1.0
 */
public class ESearch extends PlayPlugin {

    private static Client client;

    public static SearchRequestBuilder search(Class<? extends Model> clazz) {
        return client.prepareSearch(indexName()).setTypes(typeName(clazz));
    }

    private static String indexName() {
        return Play.configuration.getProperty("application.name");
    }

    private static String typeName(Class<? extends Model> clazz) {
        return slugify(clazz.getSimpleName());
    }

    static List<Property> searchableProperties(Class<? extends Model> type) {
        List<Property> result = new ArrayList<Property>();
        for (Property property : Model.Manager.factoryFor(type).listProperties()) {
            result.add(property);
        }
        return unmodifiableList(result);
    }

    /**
     * @see PlayPlugin#onApplicationStart()
     */
    @Override
    public void onApplicationStart() {
        createClient();
        createIndex();
    }
    
    /**
     * @see PlayPlugin#onApplicationStop()
     */
    @Override
    public void onApplicationStop() {
        client.close();
    }

    @Override
    public void onEvent(String message, Object context) {
        if ("JPASupport.objectPersisted".equals(message) || "JPASupport.objectUpdated".equals(message)) {
            index((Model) context);
        } else if ("JPASupport.objectDeleted".equals(message)) {
            unindex((Model) context);
        } else {
            Logger.info("Foo " +message);
        }
    }

    private void index(Model model) {
        Class<? extends Model> type = model.getClass();
        if (!isSearchable(type)) {
            return;
        }

        Logger.info("Going to index a model %s", model);
        try {
            client.prepareIndex(indexName(), typeName(type), String.valueOf(model._key()))
                  .setSource(toSource(model))
                  .execute()
                  .actionGet();
        } catch (Exception e) {
            Logger.info(e, "Failed to index a model %s", model);
        }
    }

    private void unindex(Model model) {
        Class<? extends Model> type = model.getClass();
        if (!isSearchable(type)) {
            return;
        }

        Logger.info("Going to unindex a model %s", model);
        client.delete(new DeleteRequest(indexName(), typeName(type), String.valueOf(model._key()))).actionGet();
    }

    private void createClient() {
        Logger.info("Starting ESearch for Play!");
        NodeBuilder nb = nodeBuilder().local(true).client(false).data(true);
        Node node = nb.node();
        client = node.client();
    }

    private void createIndex() {
        try {
            IndicesAdminClient indices = client.admin().indices();
    
            if (indices.exists(new IndicesExistsRequest(indexName())).actionGet().exists()) {
                Logger.info("The index exists already, deleting ...");
                indices.delete(new DeleteIndexRequest(indexName())).actionGet();
            }
    
            CreateIndexRequest request = new CreateIndexRequest(indexName());
            for (Class<? extends Model> type : searchableTypes()) {
                request.mapping(typeName(type), toMapping(type));
            }
            indices.create(request).actionGet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private XContentBuilder toSource(Model model) throws IOException, IllegalArgumentException, IllegalAccessException {
        XContentBuilder source = jsonBuilder().startObject();
        for (Property property : searchableProperties(model.getClass())) {
            Object value = property.field.get(model);
            if (value != null) {
                source.field(property.name, value);
            }
        }
        
        return source.endObject();
    }

    private XContentBuilder toMapping(Class<? extends Model> type) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject();
        mapping.field(typeName(type)).startObject();
        mapping.field("properties").startObject();
        for (Property property : searchableProperties(type)) {
            mapping.field(property.name).startObject();
            mapping.field("type", type(property));
            index(property, mapping);
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        return mapping;
    }

    private boolean isSearchable(Class<? extends Model> type) {
        return type.isAnnotationPresent(Searchable.class);
    }

    private List<Class<? extends Model>> searchableTypes() {
        List<Class<? extends Model>> result = new ArrayList<Class<? extends Model>>();
        for (ApplicationClass modelClass : Play.classes.getAssignableClasses(Model.class)) {
            Class<? extends Model> type = (Class<? extends Model>) modelClass.javaClass;
            if (isSearchable(type)) {
                result.add(type);
            }
        }
        return unmodifiableList(result);
    }

    private String type(Property property) {
        Fields fields = property.field.getAnnotation(Fields.class);
        if (fields != null && fields.fields().length > 1) {
            return "multi_field";            
        } else if (!property.isRelation) {
            if (property.type == int.class || property.type == Integer.class) {
                return "integer";
            } else if (property.type == long.class || property.type == Long.class) {
                return "long";
            }
        }
        return "string";
    }

    private XContentBuilder index(Property property, XContentBuilder builder) throws IOException {
        Field field = property.field.getAnnotation(Field.class);
        if (field != null) {
            switch (field.index()) {
                case NOT_ANALYZED:
                    builder.field("index", "not_analyzed");
                    break;
            }
        }
        return builder;
    }
}