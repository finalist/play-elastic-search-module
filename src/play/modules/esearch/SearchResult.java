package play.modules.esearch;

import static play.modules.esearch.ESearch.searchableProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import play.db.Model;
import play.db.Model.Property;

public class SearchResult<T extends Model> {

    private final Collection<T> hits = new ArrayList<T>();

    public SearchResult(SearchResponse response, Class<T> modelClass) {
        try {
            for (SearchHit hit : response.getHits()) {
                T model = modelClass.newInstance();

                Map<String, Object> values = hit.sourceAsMap();
                
                for (Property property : searchableProperties(modelClass)) {
                    Object value = values.get(property.name);

                    if (property.type.isInstance(value)) {
                        property.field.set(model, value);
                    } else if ((property.type == long.class || property.type == Long.class) && value instanceof Integer) {
                        property.field.set(model, Integer.class.cast(value).longValue());
                    }
                }
                
                hits.add(model);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}