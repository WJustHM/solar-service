package common.es;

import com.sksamuel.elastic4s.ElasticClient;
import com.sksamuel.elastic4s.ElasticsearchClientUri;
import common.ConnectionPool;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by cloud computing on 2016/9/21 0021.
 */
public class EsSharedConnPool implements ConnectionPool<ElasticClient> {

    private static final AtomicReference<EsSharedConnPool> pool = new AtomicReference<EsSharedConnPool>();

    private final ElasticClient client;

    private EsSharedConnPool(ElasticsearchClientUri url, Settings settings) {

        this.client = ElasticClient.remote(settings, url);

    }

    public synchronized static EsSharedConnPool getInstance(ElasticsearchClientUri url, Settings settings) {
        if (pool.get() == null)

            pool.set(new EsSharedConnPool(url, settings));

        return pool.get();
    }

    public synchronized static EsSharedConnPool getInstance(String url) {

        ElasticsearchClientUri clientUrl = ElasticsearchClientUri.apply(url);

        Settings settings = Settings.EMPTY;

        return getInstance(clientUrl, settings);
    }

    @Override
    public ElasticClient getConnection() {
        return client;
    }

    @Override
    public void returnConnection(ElasticClient client) {


    }

    @Override
    public void invalidateConnection(ElasticClient client) {
        try {
            if (client != null)

                client.close();

        } catch (Exception e) {

            e.printStackTrace();
        }

    }
}
