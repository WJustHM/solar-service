package common.es;


import com.sksamuel.elastic4s.ElasticClient;
import com.sksamuel.elastic4s.ElasticsearchClientUri;
import common.ConnectionPool;
import common.PoolBase;
import common.PoolConfig;
import org.elasticsearch.common.settings.Settings;


/**
 * Created by cloud computing on 2016/9/21 0021.
 */
public class EsConnectionPool extends PoolBase<ElasticClient> implements ConnectionPool<ElasticClient> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -9126420905798370263L;


    public EsConnectionPool(final PoolConfig poolConfig, final String host, final int port, final Settings settings) {

        super(poolConfig, new EsConnectionFactory(host, port, settings));

    }

    public EsConnectionPool(final PoolConfig poolConfig, final String url) {
        super(poolConfig, new EsConnectionFactory(url));
    }

    public EsConnectionPool(final PoolConfig poolConfig, final Settings settings, final ElasticsearchClientUri esurl) {
        super(poolConfig, new EsConnectionFactory(esurl, settings));
    }

    public EsConnectionPool(final String url) {

        this(new PoolConfig(), url);
    }


    @Override
    public ElasticClient getConnection() {

        return super.getResource();
    }

    @Override
    public void returnConnection(ElasticClient client) {

        super.returnResource(client);
    }

    @Override
    public void invalidateConnection(ElasticClient client) {

        super.invalidateResource(client);
    }

}