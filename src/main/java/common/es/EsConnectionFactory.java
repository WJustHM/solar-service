package common.es;


import com.sksamuel.elastic4s.ElasticClient;
import com.sksamuel.elastic4s.ElasticsearchClientUri;
import common.ConnectionException;
import common.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.common.settings.Settings;

/**
 * Created by cloud computing on 2016/9/21 0021.
 */
class EsConnectionFactory implements ConnectionFactory<ElasticClient> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 4024923894283696465L;

    private final ElasticsearchClientUri elasticsearchClientUri;

    private final Settings settings;

    public EsConnectionFactory(final ElasticsearchClientUri elasticsearchClientUri) {
        this.elasticsearchClientUri = elasticsearchClientUri;
        this.settings = Settings.builder().build();
    }

    public EsConnectionFactory(final ElasticsearchClientUri elasticsearchClientUri, final Settings settings) {
        this.elasticsearchClientUri = elasticsearchClientUri;
        this.settings = settings;
    }

    public EsConnectionFactory(final String host, final int port, final Settings settings) {
        if (host == null)
            throw new ConnectionException("[" + ESConfig.DEFAULT_HOST + "] is required !");
        if (port == 0)
            throw new ConnectionException("[" + ESConfig.DEFAULT_PORT + "] is required !");
        this.elasticsearchClientUri = ElasticsearchClientUri.apply(host, port);
        this.settings = settings;
    }

    public EsConnectionFactory(final String host, final int port) {
        if (host == null)
            throw new ConnectionException("[" + ESConfig.DEFAULT_HOST + "] is required !");
        if (port == 0)
            throw new ConnectionException("[" + ESConfig.DEFAULT_PORT + "] is required !");
        this.elasticsearchClientUri = ElasticsearchClientUri.apply(host, port);
        this.settings = Settings.builder().build();
    }

    public EsConnectionFactory(final String url) {
        if (url == null)
            throw new ConnectionException("[" + ESConfig.URL + "] is required !");
        elasticsearchClientUri = ElasticsearchClientUri.apply(url);
        this.settings = Settings.builder().build();
    }

    public EsConnectionFactory(final String url, final Settings settings) {
        if (url == null)
            throw new ConnectionException("[" + ESConfig.URL + "] is required !");
        elasticsearchClientUri = ElasticsearchClientUri.apply(url);
        this.settings = settings;
    }


    public PooledObject<ElasticClient> makeObject() throws Exception {
        ElasticClient clent = this.createConnection();
        return new DefaultPooledObject<ElasticClient>(clent);
    }

    public void destroyObject(PooledObject<ElasticClient> p) throws Exception {

        ElasticClient client = p.getObject();

        if (client != null)

            client.close();
    }


    public boolean validateObject(PooledObject<ElasticClient> p) {

        ElasticClient client = p.getObject();
        if (client == null)
            return false;
        return true;
    }


    public void activateObject(PooledObject<ElasticClient> p) throws Exception {
        // TODO Auto-generated method stub

    }


    public void passivateObject(PooledObject<ElasticClient> p) throws Exception {
        // TODO Auto-generated method stub

    }


    public ElasticClient createConnection() throws Exception {

        ElasticClient client = ElasticClient.remote(settings, elasticsearchClientUri);
        return client;
    }
}
