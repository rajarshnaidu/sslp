from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CassandraSecureSession").getOrCreate()
sc = spark.sparkContext
jvm = sc._jvm

try:
    # 1. Contact point
    ip = jvm.java.net.InetSocketAddress("your.cassandra.host", 9042)
    contactPoints = jvm.java.util.Collections.singletonList(ip)

    # 2. SSL context factory setup
    File = jvm.java.io.File
    Paths = jvm.java.nio.file.Paths
    TrustManagerFactory = jvm.javax.net.ssl.TrustManagerFactory
    SSLContext = jvm.javax.net.ssl.SSLContext
    KeyStore = jvm.java.security.KeyStore
    FileInputStream = jvm.java.io.FileInputStream

    # Load the truststore
    trustStoreFile = File("/full/path/to/truststore.jks")
    trustStorePassword = "your_truststore_password"

    ks = KeyStore.getInstance("JKS")
    ks.load(FileInputStream(trustStoreFile), trustStorePassword.toCharArray())

    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
    tmf.init(ks)

    sslContext = SSLContext.getInstance("TLS")
    sslContext.init(None, tmf.getTrustManagers(), None)

    # 3. Auth provider
    PlainTextAuthProvider = jvm.com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider
    authProvider = PlainTextAuthProvider("your_username", "your_password")

    # 4. Session builder
    sessionBuilder = jvm.com.datastax.oss.driver.api.core.CqlSession.builder()

    sessionBuilder = sessionBuilder.addContactPoints(contactPoints)
    sessionBuilder = sessionBuilder.withLocalDatacenter("datacenter1")
    sessionBuilder = sessionBuilder.withAuthProvider(authProvider)
    sessionBuilder = sessionBuilder.withSslContext(sslContext)
    sessionBuilder = sessionBuilder.withKeyspace(jvm.java.util.Optional.of("your_keyspace"))

    # 5. Build session
    session = sessionBuilder.build()

    # 6. Test query
    rs = session.execute("SELECT * FROM your_table LIMIT 1")
    it = rs.iterator()
    while it.hasNext():
        print(it.next())

except Exception as e:
    print("Error creating secure session:", e)
