from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("CassandraSecureSession").getOrCreate()
sc=spark.sparkContext
jvm=sc._jvm
try:
 ip=jvm.java.net.InetSocketAddress("your.cassandra.host",9042)
 contactPoints=jvm.java.util.Collections.singletonList(ip)
 File=jvm.java.io.File
 Paths=jvm.java.nio.file.Paths
 TrustManagerFactory=jvm.javax.net.ssl.TrustManagerFactory
 SSLContext=jvm.javax.net.ssl.SSLContext
 KeyStore=jvm.java.security.KeyStore
 FileInputStream=jvm.java.io.FileInputStream
 trustStoreFile=File("/full/path/to/truststore.jks")
 truststore_password="your_truststore_password"
 char_array=jvm.new_array(jvm.java.lang.Character.TYPE,len(truststore_password))
 for i,c in enumerate(truststore_password):char_array[i]=c
 ks=KeyStore.getInstance("JKS")
 ks.load(FileInputStream(trustStoreFile),char_array)
 tmf=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
 tmf.init(ks)
 sslContext=SSLContext.getInstance("TLS")
 sslContext.init(None,tmf.getTrustManagers(),None)
 PlainTextAuthProvider=jvm.com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider
 username="your_username"
 password="your_password"
 authProvider=PlainTextAuthProvider(username,password)
 sessionBuilder=jvm.com.datastax.oss.driver.api.core.CqlSession.builder()
 sessionBuilder=sessionBuilder.addContactPoints(contactPoints)
 sessionBuilder=sessionBuilder.withLocalDatacenter("datacenter1")
 sessionBuilder=sessionBuilder.withAuthProvider(authProvider)
 sessionBuilder=sessionBuilder.withSslContext(sslContext)
 sessionBuilder=sessionBuilder.withKeyspace(jvm.java.util.Optional.of("your_keyspace"))
 session=sessionBuilder.build()
 rs=session.execute("SELECT * FROM your_table LIMIT 1")
 it=rs.iterator()
 while it.hasNext():print(it.next())
except Exception as e:print("Error:",e)
