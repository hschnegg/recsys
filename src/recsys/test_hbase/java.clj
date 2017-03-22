(ns recsys.test-hbase.java
  (:import ;[com.google.cloud.bigtable.hbase.BigtableConfiguration]
           [org.apache.hadoop.hbase.HBaseConfiguration]
           [org.apache.hadoop.hbase.client.HTable]
           ;[org.apache.hadoop.hbase.HColumnDescriptor]
           ;[org.apache.hadoop.hbase.HTableDescriptor]
           ;[org.apache.hadoop.hbase.TableName]
           ;[org.apache.hadoop.hbase.client.Admin]
           ;[org.apache.hadoop.hbase.client.Connection]
           [org.apache.hadoop.hbase.client.Get]
           ;[org.apache.hadoop.hbase.client.Put]
           ;[org.apache.hadoop.hbase.client.Result]
           ;[org.apache.hadoop.hbase.client.ResultScanner]
           ;[org.apache.hadoop.hbase.client.Scan]
           [org.apache.hadoop.hbase.client.Table]
           [org.apache.hadoop.hbase.util.Bytes]))


(def conf
  (doto (org.apache.hadoop.hbase.HBaseConfiguration.)
    (.set "hbase.zookeeper.quorum" "localhost")
    (.set "hbase.zookeeper.property. clientPort" "2181")))

(def hTable
  (org.apache.hadoop.hbase.client.HTable. conf "test"))

(def row-bytes
  (org.apache.hadoop.hbase.util.Bytes/toBytes "row1"))

(def g
  (org.apache.hadoop.hbase.client.Get. row-bytes))

(def result
  (.get hTable get))

(def result-map
  (.getMap result))

(org.apache.hadoop.hbase.util.Bytes/toString (first result-map))


(first result-map)




;; https://github.com/sleberknight/basic-hbase-examples/blob/master/src/main/java/com/nearinfinity/explore/hbase/samples/basic/GetRow.java





