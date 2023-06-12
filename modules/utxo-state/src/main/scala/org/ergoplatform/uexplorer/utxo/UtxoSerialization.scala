package org.ergoplatform.uexplorer.utxo

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block, BlockBuilder, ForkInserted}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.utxo.MvUtxoState.*
import org.h2.mvstore.{MVMap, MVStore}

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

trait UtxoSerialization {

  protected def serValueByBoxId(valueByBoxId: java.util.Map[BoxId, Value]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(valueByBoxId.size() * 256)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, valueByBoxId)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  protected def serValueByBoxIdNew(boxId: BoxId, value: Value): Array[Byte] = {
    val map = new util.HashMap[BoxId, Value]()
    map.put(boxId, value)
    serValueByBoxId(map)
  }

  protected def deserValueByBoxId(bytes: Array[Byte]): java.util.Map[BoxId, Value] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashMap[BoxId, Value]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  protected def addValue(bytes: Array[Byte], boxId: BoxId, value: Value): Array[Byte] = {
    val map = deserValueByBoxId(bytes)
    map.put(boxId, value)
    serValueByBoxId(map)
  }

}
